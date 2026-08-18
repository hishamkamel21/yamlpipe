

import logging
import re
from typing import Dict, Any, List, Set, Tuple
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from yamlpipe.utility.helper import Helper

logger = logging.getLogger("TransformationManager")


class TransformationManager:

    def __init__(self, parsed_config: Dict[str, Any], df: DataFrame):
        if not isinstance(parsed_config, dict):
            raise TypeError(f"[TransformationManager Error] Expected dict, got {type(parsed_config).__name__}")
        if not isinstance(df, DataFrame):
            raise TypeError(f"[TransformationManager Error] Expected PySpark DataFrame, got {type(df).__name__}")

        self.parsed_config = parsed_config
        self.df = df
        self.spark = df.sparkSession
        self.main_alias = parsed_config.get("alias") or "c"
        self.registered_aliases = set(parsed_config.get("registered_aliases", [self.main_alias]))
        raw_table = parsed_config.get("table", "unknown_table")
        self.table_name = re.sub(r"[^a-zA-Z0-9_]", "_", Helper.parse_table_name(raw_table))

    def apply_transformations(self) -> DataFrame:
        logger.info(f"Applying DataFrame transformations for table '{self.table_name}'...")
        
        joined_dfs: Dict[str, DataFrame] = {}
        current_df = self.df
        joined_dfs[self.main_alias] = current_df

        stages = self.parsed_config.get("stages", [])

        for stage_idx, stage in enumerate(stages, 1):
            stage_type = stage.get("type")
            stage_data = stage.get("data", [])

            if stage_type == "joins":
                logger.info(f"Executing Stage {stage_idx}: JOINS...")                
                current_df = current_df.alias(self.main_alias)
                joined_dfs[self.main_alias] = current_df
                
                current_df, joined_dfs = self._apply_joins(current_df, stage_data, joined_dfs)

            elif stage_type == "rules":
                logger.info(f"Executing Stage {stage_idx}: RULES...")
                current_df = self._apply_rules(current_df, stage_data, joined_dfs)

        logger.info(f"Successfully applied transformations for '{self.table_name}'.")
        return current_df
    
    def _apply_joins(
        self, df: DataFrame, joins_meta: List[Dict[str, Any]], joined_dfs: Dict[str, DataFrame]
    ) -> Tuple[DataFrame, Dict[str, DataFrame]]:
        for j in joins_meta:
            if not isinstance(j, dict):
                continue

            target_table = j.get("table")
            tbl_alias = j.get("alias") or target_table
            how = j.get("how", "left")

            right_df = self.spark.table(target_table)
            if tbl_alias:
                right_df = right_df.alias(tbl_alias)

            joined_dfs[tbl_alias] = right_df

            if j.get("broadcast"):
                right_df = F.broadcast(right_df)

            on_clause = j.get("on_clause")
            if on_clause:
                df = df.join(right_df, on=F.expr(on_clause), how=how)
            else:
                df = df.join(right_df, how=how)

        return df, joined_dfs

    def _apply_rules(
        self, df: DataFrame, rules_meta: List[Dict[str, Any]], joined_dfs: Dict[str, DataFrame]
    ) -> DataFrame:
        already_selected_cols: Set[str] = set()
        raw_except_list: List[str] = []
        select_rest_enabled = False

        for rule in rules_meta:
            if not isinstance(rule, dict):
                continue

            # 1. Column + Expression Processing
            if "column" in rule and "expression" in rule:
                raw_col = rule["column"].strip()
                target_col = self._strip_prefix(raw_col)
                expr_str = self._sanitize_expression_quotes(rule["expression"].strip())
                
                df = df.withColumn(target_col, F.expr(expr_str))
                
                already_selected_cols.add(target_col)
                already_selected_cols.add(raw_col)

            # 2. General RUN Execution Logic (Struct Unpacking OR Arbitrary SQL Expr)
            elif "run" in rule:
                run_expr = rule["run"].strip()
                
                # Case A: Struct Unpacking (e.g., run: "location.*")
                if run_expr.endswith(".*"):
                    col_prefix = run_expr[:-2]
                    matching_cols = [c for c in df.columns if c == col_prefix or c.endswith(f".{col_prefix}")]
                    if matching_cols:
                        target_col = matching_cols[0]
                        schema_field = next((f for f in df.schema.fields if f.name == target_col), None)
                        if schema_field and hasattr(schema_field.dataType, "names"):
                            for field in schema_field.dataType.names:
                                out_col = f"{self._strip_prefix(col_prefix)}_{field}"
                                df = df.withColumn(out_col, F.col(f"`{target_col}`.{field}"))
                                already_selected_cols.add(out_col)

                # Case B: Arbitrary SQL Expression (e.g., run: "split(email, '@')[0] as username")
                else:
                    sanitized_run_expr = self._sanitize_expression_quotes(run_expr)
                    df = df.selectExpr("*", sanitized_run_expr)

            # 3. Select The Rest Processing
            elif "select_the_rest" in rule:
                rest_cfg = rule["select_the_rest"]
                select_rest_enabled = rest_cfg.get("enable", False)
                raw_except_list = rest_cfg.get("except", [])

        if select_rest_enabled:
            df = self.resolve_select_the_rest(
                df=df,
                except_list=raw_except_list,
                already_selected_cols=already_selected_cols,
                joined_dfs=joined_dfs,
            )

        return df

    
    def resolve_select_the_rest(
        self,
        df: DataFrame,
        except_list: List[str],
        already_selected_cols: Set[str],
        joined_dfs: Dict[str, DataFrame],
    ) -> DataFrame:
        excluded_qualified: Set[str] = set()
        excluded_simple: Set[str] = set()

        for item in except_list:
            item_clean = item.replace("`", "").strip()
            if "." in item_clean:
                excluded_qualified.add(item_clean)
            else:
                excluded_simple.add(item_clean)

        for qual in list(excluded_qualified):
            alias_part, col_part = qual.split(".", 1)
            if alias_part in joined_dfs:
                source_df = joined_dfs[alias_part]
                if col_part in source_df.columns:
                    try:
                        df = df.drop(source_df[col_part])
                    except Exception as e:
                        logger.warning(f"Failed to drop qualified column {qual}: {e}")

        cols_to_select = []
        seen_output_names = set()

        for col_name in df.columns:
            plain_name = self._strip_prefix(col_name)

            if col_name in excluded_simple or plain_name in excluded_simple:
                continue

            if col_name in already_selected_cols or plain_name in already_selected_cols:
                if plain_name not in seen_output_names:
                    cols_to_select.append(col_name)
                    seen_output_names.add(plain_name)
                continue

            if plain_name not in seen_output_names:
                cols_to_select.append(col_name)
                seen_output_names.add(plain_name)

        return df.select(*cols_to_select)

    def _strip_prefix(self, col_ref: str) -> str:
        clean = col_ref.replace("`", "").strip()
        return clean.split(".")[-1]

    def _sanitize_expression_quotes(self, expr: str) -> str:
        return re.sub(r'\"([^\"]*)\"', r"'\1'", expr)