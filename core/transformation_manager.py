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
        raw_table = parsed_config.get("table", "unknown_table")
        self.table_name = re.sub(r"[^a-zA-Z0-9_]", "_", Helper.parse_table_name(raw_table))

    def apply_transformations(self) -> DataFrame:
        logger.info(f"Applying DataFrame transformations for table '{self.table_name}'...")
        
        joined_dfs: Dict[str, DataFrame] = {}
        
        current_df = self.df.alias(self.main_alias)
        joined_dfs[self.main_alias] = current_df

        joins_meta = self.parsed_config.get("joins", [])
        if joins_meta:
            current_df, joined_dfs = self._apply_joins(current_df, joins_meta, joined_dfs)

        rules_meta = self.parsed_config.get("rules", [])
        if rules_meta:
            current_df = self._apply_rules(current_df, rules_meta, joined_dfs)

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

            if "column" in rule and "expression" in rule:
                col_name = rule["column"].strip()
                expr_str = self._sanitize_expression_quotes(rule["expression"].strip())
                df = df.withColumn(col_name, F.expr(expr_str))
                already_selected_cols.add(self._normalize_col_name(col_name))
                already_selected_cols.add(col_name)

            elif "run" in rule:
                run_expr = rule["run"].strip()
                if run_expr.endswith(".*"):
                    col_prefix = run_expr[:-2]
                    matching_cols = [c for c in df.columns if c == col_prefix or c.endswith(f".{col_prefix}")]
                    if matching_cols:
                        target_col = matching_cols[0]
                        schema_field = next((f for f in df.schema.fields if f.name == target_col), None)
                        if schema_field and hasattr(schema_field.dataType, "names"):
                            for field in schema_field.dataType.names:
                                out_col = f"{col_prefix}_{field}"
                                df = df.withColumn(out_col, F.col(f"`{target_col}`.{field}"))
                                already_selected_cols.add(self._normalize_col_name(out_col))
                                already_selected_cols.add(out_col)

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
                excluded_simple.add(self._normalize_col_name(item_clean))

        right_cols_to_drop = []
        for qual in excluded_qualified:
            alias_part, col_part = qual.split(".", 1)
            if alias_part in joined_dfs and alias_part != self.main_alias:
                right_cols_to_drop.append(joined_dfs[alias_part][col_part])

        for col_ref in right_cols_to_drop:
            try:
                df = df.drop(col_ref)
            except Exception as e:
                logger.warning(f"Failed to drop qualified column {col_ref}: {e}")

        cols_to_select = []
        main_df = joined_dfs.get(self.main_alias)

        for col_name in df.columns:
            norm_name = self._normalize_col_name(col_name)

            if col_name in already_selected_cols or norm_name in already_selected_cols:
                cols_to_select.append(col_name)
                continue

            if col_name in excluded_simple or norm_name in excluded_simple:
                continue

            if main_df and col_name in main_df.columns:
                cols_to_select.append(main_df[col_name])
            else:
                cols_to_select.append(col_name)

        return df.select(*cols_to_select)

    def _sanitize_expression_quotes(self, expr: str) -> str:
        return re.sub(r'\"([^\"]*)\"', r"'\1'", expr)

    def _normalize_col_name(self, col_ref: str) -> str:
        clean = col_ref.replace("`", "").strip()
        return clean.split(".")[-1]