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
        self.spark = df.sparkSession
        self.main_alias = parsed_config.get("alias") or "c"
        
        # 1. Apply alias immediately at start
        self.df = df.alias(self.main_alias)
        
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
                # CRITICAL: Always re-enforce the main alias right before doing joins
                current_df = current_df.alias(self.main_alias)
                current_df, joined_dfs = self._apply_joins(current_df, stage_data, joined_dfs)

            elif stage_type == "rules":
                logger.info(f"Executing Stage {stage_idx}: RULES...")
                current_df = self._apply_rules(current_df, stage_data, joined_dfs)
                # Re-apply main alias after transformations so PySpark logic retains 'c'
                current_df = current_df.alias(self.main_alias)

            joined_dfs[self.main_alias] = current_df

        logger.info(f"Successfully applied transformations for '{self.table_name}'.")
        return current_df

    def _apply_rules(
        self, df: DataFrame, rules_meta: List[Dict[str, Any]], joined_dfs: Dict[str, DataFrame]
    ) -> DataFrame:
        already_selected_cols: Set[str] = set()

        for rule in rules_meta:
            if not isinstance(rule, dict):
                continue

            if "column" in rule and "expression" in rule:
                raw_col = rule["column"].strip()
                target_col = self._strip_prefix(raw_col)
                expr_str = self._sanitize_expression_quotes(rule["expression"].strip())
                
                df = df.withColumn(target_col, F.expr(expr_str))
                already_selected_cols.add(target_col.lower())

            elif "run" in rule:
                run_expr = rule["run"].strip()
                if run_expr:
                    sanitized_run_expr = self._sanitize_expression_quotes(run_expr)
                    
                    raw_lines = [
                        line.strip().rstrip(",") 
                        for line in sanitized_run_expr.splitlines() 
                        if line.strip()
                    ]
                    
                    if raw_lines:
                        full_expr_str = " , ".join(raw_lines)
                        expressions = [e.strip() for e in re.split(r',\s*(?![^()]*\))', full_expr_str) if e.strip()]
                        
                        for expr in expressions:
                            match = re.search(r'^(.*?)\s+as\s+(.*)$', expr, re.IGNORECASE)
                            
                            if match:
                                expr_body = match.group(1).strip()
                                alias_name = match.group(2).strip().replace("`", "").replace('"', '')
                                
                                df = df.withColumn(alias_name, F.expr(expr_body))
                                already_selected_cols.add(alias_name.lower())
                            else:
                                df = df.selectExpr("*", expr)

            elif "select" in rule:
                select_cfg = rule["select"]
                
                if isinstance(select_cfg, str):
                    sanitized_expr = self._sanitize_expression_quotes(select_cfg.strip())
                    raw_lines = [
                        line.strip().rstrip(",")
                        for line in sanitized_expr.splitlines()
                        if line.strip()
                    ]
                    if raw_lines:
                        full_expr_str = " , ".join(raw_lines)
                        expr_list = [e.strip() for e in re.split(r',\s*(?![^()]*\))', full_expr_str) if e.strip()]
                        df = df.selectExpr(*expr_list)

                elif isinstance(select_cfg, list):
                    select_cols = [str(c).strip() for c in select_cols]
                    df = df.select(*[F.col(f"`{c}`") for c in select_cols])

                elif isinstance(select_cfg, dict):
                    handled_enabled = select_cfg.get("handled_cols", False)
                    raw_except_list = select_cfg.get("except", [])

                    if handled_enabled:
                        df = self.resolve_select_with_except(
                            df=df,
                            except_list=raw_except_list,
                            already_selected_cols=already_selected_cols,
                            joined_dfs=joined_dfs,
                        )

            # Re-alias after each rule step to preserve lineage tag
            df = df.alias(self.main_alias)

        return df

    def resolve_select_with_except(
        self,
        df: DataFrame,
        except_list: List[str],
        already_selected_cols: Set[str],
        joined_dfs: Dict[str, DataFrame],
    ) -> DataFrame:
        logger.info("Resolving 'select' with handled_cols and except list...")

        explicit_except_map: Dict[str, Set[str]] = {alias.lower(): set() for alias in self.registered_aliases}
        global_except_set: Set[str] = set()

        for col in except_list:
            if not isinstance(col, str):
                continue
            col_str = col.strip().lower()
            if "." in col_str:
                prefix, col_name = col_str.split(".", 1)
                if prefix in explicit_except_map:
                    explicit_except_map[prefix].add(col_name)
                else:
                    global_except_set.add(col_name)
            else:
                global_except_set.add(col_str)

        selected_expressions = []
        processed_target_cols = set()

        # Build list directly from current active columns
        for col_name in df.columns:
            col_lower = col_name.lower()

            if col_lower in global_except_set:
                continue

            is_excepted = False
            for alias, except_cols in explicit_except_map.items():
                if col_lower in except_cols:
                    is_excepted = True
                    break

            if is_excepted:
                continue

            if col_lower not in processed_target_cols:
                selected_expressions.append(F.col(f"`{col_name}`"))
                processed_target_cols.add(col_lower)

        if selected_expressions:
            return df.select(*selected_expressions)

        return df

    def _apply_joins(
        self, main_df: DataFrame, joins_config: List[Dict[str, Any]], joined_dfs: Dict[str, DataFrame]
    ) -> Tuple[DataFrame, Dict[str, DataFrame]]:
        for join_item in joins_config:
            table = join_item.get("table")
            alias = join_item.get("alias")
            how = join_item.get("how", "inner")
            on_clause = join_item.get("on_clause")

            logger.info(f"Joining table '{table}' as '{alias}' using {how} join...")
            right_df = self.spark.table(table).alias(alias)
            joined_dfs[alias] = right_df

            if join_item.get("broadcast", False):
                right_df = F.broadcast(right_df)

            # Ensure main_df explicitly has main_alias right at execution time
            main_df = main_df.alias(self.main_alias)
            main_df = main_df.join(right_df, on=F.expr(on_clause), how=how)

        return main_df, joined_dfs

    def _strip_prefix(self, col_name: str) -> str:
        if "." in col_name:
            prefix, rest = col_name.split(".", 1)
            if prefix in self.registered_aliases:
                return rest
        return col_name

    def _sanitize_expression_quotes(self, expr_str: str) -> str:
        return re.sub(r'(?<!\\)"([^"\\]*(?:\\.[^"\\]*)*)"', r"'\1'", expr_str)