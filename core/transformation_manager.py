import logging
import re
from typing import Dict, Any, List, Set
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
        current_df = self.df.alias(self.main_alias)

        # STAGE 1: JOINS
        joins_meta = self.parsed_config.get("joins", [])
        if joins_meta:
            current_df = self._apply_joins(current_df, joins_meta)

        # STAGE 2: RULES
        rules_meta = self.parsed_config.get("rules", [])
        if rules_meta:
            current_df = self._apply_rules(current_df, rules_meta)

        logger.info(f"Successfully applied transformations for '{self.table_name}'.")
        return current_df

    def _apply_joins(self, df: DataFrame, joins_meta: List[Dict[str, Any]]) -> DataFrame:
        for j in joins_meta:
            if not isinstance(j, dict):
                continue

            target_table = j.get("table")
            tbl_alias = j.get("alias") or target_table
            how = j.get("how", "left")

            right_df = self.spark.table(target_table)
            if tbl_alias:
                right_df = right_df.alias(tbl_alias)

            if j.get("broadcast"):
                right_df = F.broadcast(right_df)

            on_clause = j.get("on_clause")
            if on_clause:
                df = df.join(right_df, on=F.expr(on_clause), how=how)
            else:
                df = df.join(right_df, how=how)

        return df

    def _apply_rules(self, df: DataFrame, rules_meta: List[Dict[str, Any]]) -> DataFrame:
        processed_columns: Set[str] = set()
        cols_to_except: Set[str] = set()
        select_rest_enabled = False

        for rule in rules_meta:
            if not isinstance(rule, dict):
                continue

            # CASE 1: Resolved Column SQL Expression
            if "column" in rule and "expression" in rule:
                col_name = rule["column"].strip()
                expr_str = self._sanitize_expression_quotes(rule["expression"].strip())
                df = df.withColumn(col_name, F.expr(expr_str))
                processed_columns.add(self._normalize_col_name(col_name))

            # CASE 2: Struct Expansion (e.g. location.*)
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
                                processed_columns.add(self._normalize_col_name(out_col))

            # CASE 3: Select The Rest
            elif "select_the_rest" in rule:
                rest_cfg = rule["select_the_rest"]
                select_rest_enabled = rest_cfg.get("enable", False)
                raw_except = rest_cfg.get("except", [])
                cols_to_except.update(self._normalize_col_name(c) for c in raw_except)

        # STAGE 3: DROP EXCLUDED COLUMNS
        if select_rest_enabled:
            for c in df.columns:
                norm_c = self._normalize_col_name(c)
                if norm_c in cols_to_except and norm_c not in processed_columns:
                    df = df.drop(F.col(c))

        return df

    def _sanitize_expression_quotes(self, expr: str) -> str:
        """Converts double quotes inside SQL expressions to single quotes."""
        return re.sub(r'\"([^\"]*)\"', r"'\1'", expr)

    def _normalize_col_name(self, col_ref: str) -> str:
        clean = col_ref.replace("`", "").strip()
        return clean.split(".")[-1]