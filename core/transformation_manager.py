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

        # STAGE 1: EXECUTE JOINS
        joins_meta = self.parsed_config.get("joins", [])
        if joins_meta:
            current_df = self._apply_joins(current_df, joins_meta)

        # STAGE 2: APPLY RULES & PROJECTIONS
        rules_meta = self.parsed_config.get("rules", [])
        if rules_meta:
            current_df = self._apply_rules(current_df, rules_meta)

        logger.info(f"Successfully applied transformations for '{self.table_name}'.")
        return current_df

    def _apply_joins(self, df: DataFrame, joins_meta: List[Dict[str, Any]]) -> DataFrame:
        """Executes DataFrame joins using PySpark API based on join configuration metadata."""
        for j in joins_meta:
            if not isinstance(j, dict):
                continue

            target_table = j.get("table")
            tbl_alias = j.get("alias") or target_table
            how = j.get("how", "left")

            # Load target table relation
            right_df = self.spark.table(target_table)
            if tbl_alias:
                right_df = right_df.alias(tbl_alias)

            # Apply Broadcast Hint if enabled
            if j.get("broadcast"):
                right_df = F.broadcast(right_df)

            # Parse Join Condition Clause
            on_clause = j.get("on_clause")
            sql_clause = j.get("sql", "")

            if not on_clause and " ON " in sql_clause.upper():
                on_clause = re.split(r"\s+ON\s+", sql_clause, flags=re.IGNORECASE)[-1]

            if on_clause:
                join_condition = F.expr(on_clause)
                df = df.join(right_df, on=join_condition, how=how)
            else:
                df = df.join(right_df, how=how)

        return df

    def _apply_rules(self, df: DataFrame, rules_meta: List[Dict[str, Any]]) -> DataFrame:
        """Applies transformation rules sequentially using withColumn and drops excluded columns."""
        processed_columns: Set[str] = set()
        cols_to_except: Set[str] = set()
        select_rest_enabled = False

        for rule in rules_meta:
            if not isinstance(rule, dict):
                continue

            # Case 1: Template expression over multiple columns
            if "expression" in rule and "columns" in rule:
                template_expr = rule["expression"]
                for col_name in rule["columns"]:
                    formatted_expr = template_expr.replace("${col}", col_name).replace("${column}", col_name)
                    df = df.withColumn(col_name, F.expr(formatted_expr))
                    processed_columns.add(self._normalize_col_name(col_name))

            # Case 2: Structural expansion / wildcard run (e.g., location.*)
            elif "run" in rule:
                run_expr = rule["run"].strip()
                if run_expr.endswith(".*"):
                    col_prefix = run_expr[:-2]
                    if col_prefix in df.columns:
                        for field in df.schema[col_prefix].dataType.names:
                            out_col = f"{col_prefix}_{field}"
                            df = df.withColumn(out_col, F.col(f"`{col_prefix}`.{field}"))
                            processed_columns.add(self._normalize_col_name(out_col))

            # Case 3: Explicit single column mapping with expression
            elif "column" in rule and "expression" in rule:
                col_name = rule["column"].strip()
                expr_str = rule["expression"].strip()
                df = df.withColumn(col_name, F.expr(expr_str))
                processed_columns.add(self._normalize_col_name(col_name))

            # Case 4: select_the_rest rule
            elif "select_the_rest" in rule:
                rest_cfg = rule["select_the_rest"]
                select_rest_enabled = rest_cfg.get("enable", False)
                raw_except = rest_cfg.get("except", [])
                cols_to_except.update(self._normalize_col_name(c) for c in raw_except)

        # STAGE 3: APPLY SELECT_THE_REST DROPS
        if select_rest_enabled:
            existing_cols_to_drop = [
                c for c in df.columns 
                if self._normalize_col_name(c) in cols_to_except and self._normalize_col_name(c) not in processed_columns
            ]

            if existing_cols_to_drop:
                df = df.drop(*existing_cols_to_drop)

        return df

    def _normalize_col_name(self, col_ref: str) -> str:
        """Extracts column identifier excluding alias prefixes or backticks."""
        clean = col_ref.replace("`", "").strip()
        return clean.split(".")[-1]