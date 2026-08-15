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
        jobs = self.parsed_config.get("job", {})
        if not jobs:
            logger.warning(f"No transformation jobs configured for table '{self.table_name}'. Returning original DataFrame.")
            return self.df

        # Extract primary job definition
        job_meta = jobs.get("base") or next(iter(jobs.values()))
        
        logger.info(f"Applying DataFrame transformations for table '{self.table_name}'...")

        current_df = self.df.alias(self.main_alias)

        # STAGE 1: EXECUTE JOINS
        joins_meta = job_meta.get("joins", [])
        if joins_meta:
            current_df = self._apply_joins(current_df, joins_meta)

        # STAGE 2: APPLY EXPRESSIONS & PROJECTIONS
        current_df = self._apply_projections(current_df, job_meta)

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
            sql_clause = j.get("sql", "")
            on_clause = j.get("on_clause")

            if not on_clause and " ON " in sql_clause.upper():
                on_clause = re.split(r"\s+ON\s+", sql_clause, flags=re.IGNORECASE)[-1]

            if on_clause:
                join_condition = F.expr(on_clause)
                df = df.join(right_df, on=join_condition, how=how)
            else:
                df = df.join(right_df, how=how)

        return df

    def _apply_projections(self, df: DataFrame, job_meta: Dict[str, Any]) -> DataFrame:
        """Applies explicit column rules, star expansions, and select_the_rest pruning."""
        raw_exprs: List[str] = job_meta.get("exprs", [])
        explicit_handled_raw = job_meta.get("explicitly_handled_cols", [])
        select_rest_cfg = job_meta.get("select_the_rest", {})

        processed_columns: Set[str] = set()

        # 1. Process Explicit Expressions and Structural Operations
        for expr_str in raw_exprs:
            if not isinstance(expr_str, str):
                continue

            clean_expr = expr_str.strip().rstrip(",")

            # Handle struct or table expansion (e.g., location.*)
            if clean_expr.endswith(".*"):
                col_prefix = clean_expr[:-2]
                if col_prefix in df.columns:
                    for field in df.schema[col_prefix].dataType.names:
                        out_col = f"{col_prefix}_{field}"
                        df = df.withColumn(out_col, F.col(f"`{col_prefix}`.{field}"))
                        processed_columns.add(out_col)
                continue

            # Handle Expressions with Aliases (e.g., expr AS alias)
            alias_match = re.search(r"^(.*?)\s+AS\s+[`]?([a-zA-Z0-9_]+)[`]?$", clean_expr, re.IGNORECASE | re.DOTALL)
            if alias_match:
                sql_expression, target_col = alias_match.group(1).strip(), alias_match.group(2).strip()
                df = df.withColumn(target_col, F.expr(sql_expression))
                processed_columns.add(target_col)
            else:
                # Direct SQL Select Expression fallback
                col_name = self._normalize_col_name(clean_expr)
                df = df.withColumn(col_name, F.expr(clean_expr))
                processed_columns.add(col_name)

        # 2. Process select_the_rest Exclusions
        if select_rest_cfg and select_rest_cfg.get("enable"):
            raw_except = select_rest_cfg.get("except", [])
            
            # Combine 'except' fields and explicit output columns into exclusion set
            cols_to_drop: Set[str] = {
                self._normalize_col_name(c) for c in raw_except + explicit_handled_raw
            }

            # Retain newly created/processed columns
            cols_to_drop = cols_to_drop - processed_columns

            existing_cols_to_drop = [c for c in df.columns if c in cols_to_drop or self._normalize_col_name(c) in cols_to_drop]
            
            if existing_cols_to_drop:
                df = df.drop(*existing_cols_to_drop)

        return df

    def _normalize_col_name(self, col_ref: str) -> str:
        """Extracts column identifier excluding alias prefixes or backticks."""
        clean = col_ref.replace("`", "").strip()
        return clean.split(".")[-1]