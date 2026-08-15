import logging
import re
from typing import Dict, Any, List, Set
from pyspark.sql import DataFrame

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
        self.jobs = parsed_config.get("jobs", {})
        self.created_temp_views: List[str] = []

    def apply_transformations(self) -> DataFrame:
        if not self.jobs:
            logger.warning(f"No transformation jobs configured for table '{self.table_name}'. Returning original DataFrame.")
            return self.df

        logger.info(f"Applying SQL transformation pipeline for table '{self.table_name}'...")

        # Register root source temp view
        base_view_name = f"tmp_src_{self.table_name}"
        self.df.createOrReplaceTempView(base_view_name)
        self.created_temp_views.append(base_view_name)

        current_df = self.df
        current_source_view = base_view_name
        job_dfs: Dict[str, DataFrame] = {}

        for job_name, job_meta in self.jobs.items():
            clean_job_name = re.sub(r"[^a-zA-Z0-9_]", "_", str(job_name))
            
            source_step = job_meta.get("depend_on")
            if source_step:
                clean_step_name = re.sub(r"[^a-zA-Z0-9_]", "_", str(source_step))
                source_view = f"tmp_job_{self.table_name}_{clean_step_name}"
                source_df = job_dfs[source_step]
            else:
                source_view = current_source_view
                source_df = current_df

            joins_meta = job_meta.get("joins", [])

            # =================================================================
            # STAGE 1: EXECUTE JOINS FIRST
            # =================================================================
            if joins_meta:
                join_stage_view = f"tmp_join_stage_{self.table_name}_{clean_job_name}"
                join_df = self._build_and_run_joins(
                    source_view=source_view,
                    source_df=source_df,
                    joins_meta=joins_meta
                )
                join_df.createOrReplaceTempView(join_stage_view)
                self.created_temp_views.append(join_stage_view)
                
                eval_view = join_stage_view
                eval_df = join_df
            else:
                eval_view = source_view
                eval_df = source_df

            # =================================================================
            # STAGE 2: BUILD SELECT PROJECTIONS & SELECT_THE_REST
            # =================================================================
            step_df = self._build_and_run_projections(
                eval_view=eval_view,
                eval_df=eval_df,
                job_meta=job_meta
            )

            job_view_name = f"tmp_job_{self.table_name}_{clean_job_name}"
            step_df.createOrReplaceTempView(job_view_name)
            self.created_temp_views.append(job_view_name)

            job_dfs[job_name] = step_df
            current_df = step_df
            current_source_view = job_view_name

        logger.info(f"Successfully executed transformation pipeline for '{self.table_name}'.")
        return current_df

    def _build_and_run_joins(
        self, 
        source_view: str, 
        source_df: DataFrame, 
        joins_meta: List[Dict[str, Any]]
    ) -> DataFrame:
        """Runs joins and flattens base table and joined columns into a single relation."""
        broadcast_targets = []
        join_sql_clauses = []

        for j in joins_meta:
            if not isinstance(j, dict):
                continue

            tbl_alias = j.get("alias") or j.get("table")
            if j.get("broadcast") and tbl_alias:
                broadcast_targets.append(f"`{tbl_alias}`")

            if j.get("sql"):
                join_sql_clauses.append(j["sql"])

        hint_clause = f"/*+ BROADCAST({', '.join(broadcast_targets)}) */ " if broadcast_targets else ""

        # Select all columns from main table and joined tables
        select_clause = f"`{self.main_alias}`.*"
        for j in joins_meta:
            if isinstance(j, dict):
                tbl_alias = j.get("alias") or j.get("table")
                if tbl_alias:
                    select_clause += f", `{tbl_alias}`.*"

        join_sql = "\n".join(join_sql_clauses)
        sql_query = f"SELECT {hint_clause} {select_clause} FROM `{source_view}` AS `{self.main_alias}` {join_sql}".strip()

        logger.debug(f"Executing Join Stage SQL:\n{sql_query}")
        return self.spark.sql(sql_query)

    def _build_and_run_projections(
        self, 
        eval_view: str, 
        eval_df: DataFrame, 
        job_meta: Dict[str, Any]
    ) -> DataFrame:
        """Processes expressions, resolves select_the_rest without alias duplication."""
        raw_exprs: List[str] = list(job_meta.get("exprs", []))
        explicit_handled_raw = job_meta.get("explicitly_handled_cols", [])
        select_rest_cfg = job_meta.get("select_the_rest", {})

        select_exprs: List[str] = []
        seen_output_cols: Set[str] = set()

        # Normalize explicit handled column names (strips "c.", "s.")
        handled_cols_clean: Set[str] = {
            self._normalize_col_name(col) for col in explicit_handled_raw
        }

        # 1. Process explicit SQL expressions
        for expr in raw_exprs:
            if not isinstance(expr, str):
                continue
            
            clean_expr = expr.strip().rstrip(",")
            select_exprs.append(clean_expr)

            # Extract output alias (e.g. 'AS `customer_id`' -> 'customer_id')
            match = re.search(r"AS\s+[`]?([a-zA-Z0-9_]+)[`]?$", clean_expr, re.IGNORECASE)
            if match:
                col_alias = match.group(1)
                seen_output_cols.add(self._normalize_col_name(col_alias))

        # Merge handled columns into tracked output columns
        seen_output_cols.update(handled_cols_clean)

        # 2. Process select_the_rest safely against the evaluated DataFrame
        if select_rest_cfg and select_rest_cfg.get("enable"):
            raw_except = select_rest_cfg.get("except", [])
            
            # Normalize except list
            except_clean: Set[str] = {
                self._normalize_col_name(exc) for exc in raw_except
            }

            for col in eval_df.columns:
                normalized_col = self._normalize_col_name(col)

                # Include column ONLY if it's not excluded and not explicitly mapped already
                if normalized_col not in except_clean and normalized_col not in seen_output_cols:
                    select_exprs.append(f"`{col}`")
                    seen_output_cols.add(normalized_col)

        select_clause = ",\n    ".join(select_exprs) if select_exprs else "*"
        sql_query = f"SELECT\n    {select_clause}\nFROM `{eval_view}`".strip()

        logger.debug(f"Executing Projection Stage SQL:\n{sql_query}")
        return self.spark.sql(sql_query)

    def _normalize_col_name(self, col_ref: str) -> str:
        """Extracts simple column identifier without table prefixes or backticks."""
        clean = col_ref.replace("`", "").strip()
        return clean.split(".")[-1]

    def cleanup(self):
        """Removes registered session views from Spark catalog."""
        for view_name in self.created_temp_views:
            try:
                self.spark.catalog.dropTempView(view_name)
            except Exception as e:
                logger.debug(f"Failed to drop temp view {view_name}: {e}")
        self.created_temp_views.clear()