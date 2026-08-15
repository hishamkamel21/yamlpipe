import logging
import re
from typing import Dict, Any, List
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
        self.main_alias = parsed_config.get("alias") or "main_tbl"
        
        raw_table = parsed_config.get("table", "unknown_table")
        self.table_name = re.sub(r"[^a-zA-Z0-9_]", "_", Helper.parse_table_name(raw_table))
        self.jobs = parsed_config.get("jobs", {})
        self.created_temp_views: List[str] = []

    def apply_transformations(self) -> DataFrame:
        if not self.jobs:
            logger.warning(f"No transformation jobs configured for table '{self.table_name}'. Returning original DataFrame.")
            return self.df

        logger.info(f"Applying SQL transformation pipeline for table '{self.table_name}'...")

        job_dfs: Dict[str, DataFrame] = {}

        # 1. Unique base temp view name registered in Catalog
        base_view_name = f"tmp_src_{self.table_name}"
        self.df.createOrReplaceTempView(base_view_name)
        self.created_temp_views.append(base_view_name)

        current_source_view = f"`{base_view_name}`"
        current_df = self.df
        current_alias = self.main_alias

        for job_name, job_meta in self.jobs.items():
            clean_job_name = re.sub(r"[^a-zA-Z0-9_]", "_", str(job_name))
            source_step = job_meta.get("depend_on")

            if source_step:
                clean_step_name = re.sub(r"[^a-zA-Z0-9_]", "_", str(source_step))
                source_view_name = f"tmp_job_{self.table_name}_{clean_step_name}"
                source_view = f"`{source_view_name}`"
                source_df = job_dfs[source_step]
            else:
                source_view = current_source_view
                source_df = current_df

            select_exprs: List[str] = list(job_meta.get("exprs", []))
            joins_meta = job_meta.get("joins", [])
            join_sql_clauses = [j["sql"] for j in joins_meta if isinstance(j, dict) and "sql" in j]

            # --- Broadcast Hint Logic ---
            broadcast_targets = []
            for j in joins_meta:
                if isinstance(j, dict) and j.get("broadcast"):
                    target = j.get("alias") or j.get("table")
                    if target:
                        broadcast_targets.append(f"`{target}`" if "`" not in target else target)

            hint_clause = f"/*+ BROADCAST({', '.join(broadcast_targets)}) */ " if broadcast_targets else ""

            # --- Handle select_the_rest ---
            select_rest_cfg = job_meta.get("select_the_rest", {})
            if select_rest_cfg and select_rest_cfg.get("enable"):
                excluded_cols = set(select_rest_cfg.get("except", []))

                # Main table rest
                main_rest = [
                    f"`{current_alias}`.`{c}` AS `{c}`"
                    for c in source_df.columns
                    if c not in excluded_cols
                ]
                select_exprs.extend(main_rest)

                # Joined tables rest
                if select_rest_cfg.get("include_joined_tables", True) and joins_meta:
                    for join_item in joins_meta:
                        tbl_name = join_item.get("table")
                        tbl_alias = join_item.get("alias") or tbl_name

                        if tbl_name and self.spark.catalog.tableExists(tbl_name):
                            joined_df = self.spark.table(tbl_name)
                            joined_rest = [
                                f"`{tbl_alias}`.`{c}` AS `{c}`"
                                for c in joined_df.columns
                                if c not in excluded_cols and f"`{tbl_alias}`.`{c}`" not in select_exprs
                            ]
                            select_exprs.extend(joined_rest)

            # --- Clean & Deduplicate SELECT Expressions ---
            cleaned_exprs = []
            seen_exprs = set()

            for expr in select_exprs:
                if isinstance(expr, str):
                    clean_e = expr.strip().rstrip(",")
                    if clean_e and clean_e not in seen_exprs:
                        cleaned_exprs.append(clean_e)
                        seen_exprs.add(clean_e)

            if not cleaned_exprs:
                cleaned_exprs = [f"`{current_alias}`.*"]

            select_clause = ",\n    ".join(cleaned_exprs)

            raw_joins_sql = "\n".join(join_sql_clauses) if join_sql_clauses else ""
            cleaned_joins_sql = re.sub(r"/\*\+\s*.*?\*/", "", raw_joins_sql)

            # --- Build & Execute SQL ---
            sql_query = f"""SELECT {hint_clause}
    {select_clause}
FROM {source_view} AS `{current_alias}`
{cleaned_joins_sql}""".strip()

            logger.debug(f"Executing SQL for Job '{job_name}':\n{sql_query}")

            step_df = self.spark.sql(sql_query)

            job_view_name = f"tmp_job_{self.table_name}_{clean_job_name}"
            step_df.createOrReplaceTempView(job_view_name)
            self.created_temp_views.append(job_view_name)

            job_dfs[job_name] = step_df
            current_df = step_df
            current_source_view = f"`{job_view_name}`"
            current_alias = clean_job_name

        logger.info(f"Successfully executed transformation pipeline for '{self.table_name}'.")
        return current_df

    def cleanup(self):
        """
        Call this method explicitly after executing actions on the output DataFrame 
        to clean up registered session views.
        """
        for view_name in self.created_temp_views:
            try:
                self.spark.catalog.dropTempView(view_name)
            except Exception as e:
                logger.debug(f"Failed to drop temp view {view_name}: {e}")
        self.created_temp_views.clear()