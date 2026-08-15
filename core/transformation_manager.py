import logging
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
        self.table_name = Helper.parse_table_name(parsed_config.get("table", "unknown_table"))
        self.jobs = parsed_config.get("jobs", {})

    def apply_transformations(self) -> DataFrame:
        if not self.jobs:
            logger.warning(f"No transformation jobs configured for table '{self.table_name}'. Returning original DataFrame.")
            return self.df

        logger.info(f"Applying SQL transformation pipeline for table '{self.table_name}'...")

        job_dfs: Dict[str, DataFrame] = {}
        created_temp_views: List[str] = []

        base_view_name = f"tmp_src_{self.table_name}_{id(self.df)}"
        self.df.createOrReplaceTempView(base_view_name)
        created_temp_views.append(base_view_name)

        current_view_name = base_view_name
        current_df = self.df
        current_alias = self.main_alias

        try:
            for job_name, job_meta in self.jobs.items():
                source_step = job_meta.get("depend_on")

                if source_step:
                    source_view = f"tmp_job_{source_step}_{id(self.df)}"
                    source_df = job_dfs[source_step]
                else:
                    source_view = current_view_name
                    source_df = current_df

                select_exprs: List[str] = list(job_meta.get("exprs", []))
                joins_meta = job_meta.get("joins", [])
                join_sql_clauses = [j["sql"] for j in joins_meta if isinstance(j, dict) and "sql" in j]

                # --- 1. Handle select_the_rest (Includes Main + Joined Tables) ---
                select_rest_cfg = job_meta.get("select_the_rest", {})
                if select_rest_cfg and select_rest_cfg.get("enable"):
                    excluded_cols = set(select_rest_cfg.get("except", []))

                    # أ) أخذ باقي أعمدة الجدول الرئيسي
                    main_rest = [
                        f"`{current_alias}`.`{c}` AS `{c}`"
                        for c in source_df.columns
                        if c not in excluded_cols
                    ]
                    select_exprs.extend(main_rest)

                    # ب) أخذ باقي أعمدة الجداول المضمومة (Joined Tables) تلقائياً
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

                # --- 2. Clean & Deduplicate SELECT Expressions to prevent Syntax Errors ---
                cleaned_exprs = []
                seen_exprs = set()

                for expr in select_exprs:
                    if isinstance(expr, str):
                        # تنظيف الفواصل المسربة في النهاية
                        clean_e = expr.strip().rstrip(",")
                        if clean_e and clean_e not in seen_exprs:
                            cleaned_exprs.append(clean_e)
                            seen_exprs.add(clean_e)

                # حماية: إذا كانت القائمة فارغة نهائياً، نختار كل أعمدة الجدول الأساسي
                if not cleaned_exprs:
                    cleaned_exprs = [f"`{current_alias}`.*"]

                select_clause = ",\n    ".join(cleaned_exprs)
                joins_sql = "\n".join(join_sql_clauses) if join_sql_clauses else ""

                # --- 3. Build & Execute SQL Statement ---
                sql_query = f"""SELECT
    {select_clause}
FROM {source_view} AS `{current_alias}`
{joins_sql}""".strip()

                logger.debug(f"Executing SQL for Job '{job_name}':\n{sql_query}")

                step_df = self.spark.sql(sql_query)

                current_view_name = f"tmp_job_{job_name}_{id(self.df)}"
                step_df.createOrReplaceTempView(current_view_name)
                created_temp_views.append(current_view_name)

                job_dfs[job_name] = step_df
                current_df = step_df
                current_alias = job_name

            logger.info(f"Successfully executed transformation pipeline for '{self.table_name}'.")
            return current_df

        finally:
            for view_name in created_temp_views:
                self.spark.catalog.dropTempView(view_name)