import logging
import re
from typing import Dict, Any, List, Union
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
        
        # Normalize jobs config (supports list or dict structures)
        raw_jobs = parsed_config.get("jobs", [])
        if isinstance(raw_jobs, list):
            self.jobs = {}
            for item in raw_jobs:
                if isinstance(item, dict):
                    self.jobs.update(item)
        else:
            self.jobs = raw_jobs

        self.created_temp_views: List[str] = []

    def apply_transformations(self) -> DataFrame:
        if not self.jobs:
            logger.warning(f"No transformation jobs configured for table '{self.table_name}'. Returning original DataFrame.")
            return self.df

        logger.info(f"Applying SQL transformation pipeline for table '{self.table_name}'...")

        # 1. Register root source temp view
        base_view_name = f"tmp_src_{self.table_name}"
        self.df.createOrReplaceTempView(base_view_name)
        self.created_temp_views.append(base_view_name)

        current_df = self.df
        current_source_view = base_view_name
        job_dfs: Dict[str, DataFrame] = {}

        for job_name, job_meta in self.jobs.items():
            clean_job_name = re.sub(r"[^a-zA-Z0-9_]", "_", str(job_name))
            
            # Resolve input DataFrame/View dependency if explicitly defined
            source_step = job_meta.get("depend_on")
            if source_step:
                clean_step_name = re.sub(r"[^a-zA-Z0-9_]", "_", str(source_step))
                source_view = f"tmp_job_{self.table_name}_{clean_step_name}"
                source_df = job_dfs[source_step]
            else:
                source_view = current_source_view
                source_df = current_df

            joins_meta = job_meta.get("joins", [])
            rules_meta = job_meta.get("rules", [])

            # =================================================================
            # STAGE 1: EXECUTE JOINS FIRST (Creates intermediate joined relation)
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
                
                # Rules now run against the joined dataset
                eval_view = join_stage_view
                eval_df = join_df
            else:
                eval_view = source_view
                eval_df = source_df

            # =================================================================
            # STAGE 2: EVALUATE RULES & PROJECTIONS AGAINST JOINED DATA
            # =================================================================
            step_df = self._build_and_run_rules(
                source_view=eval_view,
                source_df=eval_df,
                rules_meta=rules_meta
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
        """Helper to evaluate join clauses and project combined columns into a temp view."""
        broadcast_targets = []
        join_sql_clauses = []

        for j in joins_meta:
            if not isinstance(j, dict):
                continue

            tbl_name = j.get("table")
            tbl_alias = j.get("alias") or tbl_name
            how = j.get("how", "left").upper()
            on_clause = j.get("on_clause") or j.get("on")

            if j.get("broadcast"):
                broadcast_targets.append(f"`{tbl_alias}`")

            if j.get("sql"):
                join_sql_clauses.append(j["sql"])
            elif tbl_name and on_clause:
                join_sql_clauses.append(f"{how} JOIN `{tbl_name}` AS `{tbl_alias}` ON {on_clause}")

        hint_clause = f"/*+ BROADCAST({', '.join(broadcast_targets)}) */ " if broadcast_targets else ""

        # Select primary table and joined table attributes
        select_clause = f"`{self.main_alias}`.*"
        
        joined_selects = []
        for j in joins_meta:
            if isinstance(j, dict):
                tbl_alias = j.get("alias") or j.get("table")
                if tbl_alias:
                    joined_selects.append(f"`{tbl_alias}`.*")

        if joined_selects:
            select_clause += ", " + ", ".join(joined_selects)

        join_sql = "\n".join(join_sql_clauses)
        sql_query = f"SELECT {hint_clause} {select_clause} FROM `{source_view}` AS `{self.main_alias}` {join_sql}".strip()

        logger.debug(f"Executing Join Stage SQL:\n{sql_query}")
        return self.spark.sql(sql_query)

    def _build_and_run_rules(
        self, 
        source_view: str, 
        source_df: DataFrame, 
        rules_meta: List[Dict[str, Any]]
    ) -> DataFrame:
        """Helper to expand rules, wildcards, expressions, and handle select_the_rest."""
        select_exprs: List[str] = []
        excluded_cols: set = set()
        explicitly_handled_cols: set = set()

        for rule in rules_meta:
            if not isinstance(rule, dict):
                continue

            # 1. Multi-Column Template Expression Rule
            if "expression" in rule and "columns" in rule:
                tmpl = rule["expression"]
                for col_ref in rule["columns"]:
                    clean_col = self._clean_col_name(col_ref)
                    expr_str = tmpl.replace("${col}", f"`{col_ref}`")
                    select_exprs.append(f"{expr_str} AS `{clean_col}`")
                    explicitly_handled_cols.add(clean_col)
                    explicitly_handled_cols.add(col_ref)

            # 2. Wildcard Struct/Table Rule (e.g. run: location.*)
            elif "run" in rule:
                run_pattern = rule["run"]
                clean_pattern = run_pattern.replace("c.", "").replace(f"{self.main_alias}.", "")
                select_exprs.append(f"`{clean_pattern}`")

            # 3. Explicit Single Column/Case Expression
            elif "column" in rule and "expression" in rule:
                col_name = rule["column"]
                expr_str = rule["expression"].strip()
                select_exprs.append(f"({expr_str}) AS `{col_name}`")
                explicitly_handled_cols.add(col_name)

            # 4. Handle select_the_rest config block
            elif "select_the_rest" in rule:
                rest_cfg = rule["select_the_rest"]
                if rest_cfg.get("enable"):
                    raw_except = rest_cfg.get("except", [])
                    for exc in raw_except:
                        excluded_cols.add(exc)
                        excluded_cols.add(self._clean_col_name(exc))

        # --- Expand select_the_rest automatically if enabled ---
        for col_name in source_df.columns:
            if col_name not in excluded_cols and col_name not in explicitly_handled_cols:
                select_exprs.append(f"`{col_name}`")

        select_clause = ",\n    ".join(select_exprs) if select_exprs else "*"
        sql_query = f"SELECT\n    {select_clause}\nFROM `{source_view}`".strip()

        logger.debug(f"Executing Rules Stage SQL:\n{sql_query}")
        return self.spark.sql(sql_query)

    def _clean_col_name(self, col_ref: str) -> str:
        """Strips alias prefixing (e.g., 'c.customer_id' -> 'customer_id')."""
        return col_ref.split(".")[-1]

    def cleanup(self):
        """Removes registered session views from Spark catalog."""
        for view_name in self.created_temp_views:
            try:
                self.spark.catalog.dropTempView(view_name)
            except Exception as e:
                logger.debug(f"Failed to drop temp view {view_name}: {e}")
        self.created_temp_views.clear()