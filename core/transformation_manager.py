import logging
from typing import Dict, Any
from pyspark.sql import DataFrame

from yamlpipe.utility.helper import Helper
from yamlpipe.registry.transformation_registry import TransformationRegistry

logger = logging.getLogger("TransformationManager")


class TransformationManager:

    def __init__(self, parsed_config: Dict[str, Any], df: DataFrame):
        """
        Initializes TransformationManager with configuration and main DataFrame.
        Extracts SparkSession dynamically from the DataFrame.
        """
        if not isinstance(parsed_config, dict):
            raise TypeError(f"[TransformationManager Error] Expected dict, got {type(parsed_config).__name__}")

        if not isinstance(df, DataFrame):
            raise TypeError(f"[TransformationManager Error] Expected PySpark DataFrame, got {type(df).__name__}")

        self.parsed_config = parsed_config
        self.df = df
        
        # Extract SparkSession directly from DataFrame context
        self.spark = df.sparkSession
        
        self.main_alias = parsed_config.get("alias", "main_tbl")
        self.table_name = Helper.parse_table_name(parsed_config.get("table", "unknown_table"))
        self.jobs = parsed_config.get("jobs", {})

    def apply_transformations(self) -> DataFrame:
        """
        Executes multi-step dynamic PySpark SQL queries supporting JOINS and custom rules.
        """
        if not self.jobs:
            logger.warning(f"No transformation jobs configured for table '{self.table_name}'. Returning original DataFrame.")
            return self.df

        logger.info(f"Applying SQL transformation pipeline for table '{self.table_name}'...")

        # Keep track of generated DataFrames per job step
        job_dfs: Dict[str, DataFrame] = {}
        
        base_view_name = f"tmp_src_{self.table_name}_{id(self.df)}"
        self.df.createOrReplaceTempView(base_view_name)

        current_view_name = base_view_name
        current_df = self.df
        current_alias = self.main_alias

        try:
            for job_name, job_meta in self.jobs.items():
                source_step = job_meta.get("depend_on")
                
                # Resolve source View and source DataFrame for the current step
                if source_step:
                    source_view = f"tmp_job_{source_step}"
                    source_df = job_dfs[source_step]
                else:
                    source_view = current_view_name
                    source_df = current_df
                
                # 1. Build Expressions (SELECT)
                select_exprs = []
                rules = job_meta.get("rules", [])
                for rule in rules:
                    select_exprs.append(TransformationRegistry.build_rule_expr(rule))

                # 2. Handle 'select_the_rest' efficiently using in-memory DataFrame schema
                select_rest_cfg = job_meta.get("select_the_rest", {})
                if select_rest_cfg and select_rest_cfg.get("enable"):
                    excluded_cols = set(select_rest_cfg.get("except", []))
                    from_alias = select_rest_cfg.get("from_alias", current_alias)
                    
                    rest_exprs = [
                        f"{from_alias}.`{c}` AS `{c}`" 
                        for c in source_df.columns 
                        if c not in excluded_cols
                    ]
                    select_exprs.extend(rest_exprs)

                select_clause = ",\n    ".join(select_exprs)

                # 3. Build Joins
                join_clauses = []
                for join_cfg in job_meta.get("joins", []):
                    join_clauses.append(TransformationRegistry.build_join_clause(join_cfg))
                
                joins_sql = "\n".join(join_clauses)

                # 4. Construct and Execute SQL Query using self.spark
                sql_query = f"""
                SELECT 
                    {select_clause}
                FROM {source_view} AS `{current_alias}`
                {joins_sql}
                """

                logger.debug(f"Executing SQL for Job '{job_name}':\n{sql_query}")

                step_df = self.spark.sql(sql_query)
                
                # Register temporary view for downstream SQL queries
                current_view_name = f"tmp_job_{job_name}"
                step_df.createOrReplaceTempView(current_view_name)
                
                # Cache step DataFrame references
                job_dfs[job_name] = step_df
                current_df = step_df
                current_alias = job_name

            logger.info(f"Successfully executed transformation pipeline for '{self.table_name}'.")
            return current_df

        finally:
            # Cleanup temporary source view
            self.spark.catalog.dropTempView(base_view_name)