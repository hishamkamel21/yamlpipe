import logging
from typing import Dict, Any
from pyspark.sql import DataFrame

from yamlpipe.utility.helper import Helper

logger = logging.getLogger("TransformationManager")


class TransformationManager:

    def __init__(self, parsed_config: Dict[str, Any], df: DataFrame):
        """
        Initializes TransformationManager with output from TransformationParser and input DataFrame.
        """
        if not isinstance(parsed_config, dict):
            raise TypeError(f"[TransformationManager Error] Expected dict, got {type(parsed_config).__name__}")

        if not isinstance(df, DataFrame):
            raise TypeError(f"[TransformationManager Error] Expected PySpark DataFrame, got {type(df).__name__}")

        self.parsed_config = parsed_config
        self.df = df
        self.table_name = Helper.parse_table_name(parsed_config.get("table", "unknown_table"))
        self.jobs = parsed_config.get("jobs", {})

    def apply_transformations(self) -> DataFrame:
        """
        Executes transformation pipeline step-by-step against the PySpark DataFrame.
        Dynamically calculates 'select_the_rest' based on upstream DataFrame schema at runtime.
        """
        if not self.jobs:
            logger.warning(f"No transformation jobs configured for table '{self.table_name}'. Returning original DataFrame.")
            return self.df

        logger.info(f"Applying transformation pipeline for table '{self.table_name}'...")
        
        job_outputs: Dict[str, DataFrame] = {}
        current_df = self.df

        try:
            for job_name, job_meta in self.jobs.items():
                source_step = job_meta.get("depend_on")
                base_exprs = list(job_meta.get("exprs", []))
                select_rest_cfg = job_meta.get("select_the_rest")

                # Resolve input DataFrame for current step
                source_df = job_outputs.get(source_step, current_df)
                input_columns = source_df.columns

                # Build runtime select expressions
                final_select_exprs = list(base_exprs)

                if select_rest_cfg and select_rest_cfg.get("enable"):
                    excluded_cols = set(select_rest_cfg.get("except", []))
                    
                    rest_exprs = [
                        f"`{c}` AS `{c}`" 
                        for c in input_columns 
                        if c not in excluded_cols
                    ]
                    final_select_exprs.extend(rest_exprs)

                logger.debug(f"Executing job '{job_name}' with expressions: {final_select_exprs}")
                
                # Apply PySpark transformations
                current_df = source_df.selectExpr(*final_select_exprs)
                
                # Cache output DataFrame for downstream step dependencies
                job_outputs[job_name] = current_df

            logger.info(f"Successfully finished all transformation jobs for '{self.table_name}'.")
            return current_df

        except Exception as e:
            logger.error(f"Transformation execution failed at runtime: {str(e)}")
            raise RuntimeError(f"PySpark transformation pipeline failed for '{self.table_name}': {str(e)}") from e