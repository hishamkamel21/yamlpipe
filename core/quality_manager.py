import logging
import re
from typing import Dict, Any, Tuple, Union
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, col, size
from pyspark.storagelevel import StorageLevel

from yamlpipe.registries.schema_quality_registry import SchemaQualityRegistry
from yamlpipe.core.monitor_manager import MonitorManager
from yamlpipe.utility.helper import Helper

logger = logging.getLogger("QualityManager")


class QualityManager:

    def __init__(self, parsed_config: Dict[str, Any], df: DataFrame):
        """
        Initializes QualityManager with parsed quality checks configuration and target DataFrame.

        :param parsed_config: Dictionary output from QualityChecksParser.
        :param df: Input PySpark DataFrame to run quality rules against.
        """
        if not isinstance(parsed_config, dict):
            raise TypeError(
                f"[QualityManager Init Error] 'parsed_config' must be a dictionary, "
                f"got '{type(parsed_config).__name__}'."
            )

        if not isinstance(df, DataFrame):
            raise TypeError(
                f"[QualityManager Init Error] 'df' must be a valid PySpark DataFrame, "
                f"got '{type(df).__name__}'."
            )

        self.parsed_config = parsed_config
        self.df = df

        # Clean catalog.schema.table or path string into a unified table identifier
        raw_table_identifier = (
            parsed_config.get("table") 
            or parsed_config.get("table_name") 
            or "unknown_table"
        )
        self.table_name = Helper.parse_table_name(raw_table_identifier)

        # Extract structured layers directly from QualityChecksParser output
        self.schema_checks = parsed_config.get("schema_checks", [])
        self.columns_checks = parsed_config.get("columns_checks", {"error_expr": [], "warn_expr": []})
        self.table_checks = parsed_config.get("table_checks", {"expr": "", "temp_views_to_create": []})

        # Internal state tracking
        self.flags = []
        self.final_df = None

    def apply_checks(
        self,
        batch_id: str = None,
        action: str = "split",
        show_monitor_metrics: bool = False,
        persist_df: bool = True,
        storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK
    ) -> Union[Tuple[DataFrame, DataFrame], DataFrame, Tuple[Any, ...]]:
        """
        Main Execution Engine. Runs Schema, Column, and Table quality rules against the DataFrame.
        """
        current_df = self.df

        # ---------------------------------------------------------------------
        # 1. Apply Schema-Level Checks (via SchemaQualityRegistry)
        # ---------------------------------------------------------------------
        if self.schema_checks:
            try:
                current_df = SchemaQualityRegistry.apply_schema_checks(current_df, self.schema_checks)
                logger.info("Successfully applied schema checks.")
            except Exception as e:
                logger.error(f"Failed during schema checks execution: {str(e)}")
                raise RuntimeError(f"Schema Quality evaluation failed: {str(e)}") from e

        # ---------------------------------------------------------------------
        # 2. Apply Column-Level Checks (Inject Errors & Warnings Arrays)
        # ---------------------------------------------------------------------
        error_exprs = self.columns_checks.get("error_expr", [])
        warning_exprs = self.columns_checks.get("warn_expr", [])

        try:
            if error_exprs:
                errors_sql = f"array_compact(array_flatten(array({', '.join(error_exprs)})))"
            else:
                errors_sql = "array().cast('array<string>')"

            if warning_exprs:
                warnings_sql = f"array_compact(array_flatten(array({', '.join(warning_exprs)})))"
            else:
                warnings_sql = "array().cast('array<string>')"

            current_df = (
                current_df
                .withColumn("Errors", expr(errors_sql))
                .withColumn("Warnings", expr(warnings_sql))
            )
        except Exception as e:
            logger.error(f"Failed to compile column check expressions in PySpark: {str(e)}")
            raise RuntimeError(
                f"Column Quality evaluation failed. Check SQL expressions. Error: {str(e)}"
            ) from e

        # ---------------------------------------------------------------------
        # 3. Apply Table-Level Checks (Register Temp Views & Run SQL)
        # ---------------------------------------------------------------------
        table_expr_str = self.table_checks.get("expr", "")
        temp_views = self.table_checks.get("temp_views_to_create", [])

        if table_expr_str and table_expr_str.strip():
            spark = current_df.sparkSession

            # Step 3a: Create external reference temporary views (Lookups / Foreign Keys)
            for view_meta in temp_views:
                view_name = view_meta.get("view_name")
                raw_ref_table = view_meta.get("table")
                path = view_meta.get("path")
                fmt = view_meta.get("format", "delta")

                if view_name:
                    if raw_ref_table:
                        # Support catalog.schema.table syntax via Helper.parse_table_name
                        cleaned_ref_table = Helper.parse_table_name(raw_ref_table)
                        logger.info(f"Registering temp view '{view_name}' from catalog table '{cleaned_ref_table}'.")
                        spark.read.table(cleaned_ref_table).createOrReplaceTempView(view_name)
                    elif path:
                        logger.info(f"Registering temp view '{view_name}' from path '{path}'.")
                        spark.read.format(fmt).load(path).createOrReplaceTempView(view_name)

            # Step 3b: Create local source temp view named 'tmp_src' required by Registry SQL expressions
            current_df.createOrReplaceTempView("tmp_src")

            # Step 3c: Execute compiled SQL expressions against 'tmp_src'
            try:
                sql_query = f"SELECT *, {table_expr_str} FROM tmp_src"
                current_df = spark.sql(sql_query)

                # Extract and register output flag names for monitoring and splitting logic
                self._extract_and_register_table_flags(table_expr_str)

            except Exception as e:
                logger.error(f"Failed to execute table_checks query: {str(e)}")
                raise RuntimeError(f"Table Quality evaluation failed: {str(e)}") from e

        self.final_df = current_df

        # ---------------------------------------------------------------------
        # 4. Persistence Management
        # ---------------------------------------------------------------------
        if persist_df and not self.final_df.is_cached:
            self.final_df.persist(storage_level)
            logger.info(f"Persisted evaluated DataFrame for table '{self.table_name}' using {storage_level}")

        # ---------------------------------------------------------------------
        # 5. Observability Metrics Generation
        # ---------------------------------------------------------------------
        metrics = None
        if show_monitor_metrics:
            metrics = MonitorManager.generate_metrics(
                df=self.final_df,
                table_name=self.table_name,
                batch_id=batch_id,
                flags=self.flags
            )

        # ---------------------------------------------------------------------
        # 6. Dataset Routing (Keep vs Split)
        # ---------------------------------------------------------------------
        if action.lower() == "keep":
            if show_monitor_metrics:
                return self.final_df, metrics
            return self.final_df

        valid_df, invalid_df = self.split_df(df=self.final_df)

        if show_monitor_metrics:
            return valid_df, invalid_df, metrics

        return valid_df, invalid_df

    def _extract_and_register_table_flags(self, table_expr_str: str):
        """
        Parses output column aliases from Table SQL expressions and registers flag metadata.
        """
        aliases = re.findall(r"AS\s+[`]?([a-zA-Z0-9_]+)[`]?", table_expr_str, re.IGNORECASE) 

        for flag_name in aliases:
            is_freshness = "freshness" in flag_name.lower() or "lag" in flag_name.lower()
            self.flags.append({
                "flag_name": flag_name,
                "on_split_keep": is_freshness,
                "is_freshness": is_freshness
            })

    def split_df(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Splits evaluated DataFrame into valid and invalid partitions using both Column Errors and Table Check Flags.
        """
        try:
            valid_conditions = ["size(Errors) == 0"]
            invalid_conditions = ["size(Errors) > 0"]

            for flag_info in self.flags:
                if not flag_info.get("on_split_keep", False):
                    flag_name = flag_info["flag_name"]
                    valid_conditions.append(f"coalesce(`{flag_name}`, 0) == 0")
                    invalid_conditions.append(f"coalesce(`{flag_name}`, 0) != 0")

            valid_expr = " AND ".join(valid_conditions)
            invalid_expr = " OR ".join(invalid_conditions)

            valid_df = df.filter(valid_expr)
            invalid_df = df.filter(invalid_expr)

            return valid_df, invalid_df

        except Exception as e:
            logger.error(f"Failed to split DataFrame: {str(e)}")
            raise RuntimeError(f"DataFrame split operation failed: {str(e)}") from e

    def unpersist(self):
        """
        Safely removes the cached evaluated DataFrame from Spark storage.
        """
        if self.final_df is not None and self.final_df.is_cached:
            self.final_df.unpersist()
            logger.info(f"Successfully unpersisted evaluated DataFrame for table '{self.table_name}'. Memory freed.")