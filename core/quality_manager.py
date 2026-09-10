import re
from typing import Dict, Any, Tuple, Union, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from pyspark.storagelevel import StorageLevel

from yamlpipe.registry.schema_checks_registry import SchemaQualityRegistry
from yamlpipe.core.monitor_manager import MonitorManager
from yamlpipe.utility.helper import Helper
from yamlpipe.utility.logger import get_logger

logger = get_logger("[ QualityManager ]")


class QualityManager:

    def __init__(self, parsed_config: Dict[str, Any], df: DataFrame):
        """
        Initializes QualityManager with parsed quality checks configuration and target DataFrame.
        """
        if not isinstance(parsed_config, dict):
            logger.error("[QualityManager Init Error] 'parsed_config' must be a dict.")
            raise TypeError(
                f"[QualityManager Init Error] 'parsed_config' must be a dictionary, "
                f"got '{type(parsed_config).__name__}'."
            )

        if not isinstance(df, DataFrame):
            logger.error("[QualityManager Init Error] 'df' must be a valid PySpark DataFrame.")
            raise TypeError(
                f"[QualityManager Init Error] 'df' must be a valid PySpark DataFrame, "
                f"got '{type(df).__name__}'."
            )

        self.parsed_config = parsed_config
        self.df = df

        raw_table_identifier = (
            parsed_config.get("table") 
            or parsed_config.get("table_name") 
            or "unknown_table"
        )
        self.table_name = Helper.parse_table_name(raw_table_identifier)
        logger.info(f"Initialized QualityManager for table: '{self.table_name}'")

        self.schema_checks = parsed_config.get("schema_checks", [])
        self.columns_checks = parsed_config.get("columns_checks", {"error_expr": [], "warn_expr": []})
        self.registered_error_suffixes = parsed_config.get("registered_error_suffixes", [])
        self.table_checks = parsed_config.get("table_checks", {"checks": [], "temp_views_to_create": []})

        self.check_summary = {
            "schema_checks_exist": bool(self.schema_checks),
            "columns_checks_exist": bool(
                self.columns_checks.get("error_expr") or self.columns_checks.get("warn_expr")
            ),
            "table_checks_exist": bool(self.table_checks.get("checks")),
        }

        logger.debug(f"Check execution summary for '{self.table_name}': {self.check_summary}")

        self.flags = []
        self.final_df = None

    def apply_checks(
        self,
        batch_id: Any = None,
        action: str = "split",
        show_monitor_metrics: bool = False,
        persist_df: bool = True,
        storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK
    ) -> Union[Tuple[Any, ...], DataFrame]:
        """
        Main Execution Engine. Runs Schema, Column, and Table quality rules against the DataFrame.
        """
        logger.info(f"Applying quality checks on table '{self.table_name}' (batch_id: {batch_id}, action: {action})")
        current_df = self.df

        # ---------------------------------------------------------------------
        # 1. Apply Schema-Level Checks
        # ---------------------------------------------------------------------
        if self.schema_checks:
            logger.info("Executing Schema-Level Quality Checks...")
            try:
                current_df = SchemaQualityRegistry.apply_schema_checks(current_df, self.schema_checks)
            except Exception as e:
                logger.error(f"Schema Quality evaluation failed for '{self.table_name}': {str(e)}", exc_info=True)
                raise RuntimeError(f"Schema Quality evaluation failed: {str(e)}") from e

        # ---------------------------------------------------------------------
        # 2. Apply Column-Level Checks (Inject Errors & Warnings Arrays)
        # ---------------------------------------------------------------------
        error_exprs = self.columns_checks.get("error_expr", [])
        warning_exprs = self.columns_checks.get("warn_expr", [])

        if error_exprs or warning_exprs:
            logger.info(
                f"Executing Column-Level Quality Checks... "
                f"(Errors: {len(error_exprs)}, Warnings: {len(warning_exprs)})"
            )

        try:
            empty_array_sql = "array_except(array(cast(null as string)), array(cast(null as string)))"

            if error_exprs:
                errors_sql = f"array_compact(flatten(array({', '.join(error_exprs)})))"
            else:
                errors_sql = empty_array_sql

            if warning_exprs:
                warnings_sql = f"array_compact(flatten(array({', '.join(warning_exprs)})))"
            else:
                warnings_sql = empty_array_sql

            current_df = (
                current_df
                .withColumn("Errors", expr(errors_sql))
                .withColumn("Warnings", expr(warnings_sql))
            )
        except Exception as e:
            logger.error(f"Column Quality evaluation failed for '{self.table_name}': {str(e)}", exc_info=True)
            raise RuntimeError(
                f"Column Quality evaluation failed. Check SQL expressions. Error: {str(e)}"
            ) from e

        # ---------------------------------------------------------------------
        # 3. Apply Table-Level Checks
        # ---------------------------------------------------------------------
        checks_list = self.table_checks.get("checks", [])
        temp_views = self.table_checks.get("temp_views_to_create", [])

        if checks_list:
            logger.info(f"Executing {len(checks_list)} Table-Level Quality Checks...")
            spark = current_df.sparkSession

            for view_meta in temp_views:
                view_name = view_meta.get("view_name")
                raw_ref_table = view_meta.get("table")
                path = view_meta.get("path")
                fmt = view_meta.get("format", "delta")
                filter_cond = view_meta.get("filter")

                if view_name:
                    if raw_ref_table:
                        cleaned_ref_table = Helper.parse_table_name(raw_ref_table)
                        logger.info(f"Creating temporary view '{view_name}' from table '{cleaned_ref_table}'")
                        ref_df = spark.read.table(cleaned_ref_table)
                    elif path:
                        logger.info(f"Creating temporary view '{view_name}' from path '{path}' ({fmt})")
                        ref_df = spark.read.format(fmt).load(path)
                    else:
                        continue

                    if filter_cond:
                        ref_df = ref_df.filter(filter_cond)

                    ref_df.createOrReplaceTempView(view_name)

            current_df.createOrReplaceTempView("tmp_src")

            table_expr_str = ", ".join([chk["expr"] for chk in checks_list if chk.get("expr")])
            self._register_table_flags_from_checks(checks_list)

            try:
                sql_query = f"SELECT *, {table_expr_str} FROM tmp_src"
                logger.debug(f"Running Table Check SQL Query:\n{sql_query}")
                current_df = spark.sql(sql_query)
            except Exception as e:
                logger.error(f"Table Quality evaluation failed for '{self.table_name}': {str(e)}", exc_info=True)
                raise RuntimeError(f"Table Quality evaluation failed: {str(e)}") from e

        self.final_df = current_df

        # ---------------------------------------------------------------------
        # 4. Persistence Management
        # ---------------------------------------------------------------------
        if persist_df:
            try:
                if not self.final_df.is_cached:
                    logger.info(f"Persisting evaluated DataFrame using storage level: {storage_level}")
                    self.final_df.persist(storage_level)
            except Exception as e:
                logger.warning(f"Failed to persist DataFrame: {str(e)}")

        # ---------------------------------------------------------------------
        # 5. Route Output DataFrames & Parse Metrics
        # ---------------------------------------------------------------------
        if action.lower() == "keep":
            logger.info("Routing data action: KEEP (Returning full DataFrame)")
            data_outputs = (self.final_df,)
        else:
            logger.info("Routing data action: SPLIT (Splitting into valid & invalid DataFrames)")
            valid_df, invalid_df = self.split_df(df=self.final_df)
            data_outputs = (valid_df, invalid_df)

        if show_monitor_metrics:
            logger.info("Generating monitor metrics...")
            metrics_outputs = self._generate_and_parse_metrics(batch_id=batch_id)
            return data_outputs + tuple(metrics_outputs)

        return data_outputs[0] if len(data_outputs) == 1 else data_outputs

    def _generate_and_parse_metrics(self, batch_id: Any = None) -> List[DataFrame]:
        """
        Private Helper: Calls MonitorManager and dynamically extracts metrics DataFrames.
        """
        metrics_dict = MonitorManager.generate_metrics(
            df=self.final_df,
            table_name=self.table_name,
            flags=self.flags,
            check_summary=self.check_summary,
            error_suffixes=self.registered_error_suffixes,
            batch_id=batch_id
        )

        extracted_metrics = []

        if self.check_summary.get("schema_checks_exist", False):
            if "schema_summary" in metrics_dict:
                extracted_metrics.append(metrics_dict["schema_summary"])
            if "schema_monitor_details" in metrics_dict:
                extracted_metrics.append(metrics_dict["schema_monitor_details"])

        has_active_flags = len(self.flags) > 0
        requires_data_scan = (
            self.check_summary.get("columns_checks_exist", False)
            or self.check_summary.get("table_checks_exist", False)
            or has_active_flags
        )

        if requires_data_scan and "data_monitor_summary" in metrics_dict:
            extracted_metrics.append(metrics_dict["data_monitor_summary"])

        if self.check_summary.get("columns_checks_exist", False) and "per_column_metrics" in metrics_dict:
            extracted_metrics.append(metrics_dict["per_column_metrics"])

        has_error_type_scope = (
            self.check_summary.get("table_checks_exist", False)
            or self.check_summary.get("columns_checks_exist", False)
        )
        if has_error_type_scope and "per_error_type_metrics" in metrics_dict:
            extracted_metrics.append(metrics_dict["per_error_type_metrics"])

        logger.info(f"Extracted {len(extracted_metrics)} metrics DataFrames successfully.")
        return extracted_metrics

    def _register_table_flags_from_checks(self, checks_list: List[Dict[str, Any]]):
        """
        Extracts column alias from each check expression and attaches metadata directly.
        """
        for check in checks_list:
            expr_str = check.get("expr", "").strip()
            match = re.search(r"AS\s+[`]?([a-zA-Z0-9_]+)[`]?$", expr_str, re.IGNORECASE)
            
            if match:
                flag_name = match.group(1)
                self.flags.append({
                    "flag_name": flag_name,
                    "on_split_keep": check.get("on_split_keep", False),
                    "is_freshness": check.get("is_freshness", False)
                })
                logger.debug(f"Registered table flag: '{flag_name}'")

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

            logger.debug(f"Valid Filter Expr: {valid_expr}")
            logger.debug(f"Invalid Filter Expr: {invalid_expr}")

            valid_df = df.filter(valid_expr)
            invalid_df = df.filter(invalid_expr)

            return valid_df, invalid_df

        except Exception as e:
            logger.error(f"DataFrame split operation failed for '{self.table_name}': {str(e)}", exc_info=True)
            raise RuntimeError(f"DataFrame split operation failed: {str(e)}") from e

    def unpersist(self):
        """
        Safely removes the cached evaluated DataFrame from Spark storage.
        """
        try:
            if self.final_df is not None and self.final_df.is_cached:
                self.final_df.unpersist()
                logger.info(f"Unpersisted evaluated DataFrame for '{self.table_name}'")
        except Exception as e:
            logger.warning(f"Unpersist warning: {str(e)}")