import logging
from typing import List, Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("MonitorManager")


class MonitorManager:
    """
    Manager class responsible for generating monitoring metrics across schema,
    data row-level issues, column-level rules, and table flags.
    
    Fully optimized for distributed execution:
    - Zero driver action calls (no df.count() or df.collect()).
    - All total row counts and rates are computed natively inside Spark aggregations.
    - Lazy execution paths avoid scanning data when only schema checks are enabled.
    """

    @classmethod
    def generate_metrics(
        cls, 
        df: DataFrame, 
        table_name: str, 
        flags: List[dict],
        check_summary: Dict[str, bool],
        batch_id= None, 
    ) -> Dict[str, DataFrame]:
        """
        Generates monitoring metrics DataFrames dynamically based on active check_summary flags.
        
        :param df: Input PySpark DataFrame containing monitoring columns (Errors, Warnings, schema_monitor).
        :param table_name: Target table identifier for reporting.
        :param batch_id: Unique pipeline run/batch identifier.
        :param flags: List of table-level flag configurations.
        :param check_summary: Dictionary indicating active check modules.
        :return: Dictionary containing generated metrics DataFrames.
        """
        try:
            batch_id_val = batch_id if batch_id else None
            results = {}

            schema_checks = check_summary.get("schema_checks_exist", False)
            column_checks = check_summary.get("columns_checks_exist", False)
            table_checks = check_summary.get("table_checks_exist", False)

            # Determine whether data scanning is required
            requires_data_scan = column_checks or table_checks or cls._has_active_flags(flags)

            # Route 1: SCHEMA ONLY (Zero action overhead on data records)
            if schema_checks and "schema_monitor" in df.columns:

                results["schema_summary"] = cls._build_schema_summary(
                    df, table_name, batch_id_val
                )
                results["schema_monitor_details"] = cls._build_schema_details_metrics(
                    df, table_name, batch_id_val
                )

            # Short-circuit early if no row/column/table level checks were requested
            if not requires_data_scan:
                return results

            # Route 2: DATA SUMMARY METRICS
            data_summary_df = cls._build_data_monitor_summary(
                df, table_name, flags , batch_id_val
            )
            results["data_monitor_summary"] = data_summary_df

            # Prepare exploded streams for rule-level checks
            exploded_errors = None
            exploded_warnings = None

            if column_checks:
                
                exploded_errors, exploded_warnings = cls._prepare_exploded_issue_streams(df)

                # Column metrics use crossJoin with data_summary_df to obtain total_rows lazily
                results["per_column_metrics"] = cls._build_per_column_metrics(
                    table_name, data_summary_df, exploded_errors, exploded_warnings , batch_id_val
                )

            # Route 3: ERROR TYPE METRICS
            if table_checks or column_checks:
                results["per_error_type_metrics"] = cls._build_by_error_type_metrics(
                    df, table_name, flags, data_summary_df, exploded_errors, exploded_warnings,batch_id_val
                )

            return results

        except Exception as e:
            logger.error(f"Failed to generate monitor metrics: {str(e)}")
            raise RuntimeError(f"Error generating monitoring metrics: {str(e)}") from e

    # -----------------------------------------------------------------
    # ROUTING UTILITIES & STREAM HELPERS
    # -----------------------------------------------------------------
    @staticmethod
    def _has_active_flags(flags: List[dict]) -> bool:
        """Checks if the flags list contains active configuration dictionaries."""
        return len(flags) > 0

    @staticmethod
    def _prepare_exploded_issue_streams(df: DataFrame):
        """Prepares lazy transformation streams for exploded errors and warnings."""
        filtered_errors = df.filter(F.coalesce(F.size(F.col("Errors")), F.lit(0)) > 0)
        filtered_warnings = df.filter(F.coalesce(F.size(F.col("Warnings")), F.lit(0)) > 0)

        exploded_errors = filtered_errors.select(
            F.explode(F.col("Errors")).alias("raw_rule"),
            F.lit("error").alias("severity")
        )
        exploded_warnings = filtered_warnings.select(
            F.explode(F.col("Warnings")).alias("raw_rule"),
            F.lit("warn").alias("severity")
        )
        return exploded_errors, exploded_warnings

    # -----------------------------------------------------------------
    # SUB-BUILDERS
    # -----------------------------------------------------------------
    @classmethod
    def _build_data_monitor_summary(
        cls, 
        df: DataFrame, 
        table_name: str, 
        flags: List[dict],
        batch_id_val= None, 
    ) -> DataFrame:
        """
        Calculates table-level row metrics and flags in a single aggregation pass.
        Computes total rows via F.count(F.lit(1)) directly within the select projection.
        """
        total_rows_expr = F.count(F.lit(1))
        denom_expr = F.when(total_rows_expr == 0, F.lit(1)).otherwise(total_rows_expr)

        has_errors_cond = F.when(F.coalesce(F.size(F.col("Errors")), F.lit(0)) > 0, 1).otherwise(0) if "Errors" in df.columns else F.lit(0)
        has_warns_cond = F.when(F.coalesce(F.size(F.col("Warnings")), F.lit(0)) > 0, 1).otherwise(0) if "Warnings" in df.columns else F.lit(0)

        summary_exprs = [
            F.lit(batch_id_val).alias("batch_id"),
            F.lit(table_name).alias("table"),
            total_rows_expr.alias("Total_rows"),
            
            F.sum(has_errors_cond).alias("Total_error_rows"),
            F.sum(has_warns_cond).alias("Total_warn_rows"),
            
            F.round((F.sum(has_errors_cond) / denom_expr) * 100, 2).alias("Error_row_rate"),
            F.round((F.sum(has_warns_cond) / denom_expr) * 100, 2).alias("Warn_row_rate")
        ]

        for flag_info in flags:
            flag_name = flag_info["flag_name"]
            is_freshness = flag_info.get("is_freshness", False)

            if is_freshness:
                summary_exprs.append(F.max(F.col(flag_name)).alias(f"{flag_name}_value"))
            else:
                flag_condition = F.when(F.coalesce(F.col(flag_name), F.lit(0)) != 0, 1).otherwise(0)
                summary_exprs.append(F.sum(flag_condition).alias(f"{flag_name}_count"))
                summary_exprs.append(
                    F.round((F.sum(flag_condition) / denom_expr) * 100, 2).alias(f"{flag_name}_rate")
                )

        return df.select(*summary_exprs)

    @classmethod
    def _build_schema_summary(
        cls, 
        df: DataFrame, 
        table_name: str, 
        batch_id_val = None 
    ) -> DataFrame:
        """
        Extracts top-level schema monitor metrics purely using DataFrame projections.
        Reads nested struct attributes directly from `schema_monitor`.
        """
        req_missing_cnt = F.coalesce(F.size(F.col("schema_monitor.required_missing")), F.lit(0))
        type_mismatch_cnt = F.coalesce(F.size(F.col("schema_monitor.type_mismatch")), F.lit(0))
        dup_cols_cnt = F.coalesce(F.size(F.col("schema_monitor.no_duplicate_columns.columns")), F.lit(0))
        forbidden_cnt = F.coalesce(F.size(F.col("schema_monitor.forbidden_exist")), F.lit(0))

        # schema_pass is true if all counts are 0
        schema_pass_expr = (
            (req_missing_cnt == 0) & 
            (type_mismatch_cnt == 0) & 
            (dup_cols_cnt == 0) & 
            (forbidden_cnt == 0)
        )

        return (
            df.limit(1)
            .select(
                F.lit(batch_id_val).alias("batch_id"),
                F.lit(table_name).alias("table"),
                req_missing_cnt.cast("int").alias("required_missing_count"),
                type_mismatch_cnt.cast("int").alias("type_mismatch_count"),
                dup_cols_cnt.cast("int").alias("duplicate_columns_count"),
                forbidden_cnt.cast("int").alias("forbidden_exist_count"),
                schema_pass_expr.cast("boolean").alias("schema_pass")
            )
        )

    @classmethod
    def _build_schema_details_metrics(
        cls, 
        df: DataFrame, 
        table_name: str, 
        batch_id_val = None
    ) -> DataFrame:
        """
        Parses detailed structural mismatches natively using Spark functions
        from the nested `schema_monitor` struct without collecting rows to the driver.
        """
        sample_df = df.limit(1)

        # 1. Required missing columns stream
        missing_df = (
            sample_df
            .filter(F.coalesce(F.size(F.col("schema_monitor.required_missing")), F.lit(0)) > 0)
            .select(
                F.lit(batch_id_val).alias("batch_id"),
                F.lit(table_name).alias("table"),
                F.lit("required_missing").alias("check_type"),
                F.explode(F.col("schema_monitor.required_missing")).alias("column"),
                F.lit(None).cast("string").alias("expected_type"),
                F.lit(None).cast("string").alias("actual_type")
            )
        )

        # 2. Type mismatch stream
        mismatch_df = (
            sample_df
            .filter(F.coalesce(F.size(F.col("schema_monitor.type_mismatch")), F.lit(0)) > 0)
            .select(
                F.explode(F.col("schema_monitor.type_mismatch")).alias("mm")
            )
            .select(
                F.lit(batch_id_val).alias("batch_id"),
                F.lit(table_name).alias("table"),
                F.lit("type_mismatch").alias("check_type"),
                F.col("mm.column").alias("column"),
                F.col("mm.expected_type").alias("expected_type"),
                F.col("mm.actual_type").alias("actual_type")
            )
        )

        # 3. Duplicate columns stream
        dup_df = (
            sample_df
            .filter(F.coalesce(F.size(F.col("schema_monitor.no_duplicate_columns.columns")), F.lit(0)) > 0)
            .select(
                F.lit(batch_id_val).alias("batch_id"),
                F.lit(table_name).alias("table"),
                F.lit("no_duplicate_columns").alias("check_type"),
                F.explode(F.col("schema_monitor.no_duplicate_columns.columns")).alias("column"),
                F.lit(None).cast("string").alias("expected_type"),
                F.lit(None).cast("string").alias("actual_type")
            )
        )

        # 4. Forbidden exist stream
        forbidden_df = (
            sample_df
            .filter(F.coalesce(F.size(F.col("schema_monitor.forbidden_exist")), F.lit(0)) > 0)
            .select(
                F.lit(batch_id_val).alias("batch_id"),
                F.lit(table_name).alias("table"),
                F.lit("forbidden_exist").alias("check_type"),
                F.explode(F.col("schema_monitor.forbidden_exist")).alias("column"),
                F.lit(None).cast("string").alias("expected_type"),
                F.lit(None).cast("string").alias("actual_type")
            )
        )

        # Union all sub-streams into a single schema details DataFrame
        return missing_df.union(mismatch_df).union(dup_df).union(forbidden_df)

    @classmethod
    def _build_per_column_metrics(
        cls, 
        table_name: str, 
        data_summary_df: DataFrame,
        exploded_errors: DataFrame,
        exploded_warnings: DataFrame,
        batch_id_val= None, 
    ) -> DataFrame:
        """
        Aggregates rule violations per column and severity level.
        Joins with single-row data_summary_df lazily to get total_rows for error rate.
        """
        combined_column_rules = exploded_errors.union(exploded_warnings)

        denom_df = data_summary_df.select(
            F.when(F.col("total_rows") == 0, 1).otherwise(F.col("total_rows")).alias("total_rows_denom")
        )

        return (
            combined_column_rules
            .withColumn("column", F.element_at(F.split(F.col("raw_rule"), "_(?=[^_]+$)"), 1))
            .withColumn("rule_type", F.element_at(F.split(F.col("raw_rule"), "_(?=[^_]+$)"), 2))
            .groupBy("column", "rule_type", "severity")
            .agg(F.count("*").alias("error_count"))
            .crossJoin(denom_df)
            .withColumn("error_rate", F.round((F.col("error_count") / F.col("total_rows_denom")) * 100, 2))
            .select(
                F.lit(batch_id_val).alias("batch_id"),
                F.lit(table_name).alias("table"),
                F.col("column"),
                F.col("rule_type").alias("error_type"),
                F.col("severity"),
                F.col("error_count"),
                F.col("error_rate")
            )
        )

    @classmethod
    def _build_by_error_type_metrics(
        cls, 
        df: DataFrame, 
        table_name: str, 
        flags: List[dict],
        data_summary_df: DataFrame,
        exploded_errors: Optional[DataFrame],
        exploded_warnings: Optional[DataFrame],
        batch_id_val= None, 
    ) -> DataFrame:
        """
        Consolidates errors across column rules and table flags grouped by error_type.
        """
        streams = []

        if exploded_errors is not None:
            streams.append(
                exploded_errors.select(
                    F.element_at(F.split(F.col("raw_rule"), "_(?=[^_]+$)"), 2).alias("error_type"),
                    F.lit("COLUMN_ERROR").alias("impact_category")
                )
            )
        if exploded_warnings is not None:
            streams.append(
                exploded_warnings.select(
                    F.element_at(F.split(F.col("raw_rule"), "_(?=[^_]+$)"), 2).alias("error_type"),
                    F.lit("COLUMN_WARN").alias("impact_category")
                )
            )

        for f in flags:
            if f.get("is_freshness", False):
                continue

            flag_name = f["flag_name"]
            flag_df = df.filter(F.coalesce(F.col(flag_name), F.lit(0)) != 0).select(
                F.lit(f"{flag_name.upper()}_FLAG").alias("error_type"),
                F.lit("TABLE_FLAG").alias("impact_category")
            )
            streams.append(flag_df)

        if not streams:
            empty_schema = "error_type string, impact_category string"
            all_issues_df = df.sparkSession.createDataFrame([], empty_schema)
        else:
            all_issues_df = streams[0]
            for s in streams[1:]:
                all_issues_df = all_issues_df.union(s)

        denom_df = data_summary_df.select(
            F.when(F.col("total_rows") == 0, 1).otherwise(F.col("total_rows")).alias("total_rows_denom")
        )

        return (
            all_issues_df
            .groupBy("error_type", "impact_category")
            .agg(F.count("*").alias("number_of_violations"))
            .crossJoin(denom_df)
            .withColumn(
                "error_rate", 
                F.round((F.col("number_of_violations") / F.col("total_rows_denom")) * 100, 2)
            )
            .select(
                F.lit(batch_id_val).alias("batch_id"),
                F.lit(table_name).alias("table"),
                F.col("error_type"),
                F.col("impact_category"),
                F.col("number_of_violations"),
                F.col("error_rate")
            )
        )