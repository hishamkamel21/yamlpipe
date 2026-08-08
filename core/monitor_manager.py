from typing import List, Dict, Any, Optional, Set
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class MonitorManager:

    @staticmethod
    def build_rules_regex_pattern(error_suffixes: Optional[Set[str]] = None) -> str:
        """
        Generates a regex pattern strictly based on provided error suffixes.
        Sorts longer suffixes first so sub-strings aren't prematurely matched.
        """
        if not error_suffixes:
            return r"^(.*?)_([A-Z0-9_]+_ERROR)$"

        # Sort descending by length to ensure longer rules match before shorter tokens
        sorted_suffixes = sorted(list(error_suffixes), key=len, reverse=True)
        suffix_or_pattern = "|".join(sorted_suffixes)

        return f"^(.*?)_({suffix_or_pattern})$"

    @classmethod
    def generate_metrics(
        cls, 
        df: DataFrame, 
        table_name: str, 
        flags: List[dict],
        check_summary: Dict[str, bool],
        batch_id=None,
        error_suffixes: Optional[Set[str]] = None
    ) -> Dict[str, DataFrame]:
        try:
            batch_id_val = batch_id if batch_id else None
            results = {}

            schema_checks = check_summary.get("schema_checks_exist", False)
            column_checks = check_summary.get("columns_checks_exist", False)
            table_checks = check_summary.get("table_checks_exist", False)

            requires_data_scan = column_checks or table_checks or cls._has_active_flags(flags)

            if schema_checks and "schema_monitor" in df.columns:
                results["schema_summary"] = cls._build_schema_summary(
                    df, table_name, batch_id_val
                )
                results["schema_monitor_details"] = cls._build_schema_details_metrics(
                    df, table_name, batch_id_val
                )

            if not requires_data_scan:
                return results

            data_summary_df = cls._build_data_monitor_summary(
                df, table_name, flags, batch_id_val
            )
            results["data_monitor_summary"] = data_summary_df

            exploded_errors = None
            exploded_warnings = None

            # Generate dynamic regex pattern using captured error suffixes
            rules_regex_pattern = cls.build_rules_regex_pattern(error_suffixes)

            if column_checks:
                exploded_errors, exploded_warnings = cls._prepare_exploded_issue_streams(df)

                results["per_column_metrics"] = cls._build_per_column_metrics(
                    table_name, data_summary_df, exploded_errors, exploded_warnings, rules_regex_pattern, batch_id_val
                )

            if table_checks or column_checks:
                results["per_error_type_metrics"] = cls._build_by_error_type_metrics(
                    df, table_name, flags, data_summary_df, exploded_errors, exploded_warnings, rules_regex_pattern, batch_id_val
                )

            return results

        except Exception as e:
            raise RuntimeError(f"Error generating monitoring metrics: {str(e)}") from e

    @staticmethod
    def _has_active_flags(flags: List[dict]) -> bool:
        return len(flags) > 0

    @staticmethod
    def _build_schema_summary(
        df: DataFrame, table_name: str, batch_id_val=None
    ) -> DataFrame:
        """
        Builds the high-level schema compliance summary metric.
        """
        return (
            df.limit(1)
            .select(
                F.lit(batch_id_val).alias("batch_id"),
                F.lit(table_name).alias("table"),
                F.col("schema_monitor.status").alias("schema_status"),
                F.size(F.col("schema_monitor.missing_columns")).alias("missing_columns_count"),
                F.size(F.col("schema_monitor.unexpected_columns")).alias("unexpected_columns_count"),
                F.size(F.col("schema_monitor.mismatched_types")).alias("mismatched_types_count"),
            )
        )

    @staticmethod
    def _build_schema_details_metrics(
        df: DataFrame, table_name: str, batch_id_val=None
    ) -> DataFrame:
        """
        Builds itemized breakdown of missing, unexpected, and type-mismatched columns.
        """
        missing_df = df.limit(1).select(
            F.explode_outer(F.col("schema_monitor.missing_columns")).alias("missing")
        ).select(
            F.lit(batch_id_val).alias("batch_id"),
            F.lit(table_name).alias("table"),
            F.lit("MISSING_COLUMN").alias("issue_type"),
            F.col("missing.column").alias("column_name"),
            F.col("missing.expected_type").alias("expected_type"),
            F.lit(None).cast("string").alias("actual_type")
        ).filter(F.col("column_name").isNotNull())

        unexpected_df = df.limit(1).select(
            F.explode_outer(F.col("schema_monitor.unexpected_columns")).alias("unexpected")
        ).select(
            F.lit(batch_id_val).alias("batch_id"),
            F.lit(table_name).alias("table"),
            F.lit("UNEXPECTED_COLUMN").alias("issue_type"),
            F.col("unexpected.column").alias("column_name"),
            F.lit(None).cast("string").alias("expected_type"),
            F.col("unexpected.actual_type").alias("actual_type")
        ).filter(F.col("column_name").isNotNull())

        mismatched_df = df.limit(1).select(
            F.explode_outer(F.col("schema_monitor.mismatched_types")).alias("mismatched")
        ).select(
            F.lit(batch_id_val).alias("batch_id"),
            F.lit(table_name).alias("table"),
            F.lit("TYPE_MISMATCH").alias("issue_type"),
            F.col("mismatched.column").alias("column_name"),
            F.col("mismatched.expected_type").alias("expected_type"),
            F.col("mismatched.actual_type").alias("actual_type")
        ).filter(F.col("column_name").isNotNull())

        return missing_df.union(unexpected_df).union(mismatched_df)

    @classmethod
    def _build_data_monitor_summary(
        cls, df: DataFrame, table_name: str, flags: List[dict], batch_id_val=None
    ) -> DataFrame:
        """
        Aggregates data row counts, valid/invalid splits, errors, warnings, and table flags.
        Handles freshness flags with MAX aggregation ending in '_value', and standard flags
        with COUNT aggregation ending in '_count'.
        """
        agg_exprs = [
            F.count("*").alias("total_rows"),
            F.sum(F.when(F.size(F.col("Errors")) > 0, 1).otherwise(0)).alias("rows_with_errors"),
            F.sum(F.when(F.size(F.col("Warnings")) > 0, 1).otherwise(0)).alias("rows_with_warnings"),
        ]

        for f in flags:
            flag_name = f["flag_name"]
            is_freshness = f.get("is_freshness", False)

            if is_freshness:
                agg_exprs.append(
                    F.max(F.col(flag_name)).alias(f"flag_{flag_name}_value")
                )
            else:
                agg_exprs.append(
                    F.sum(F.when(F.coalesce(F.col(flag_name), F.lit(0)) != 0, 1).otherwise(0)).alias(f"flag_{flag_name}_count")
                )

        summary_df = df.agg(*agg_exprs)

        # Build valid row condition considering both Errors array and non-freshness flags
        invalid_conditions = [F.size(F.col("Errors")) > 0]
        for f in flags:
            if not f.get("on_split_keep", False) and not f.get("is_freshness", False):
                flag_name = f["flag_name"]
                invalid_conditions.append(F.coalesce(F.col(flag_name), F.lit(0)) != 0)

        invalid_expr = invalid_conditions[0]
        for cond in invalid_conditions[1:]:
            invalid_expr = invalid_expr | cond

        invalid_rows_df = df.filter(invalid_expr).agg(F.count("*").alias("invalid_rows"))

        # Select all generated base summary columns along with dynamic flag columns
        dynamic_flag_cols = [
            F.col(f"flag_{f['flag_name']}_value") if f.get("is_freshness", False) else F.col(f"flag_{f['flag_name']}_count")
            for f in flags
        ]

        base_cols = [
            F.lit(batch_id_val).alias("batch_id"),
            F.lit(table_name).alias("table"),
            F.col("total_rows"),
            F.col("valid_rows"),
            F.col("invalid_rows"),
            F.col("valid_rate"),
            F.col("rows_with_errors"),
            F.col("rows_with_warnings")
        ]

        return (
            summary_df.crossJoin(invalid_rows_df)
            .withColumn("valid_rows", F.col("total_rows") - F.col("invalid_rows"))
            .withColumn("valid_rate", F.round((F.col("valid_rows") / F.when(F.col("total_rows") == 0, 1).otherwise(F.col("total_rows"))) * 100, 2))
            .select(*(base_cols + dynamic_flag_cols))
        )

    @staticmethod
    def _prepare_exploded_issue_streams(df: DataFrame):
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

    @classmethod
    def _build_per_column_metrics(
        cls, 
        table_name: str, 
        data_summary_df: DataFrame,
        exploded_errors: Optional[DataFrame],
        exploded_warnings: Optional[DataFrame],
        rules_regex_pattern: str,
        batch_id_val=None, 
    ) -> DataFrame:
        streams = []
        if exploded_errors is not None:
            streams.append(exploded_errors)
        if exploded_warnings is not None:
            streams.append(exploded_warnings)

        if not streams:
            empty_schema = "batch_id string, table string, column string, error_type string, severity string, error_count long, error_rate double"
            return data_summary_df.sparkSession.createDataFrame([], empty_schema)

        combined_column_rules = streams[0]
        for s in streams[1:]:
            combined_column_rules = combined_column_rules.union(s)

        denom_df = data_summary_df.select(
            F.when(F.col("total_rows") == 0, 1).otherwise(F.col("total_rows")).alias("total_rows_denom")
        )

        return (
            combined_column_rules
            .withColumn("column", F.regexp_extract(F.col("raw_rule"), rules_regex_pattern, 1))
            .withColumn("rule_type", F.regexp_extract(F.col("raw_rule"), rules_regex_pattern, 2))
            # Fallback if pattern matching fails
            .withColumn("column", F.when(F.col("column") == "", F.element_at(F.split(F.col("raw_rule"), "_"), 1)).otherwise(F.col("column")))
            .withColumn("rule_type", F.when(F.col("rule_type") == "", F.element_at(F.split(F.col("raw_rule"), "_"), -1)).otherwise(F.col("rule_type")))
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
        rules_regex_pattern: str,
        batch_id_val=None, 
    ) -> DataFrame:
        streams = []

        if exploded_errors is not None:
            extracted_err = exploded_errors.withColumn(
                "extracted_type", F.regexp_extract(F.col("raw_rule"), rules_regex_pattern, 2)
            ).withColumn(
                "error_type", 
                F.when(F.col("extracted_type") != "", F.col("extracted_type"))
                .otherwise(F.element_at(F.split(F.col("raw_rule"), "_"), -1))
            )

            streams.append(
                extracted_err.select(
                    F.col("error_type"),
                    F.lit("COLUMN_ERROR").alias("impact_category")
                )
            )

        if exploded_warnings is not None:
            extracted_warn = exploded_warnings.withColumn(
                "extracted_type", F.regexp_extract(F.col("raw_rule"), rules_regex_pattern, 2)
            ).withColumn(
                "error_type", 
                F.when(F.col("extracted_type") != "", F.col("extracted_type"))
                .otherwise(F.element_at(F.split(F.col("raw_rule"), "_"), -1))
            )

            streams.append(
                extracted_warn.select(
                    F.col("error_type"),
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