import logging
from typing import Tuple, Dict, Any, List

logger = logging.getLogger(__name__)


class ArrayQualityChecks:
    """Handles data quality SQL generation for PySpark Array columns."""

    EMPTY_ARRAY_SQL = "CAST(array() AS ARRAY<STRING>)"

    @classmethod
    def _extract_base_params(cls, check: dict, column: str):
        from yamlpipe.registry.columns_quality_registry import ColumnQualityRegistry
        return ColumnQualityRegistry._extract_base_params(check, column)

    @classmethod
    def not_empty_check(cls, check: dict, column: str) -> Tuple[str, str, str]:
        error_suffix = "ARRAY_EMPTY_ERROR"
        try:
            logger.debug("Building 'array_not_empty' check for column '%s'", column)
            severity, when_cond, col_expr = cls._extract_base_params(check, column)
            cast_cond = f"({col_expr} IS NULL OR size({col_expr}) = 0)"

            sql = f"""
            CASE
                WHEN ({when_cond}) AND ({cast_cond})
                THEN array('{column}_{error_suffix}')
                ELSE {cls.EMPTY_ARRAY_SQL}
            END
            """
            return sql, severity, error_suffix
        except Exception as e:
            logger.error("Failed to build 'array_not_empty' check for column '%s': %s", column, str(e), exc_info=True)
            raise RuntimeError(f"[ArrayQualityChecks] Error in 'array_not_empty' check for column '{column}': {e}") from e

    @classmethod
    def values_in_list_check(cls, check: dict, column: str) -> Tuple[str, str, str]:
        error_suffix = "ARRAY_INVALID_VALUES_ERROR"
        try:
            logger.debug("Building 'array_values_in_list' check for column '%s'", column)
            severity, when_cond, col_expr = cls._extract_base_params(check, column)
            allowed_values = check.get("values", check.get("allowed_values", []))

            if not allowed_values:
                raise ValueError(
                    f"Check configuration for 'array_values_in_list' on column '{column}' "
                    f"is missing 'values' or 'allowed_values'."
                )

            formatted_vals = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in allowed_values])
            cast_cond = f"NOT forall({col_expr}, x -> x IN ({formatted_vals}))"

            sql = f"""
            CASE
                WHEN ({when_cond}) 
                     AND {col_expr} IS NOT NULL 
                     AND size({col_expr}) > 0 
                     AND ({cast_cond})
                THEN array('{column}_{error_suffix}')
                ELSE {cls.EMPTY_ARRAY_SQL}
            END
            """
            return sql, severity, error_suffix
        except Exception as e:
            logger.error("Failed to build 'array_values_in_list' check for column '%s': %s", column, str(e), exc_info=True)
            raise RuntimeError(f"[ArrayQualityChecks] Error in 'array_values_in_list' check for column '{column}': {e}") from e

    @classmethod
    def values_regex_check(cls, check: dict, column: str) -> Tuple[str, str, str]:
        """Validates that all elements inside the array match a specified regex pattern."""
        error_suffix = "ARRAY_VALUES_REGEX_ERROR"
        try:
            logger.debug("Building 'array_values_regex' check for column '%s'", column)
            severity, when_cond, col_expr = cls._extract_base_params(check, column)
            pattern = check.get("pattern", check.get("regex"))

            if not pattern:
                raise ValueError(
                    f"Check configuration for 'array_values_regex' on column '{column}' "
                    f"requires a 'pattern' or 'regex' string."
                )

            # Ensure all elements match the given regex pattern
            cast_cond = f"NOT forall({col_expr}, x -> x RLIKE '{pattern}')"

            sql = f"""
            CASE
                WHEN ({when_cond}) 
                     AND {col_expr} IS NOT NULL 
                     AND size({col_expr}) > 0 
                     AND ({cast_cond})
                THEN array('{column}_{error_suffix}')
                ELSE {cls.EMPTY_ARRAY_SQL}
            END
            """
            return sql, severity, error_suffix
        except Exception as e:
            logger.error("Failed to build 'array_values_regex' check for column '%s': %s", column, str(e), exc_info=True)
            raise RuntimeError(f"[ArrayQualityChecks] Error in 'array_values_regex' check for column '{column}': {e}") from e

    @classmethod
    def values_range_check(cls, check: dict, column: str) -> Tuple[str, str, str]:
        """Validates that all elements inside the array fall within min/max bounds."""
        error_suffix = "ARRAY_VALUES_RANGE_ERROR"
        try:
            logger.debug("Building 'array_values_range' check for column '%s'", column)
            severity, when_cond, col_expr = cls._extract_base_params(check, column)
            
            range_cfg = check.get("range", check)
            minimum = range_cfg.get("min")
            maximum = range_cfg.get("max")

            conditions = []
            if minimum is not None:
                conditions.append(f"x >= {minimum}")
            if maximum is not None:
                conditions.append(f"x <= {maximum}")

            if not conditions:
                raise ValueError(
                    f"Check configuration for 'array_values_range' on column '{column}' "
                    f"requires at least 'min' or 'max'."
                )

            bound_cond = " AND ".join(conditions)
            cast_cond = f"NOT forall({col_expr}, x -> {bound_cond})"

            sql = f"""
            CASE
                WHEN ({when_cond}) 
                     AND {col_expr} IS NOT NULL 
                     AND size({col_expr}) > 0 
                     AND ({cast_cond})
                THEN array('{column}_{error_suffix}')
                ELSE {cls.EMPTY_ARRAY_SQL}
            END
            """
            return sql, severity, error_suffix
        except Exception as e:
            logger.error("Failed to build 'array_values_range' check for column '%s': %s", column, str(e), exc_info=True)
            raise RuntimeError(f"[ArrayQualityChecks] Error in 'array_values_range' check for column '{column}': {e}") from e

    @classmethod
    def length_check(cls, check: dict, column: str) -> Tuple[str, str, str]:
        error_suffix = "ARRAY_LENGTH_ERROR"
        try:
            logger.debug("Building 'array_length' check for column '%s'", column)
            severity, when_cond, col_expr = cls._extract_base_params(check, column)
            length_cfg = check.get("length", check)
            minimum = length_cfg.get("min")
            maximum = length_cfg.get("max")

            conditions = []
            if minimum is not None:
                conditions.append(f"size({col_expr}) < {minimum}")
            if maximum is not None:
                conditions.append(f"size({col_expr}) > {maximum}")

            if not conditions:
                raise ValueError(
                    f"Array length check on column '{column}' requires at least 'min' or 'max' property defined."
                )

            condition = " OR ".join(conditions)
            sql = f"""
            CASE
                WHEN ({when_cond}) AND {col_expr} IS NOT NULL AND ({condition})
                THEN array('{column}_{error_suffix}')
                ELSE {cls.EMPTY_ARRAY_SQL}
            END
            """
            return sql, severity, error_suffix
        except Exception as e:
            logger.error("Failed to build 'array_length' check for column '%s': %s", column, str(e), exc_info=True)
            raise RuntimeError(f"[ArrayQualityChecks] Error in 'array_length' check for column '{column}': {e}") from e

    @classmethod
    def no_nulls_check(cls, check: dict, column: str) -> Tuple[str, str, str]:
        error_suffix = "ARRAY_CONTAINS_NULL_ERROR"
        try:
            logger.debug("Building 'array_no_nulls' check for column '%s'", column)
            severity, when_cond, col_expr = cls._extract_base_params(check, column)
            cast_cond = f"exists({col_expr}, x -> x IS NULL)"

            sql = f"""
            CASE
                WHEN ({when_cond}) AND {col_expr} IS NOT NULL AND ({cast_cond})
                THEN array('{column}_{error_suffix}')
                ELSE {cls.EMPTY_ARRAY_SQL}
            END
            """
            return sql, severity, error_suffix
        except Exception as e:
            logger.error("Failed to build 'array_no_nulls' check for column '%s': %s", column, str(e), exc_info=True)
            raise RuntimeError(f"[ArrayQualityChecks] Error in 'array_no_nulls' check for column '{column}': {e}") from e

    @classmethod
    def distinct_values_check(cls, check: dict, column: str) -> Tuple[str, str, str]:
        error_suffix = "ARRAY_DUPLICATE_VALUES_ERROR"
        try:
            logger.debug("Building 'array_distinct_values' check for column '%s'", column)
            severity, when_cond, col_expr = cls._extract_base_params(check, column)
            cast_cond = f"size({col_expr}) != size(array_distinct({col_expr}))"

            sql = f"""
            CASE
                WHEN ({when_cond}) AND {col_expr} IS NOT NULL AND ({cast_cond})
                THEN array('{column}_{error_suffix}')
                ELSE {cls.EMPTY_ARRAY_SQL}
            END
            """
            return sql, severity, error_suffix
        except Exception as e:
            logger.error("Failed to build 'array_distinct_values' check for column '%s': %s", column, str(e), exc_info=True)
            raise RuntimeError(f"[ArrayQualityChecks] Error in 'array_distinct_values' check for column '{column}': {e}") from e


