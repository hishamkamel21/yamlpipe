from typing import Tuple, Dict, Any, List


class StructQualityChecks:
    """Handles data quality SQL generation for PySpark Struct columns."""

    EMPTY_ARRAY_SQL = "CAST(array() AS ARRAY<STRING>)"

    @classmethod
    def _extract_base_params(cls, check: dict, column: str):
        from yamlpipe.registry.columns_quality_registry import ColumnQualityRegistry
        return ColumnQualityRegistry._extract_base_params(check, column)

    @classmethod
    def _format_error_tag(cls, column: str, field: str, suffix: str) -> str:
        """يضمن بناء اسم خطأ نظيف بدون نقاط قد تسبب مشاكل بالـ SQL parsing."""
        if field:
            full_path = f"{column}_{field}"
        else:
            full_path = column
        safe_path = full_path.replace(".", "_")
        return f"{safe_path}_{suffix}"

    @classmethod
    def not_empty_check(cls, check: dict, column: str) -> Tuple[str, str, str]:
        error_suffix = "STRUCT_EMPTY_ERROR"
        try:
            severity, when_cond, col_expr = cls._extract_base_params(check, column)
            error_tag = cls._format_error_tag(column, "", error_suffix)

            sql = f"""
            CASE
                WHEN ({when_cond}) AND ({col_expr} IS NULL)
                THEN array('{error_tag}')
                ELSE {cls.EMPTY_ARRAY_SQL}
            END
            """
            return sql, severity, error_suffix
        except Exception as e:
            raise RuntimeError(f"[StructQualityChecks] Error in 'struct_not_empty' check for column '{column}': {e}") from e

    @classmethod
    def fields_not_null_check(cls, check: dict, column: str) -> Tuple[str, str, str]:
        error_suffix = "STRUCT_FIELD_NULL_ERROR"
        try:
            severity, when_cond, col_expr = cls._extract_base_params(check, column)
            fields = check.get("feilds", check.get("fields", []))

            if not fields or not isinstance(fields, list):
                raise ValueError(
                    f"Check 'feilds_not_null' on column '{column}' requires a non-empty 'feilds' or 'fields' list."
                )

            null_checks = [f"{col_expr}.{field} IS NULL" for field in fields]
            condition = " OR ".join(null_checks)
            error_tag = cls._format_error_tag(column, "", error_suffix)

            sql = f"""
            CASE
                WHEN ({when_cond}) AND {col_expr} IS NOT NULL AND ({condition})
                THEN array('{error_tag}')
                ELSE {cls.EMPTY_ARRAY_SQL}
            END
            """
            return sql, severity, error_suffix
        except Exception as e:
            raise RuntimeError(f"[StructQualityChecks] Error in 'feilds_not_null' check for column '{column}': {e}") from e

    @classmethod
    def field_not_null_check(cls, check: dict, column: str) -> Tuple[str, str, str]:
        error_suffix = "STRUCT_FIELD_NULL_ERROR"
        try:
            severity, when_cond, col_expr = cls._extract_base_params(check, column)
            field = check.get("feild", check.get("field"))

            if not field:
                raise ValueError(f"Check 'feild_not_null' on column '{column}' requires 'feild' or 'field'.")

            field_expr = f"{col_expr}.{field}"
            condition = f"{field_expr} IS NULL"
            error_tag = cls._format_error_tag(column, field, error_suffix)

            sql = f"""
            CASE
                WHEN ({when_cond}) AND {col_expr} IS NOT NULL AND ({condition})
                THEN array('{error_tag}')
                ELSE {cls.EMPTY_ARRAY_SQL}
            END
            """
            return sql, severity, error_suffix
        except Exception as e:
            raise RuntimeError(f"[StructQualityChecks] Error in 'feild_not_null' check for column '{column}': {e}") from e

    @classmethod
    def field_regex_check(cls, check: dict, column: str) -> Tuple[str, str, str]:
        """Validates that a specific struct field matches a given regex pattern."""
        error_suffix = "STRUCT_FIELD_REGEX_ERROR"
        try:
            severity, when_cond, col_expr = cls._extract_base_params(check, column)
            
            field = check.get("feild", check.get("field"))
            pattern = check.get("pattern", check.get("regex"))

            if not field or not pattern:
                raise ValueError(
                    f"Check 'feild_regex_match' on column '{column}' requires both 'feild'/'field' and 'pattern'/'regex'."
                )

            sql_safe_pattern = pattern.replace("'", "''")

            field_expr = f"{col_expr}.{field}"
            condition = f"{field_expr} IS NOT NULL AND NOT ({field_expr} RLIKE '{sql_safe_pattern}')"
            error_tag = cls._format_error_tag(column, field, error_suffix)

            sql = f"""
            CASE
                WHEN ({when_cond}) AND {col_expr} IS NOT NULL AND ({condition})
                THEN array('{error_tag}')
                ELSE {cls.EMPTY_ARRAY_SQL}
            END
            """
            return sql, severity, error_suffix
        except Exception as e:
            raise RuntimeError(f"[StructQualityChecks] Error in 'feild_regex_match' check for column '{column}': {e}") from e

    @classmethod
    def field_length_check(cls, check: dict, column: str) -> Tuple[str, str, str]:
        """Validates string length of a field within a struct using ranges min/max."""
        error_suffix = "STRUCT_FIELD_LENGTH_ERROR"
        try:
            severity, when_cond, col_expr = cls._extract_base_params(check, column)
            
            field = check.get("feild", check.get("field"))
            if not field:
                raise ValueError(f"Check 'feild_length' on column '{column}' requires a 'feild' or 'field' property.")

            range_cfg = check.get("ranges", check.get("range", check.get("length", check)))
            minimum = range_cfg.get("min")
            maximum = range_cfg.get("max")

            conditions = []
            field_expr = f"{col_expr}.{field}"

            if minimum is not None:
                conditions.append(f"length({field_expr}) < {minimum}")
            if maximum is not None:
                conditions.append(f"length({field_expr}) > {maximum}")

            if not conditions:
                raise ValueError(
                    f"Check 'feild_length' on column '{column}' requires at least 'min' or 'max' inside 'ranges'/'range'."
                )

            condition = f"{field_expr} IS NOT NULL AND (" + " OR ".join(conditions) + ")"
            error_tag = cls._format_error_tag(column, field, error_suffix)

            sql = f"""
            CASE
                WHEN ({when_cond}) AND {col_expr} IS NOT NULL AND ({condition})
                THEN array('{error_tag}')
                ELSE {cls.EMPTY_ARRAY_SQL}
            END
            """
            return sql, severity, error_suffix
        except Exception as e:
            raise RuntimeError(f"[StructQualityChecks] Error in 'feild_length' check for column '{column}': {e}") from e

    @classmethod
    def field_range_check(cls, check: dict, column: str) -> Tuple[str, str, str]:
        """Validates numeric range bounds for a field within a struct."""
        error_suffix = "STRUCT_FIELD_RANGE_ERROR"
        try:
            severity, when_cond, col_expr = cls._extract_base_params(check, column)
            
            field = check.get("feild", check.get("field"))
            if not field:
                raise ValueError(f"Check 'feild_range' on column '{column}' requires a 'feild' or 'field' property.")

            range_cfg = check.get("ranges", check.get("range", check))
            minimum = range_cfg.get("min")
            maximum = range_cfg.get("max")

            conditions = []
            field_expr = f"{col_expr}.{field}"

            if minimum is not None:
                conditions.append(f"{field_expr} < {minimum}")
            if maximum is not None:
                conditions.append(f"{field_expr} > {maximum}")

            if not conditions:
                raise ValueError(
                    f"Check 'feild_range' on column '{column}' requires at least 'min' or 'max' inside 'ranges'/'range'."
                )

            condition = f"{field_expr} IS NOT NULL AND (" + " OR ".join(conditions) + ")"
            error_tag = cls._format_error_tag(column, field, error_suffix)

            sql = f"""
            CASE
                WHEN ({when_cond}) AND {col_expr} IS NOT NULL AND ({condition})
                THEN array('{error_tag}')
                ELSE {cls.EMPTY_ARRAY_SQL}
            END
            """
            return sql, severity, error_suffix
        except Exception as e:
            raise RuntimeError(f"[StructQualityChecks] Error in 'feild_range' check for column '{column}': {e}") from e

    @classmethod
    def field_values_in_list_check(cls, check: dict, column: str) -> Tuple[str, str, str]:
        """Validates that a struct field value exists within an allowed list of values."""
        error_suffix = "STRUCT_FIELD_INVALID_VALUES_ERROR"
        try:
            severity, when_cond, col_expr = cls._extract_base_params(check, column)
            
            field = check.get("feild", check.get("field"))
            allowed_values = check.get("values", check.get("allowed_values", []))

            if not field or not allowed_values:
                raise ValueError(
                    f"Check 'feild_values_in_list' on column '{column}' requires 'feild'/'field' and 'values'/'allowed_values'."
                )

            formatted_vals = ", ".join([f"'{str(v).replace('\'', '\'\'')}'" if isinstance(v, str) else str(v) for v in allowed_values])
            field_expr = f"{col_expr}.{field}"
            condition = f"{field_expr} IS NOT NULL AND {field_expr} NOT IN ({formatted_vals})"
            error_tag = cls._format_error_tag(column, field, error_suffix)

            sql = f"""
            CASE
                WHEN ({when_cond}) AND {col_expr} IS NOT NULL AND ({condition})
                THEN array('{error_tag}')
                ELSE {cls.EMPTY_ARRAY_SQL}
            END
            """
            return sql, severity, error_suffix
        except Exception as e:
            raise RuntimeError(f"[StructQualityChecks] Error in 'feild_values_in_list' check for column '{column}': {e}") from e