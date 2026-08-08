
from yamlpipe.registry.columns_quality_registry import ColumnQualityRegistry

class ArrayAndStructChecks:

    EMPTY_ARRAY_SQL = "CAST(array() AS ARRAY<STRING>)"

    @classmethod
    def _extract_base_params(cls, check: dict, column: str):
        return ColumnQualityRegistry._extract_base_params(check, column)

    @classmethod
    def router(cls, check: dict, column: str):
        check_type = check["check_type"].lower().strip()

        if check_type == "array_not_empty":
            return cls.array_not_empty_check(check, column)
        elif check_type == "array_values_in_list":
            return cls.array_values_in_list_check(check, column)
        elif check_type in ("array_min_length", "array_max_length", "array_length"):
            return cls.array_length_check(check, column)
        elif check_type == "array_no_nulls":
            return cls.array_no_nulls_check(check, column)
        elif check_type == "array_distinct_values":
            return cls.array_distinct_values_check(check, column)
        elif check_type == "struct_not_empty":
            return cls.struct_not_empty_check(check, column)
        elif check_type == "struct_fields_not_null":
            return cls.struct_fields_not_null_check(check, column)
        else:
            raise ValueError(f"Unsupported Array/Struct check type: '{check_type}'")

    @classmethod
    def array_not_empty_check(cls, check: dict, column: str):
        error_suffix = "ARRAY_EMPTY_ERROR"
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

    @classmethod
    def array_values_in_list_check(cls, check: dict, column: str):
        error_suffix = "ARRAY_INVALID_VALUES_ERROR"
        severity, when_cond, col_expr = cls._extract_base_params(check, column)
        allowed_values = check.get("values", check.get("allowed_values", []))
        if not allowed_values:
            raise ValueError(f"array_values_in_list check for '{column}' requires 'values' list.")

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

    @classmethod
    def array_length_check(cls, check: dict, column: str):
        error_suffix = "ARRAY_LENGTH_ERROR"
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
            raise ValueError(f"Array length check on '{column}' requires 'min' or 'max'.")

        condition = " OR ".join(conditions)
        sql = f"""
        CASE
            WHEN ({when_cond}) AND {col_expr} IS NOT NULL AND ({condition})
            THEN array('{column}_{error_suffix}')
            ELSE {cls.EMPTY_ARRAY_SQL}
        END
        """
        return sql, severity, error_suffix

    @classmethod
    def array_no_nulls_check(cls, check: dict, column: str):
        error_suffix = "ARRAY_CONTAINS_NULL_ERROR"
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

    @classmethod
    def array_distinct_values_check(cls, check: dict, column: str):
        error_suffix = "ARRAY_DUPLICATE_VALUES_ERROR"
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

    @classmethod
    def struct_not_empty_check(cls, check: dict, column: str):
        error_suffix = "STRUCT_EMPTY_ERROR"
        severity, when_cond, col_expr = cls._extract_base_params(check, column)
        sql = f"""
        CASE
            WHEN ({when_cond}) AND ({col_expr} IS NULL)
            THEN array('{column}_{error_suffix}')
            ELSE {cls.EMPTY_ARRAY_SQL}
        END
        """
        return sql, severity, error_suffix

    @classmethod
    def struct_fields_not_null_check(cls, check: dict, column: str):
        error_suffix = "STRUCT_FIELD_NULL_ERROR"
        severity, when_cond, col_expr = cls._extract_base_params(check, column)
        fields = check.get("fields", [])
        if not fields:
            raise ValueError(f"struct_fields_not_null check on '{column}' requires a 'fields' list.")

        null_checks = [f"{col_expr}.{field} IS NULL" for field in fields]
        condition = " OR ".join(null_checks)
        sql = f"""
        CASE
            WHEN ({when_cond}) AND {col_expr} IS NOT NULL AND ({condition})
            THEN array('{column}_{error_suffix}')
            ELSE {cls.EMPTY_ARRAY_SQL}
        END
        """
        return sql, severity, error_suffix