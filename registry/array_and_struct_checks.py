import logging

logger = logging.getLogger("ArrayAndStructChecks")


class ArrayAndStructChecks:

    EMPTY_ARRAY_SQL = "CAST(array() AS ARRAY<STRING>)"

    @classmethod
    def _extract_base_params(cls, check: dict, column: str):
        """Helper to safely access base params without top-level circular import."""
        from yamlpipe.registry.columns_quality_registry import ColumnQualityRegistry
        return ColumnQualityRegistry._extract_base_params(check, column)

    @classmethod
    def router(cls, check: dict, column: str):
        """
        Routes array and struct check types to the appropriate handler method.
        """
        try:
            check_type = check["check_type"].lower().strip()
            logger.debug(f"Routing Array/Struct check '{check_type}' for column '{column}'")

            # --- ARRAY CHECKS ---
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

            # --- STRUCT CHECKS ---
            elif check_type == "struct_not_empty":
                return cls.struct_not_empty_check(check, column)
            elif check_type == "struct_fields_not_null":
                return cls.struct_fields_not_null_check(check, column)

            else:
                raise ValueError(f"Unsupported Array/Struct check type: '{check_type}'")

        except Exception as e:
            logger.error(f"Failed to route Array/Struct check '{check.get('check_type')}' for column '{column}': {str(e)}")
            raise

    # ARRAY CHECKS
    @classmethod
    def array_not_empty_check(cls, check: dict, column: str):
        """Validates that an array column is NOT NULL and size({column}) > 0."""
        severity, when_cond, col_expr = cls._extract_base_params(check, column)

        cast_cond = f"({col_expr} IS NULL OR size({col_expr}) = 0)"

        sql = f"""
        CASE
            WHEN ({when_cond}) AND ({cast_cond})
            THEN array('{column}_ARRAY_EMPTY_ERROR')
            ELSE {cls.EMPTY_ARRAY_SQL}
        END
        """
        return sql, severity

    @classmethod
    def array_values_in_list_check(cls, check: dict, column: str):
        """
        Validates that ALL elements in an array are present in a permitted list.
        Uses PySpark SQL 'forall' higher-order function.
        """
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
            THEN array('{column}_ARRAY_INVALID_VALUES_ERROR')
            ELSE {cls.EMPTY_ARRAY_SQL}
        END
        """
        return sql, severity

    @classmethod
    def array_length_check(cls, check: dict, column: str):
        """Validates array element count against minimum and/or maximum bounds."""
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
            THEN array('{column}_ARRAY_LENGTH_ERROR')
            ELSE {cls.EMPTY_ARRAY_SQL}
        END
        """
        return sql, severity

    @classmethod
    def array_no_nulls_check(cls, check: dict, column: str):
        """Ensures an array does not contain any internal NULL elements."""
        severity, when_cond, col_expr = cls._extract_base_params(check, column)

        cast_cond = f"exists({col_expr}, x -> x IS NULL)"

        sql = f"""
        CASE
            WHEN ({when_cond}) AND {col_expr} IS NOT NULL AND ({cast_cond})
            THEN array('{column}_ARRAY_CONTAINS_NULL_ERROR')
            ELSE {cls.EMPTY_ARRAY_SQL}
        END
        """
        return sql, severity

    @classmethod
    def array_distinct_values_check(cls, check: dict, column: str):
        """Ensures all elements inside the array are unique (no duplicates)."""
        severity, when_cond, col_expr = cls._extract_base_params(check, column)

        cast_cond = f"size({col_expr}) != size(array_distinct({col_expr}))"

        sql = f"""
        CASE
            WHEN ({when_cond}) AND {col_expr} IS NOT NULL AND ({cast_cond})
            THEN array('{column}_ARRAY_DUPLICATE_VALUES_ERROR')
            ELSE {cls.EMPTY_ARRAY_SQL}
        END
        """
        return sql, severity

    # STRUCT CHECKS
    @classmethod
    def struct_not_empty_check(cls, check: dict, column: str):
        """Validates that a struct column itself is NOT NULL."""
        severity, when_cond, col_expr = cls._extract_base_params(check, column)

        sql = f"""
        CASE
            WHEN ({when_cond}) AND ({col_expr} IS NULL)
            THEN array('{column}_STRUCT_EMPTY_ERROR')
            ELSE {cls.EMPTY_ARRAY_SQL}
        END
        """
        return sql, severity

    @classmethod
    def struct_fields_not_null_check(cls, check: dict, column: str):
        """
        Validates that specific fields inside a struct are not NULL.
        Example YAML fields: ["street", "city", "zipcode"]
        """
        severity, when_cond, col_expr = cls._extract_base_params(check, column)

        fields = check.get("fields", [])
        if not fields:
            raise ValueError(f"struct_fields_not_null check on '{column}' requires a 'fields' list.")

        null_checks = [f"{col_expr}.{field} IS NULL" for field in fields]
        condition = " OR ".join(null_checks)

        sql = f"""
        CASE
            WHEN ({when_cond}) AND {col_expr} IS NOT NULL AND ({condition})
            THEN array('{column}_STRUCT_FIELD_NULL_ERROR')
            ELSE {cls.EMPTY_ARRAY_SQL}
        END
        """
        return sql, severity