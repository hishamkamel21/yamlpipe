from yamlpipe.utility.module_loader import ModuleLoader
from yamlpipe.utility.helper import Helper
from yamlpipe.registry.array_and_struct_checks import ArrayAndStructChecks
from yamlpipe.utility.enums import AllowedOperator, DataTypeAlias


class ColumnQualityRegistry:

    ALLOWED_OPERATORS = {op.value for op in AllowedOperator}
    TYPE_MAPPING = {member.name.lower(): member.value for member in DataTypeAlias}
    EMPTY_ARRAY_SQL = "CAST(array() AS ARRAY<STRING>)"

    @classmethod
    def router(cls, check: dict, column: str):
        if not isinstance(check, dict) or ("check_type" not in check and "type" not in check):
            raise KeyError(f"Check definition for column '{column}' must contain 'check_type'.")

        check_type = check.get("check_type", check.get("type", None))

        prefix = check_type.split("_")[0]
        if prefix in ("array", "struct"):
            return ArrayAndStructChecks.router(check, column)

        dispatch = {
            "not_null": cls.not_null_check,
            "not_empty": cls.not_empty_check,
            "regex": cls.regex_check,
            "accepted_values": cls.accepted_values_check,
            "value_in_list": cls.accepted_values_check,
            "range": cls.range_check,
            "length": cls.length_check,
            "compare_columns": cls.compare_columns_check,
            "compare": cls.compare_columns_check,
            "is_type": cls.is_type_check,
            "not_future_date": cls.not_future_date_check,
            "not_future_time": cls.not_future_time_check,
            "custom": cls.custom_check,
        }

        handler = dispatch.get(check_type)
        if not handler:
            raise ValueError(f"Unsupported check type '{check_type}' for column '{column}'")

        return handler(check, column)

    @staticmethod
    def _extract_base_params(check: dict, column: str):
        severity = check.get("severity", "warn").lower().strip()
        when_cond = Helper.clean_multiline_sql(check.get("when", "1 = 1"))
        col_expr = Helper.clean_multiline_sql(check.get("expression", column))
        return severity, when_cond, col_expr

    @staticmethod
    def not_null_check(check: dict, column: str):
        error_suffix = "NULL_ERROR"
        severity, when_cond, col_expr = ColumnQualityRegistry._extract_base_params(check, column)
        sql = f"""
        CASE
            WHEN ({when_cond}) AND {col_expr} IS NULL
            THEN array('{column}_{error_suffix}')
            ELSE {ColumnQualityRegistry.EMPTY_ARRAY_SQL}
        END
        """
        return sql, severity, error_suffix

    @staticmethod
    def not_empty_check(check: dict, column: str):
        error_suffix = "EMPTY_ERROR"
        severity, when_cond, col_expr = ColumnQualityRegistry._extract_base_params(check, column)
        sql = f"""
        CASE
            WHEN ({when_cond}) AND ({col_expr} IS NULL OR trim({col_expr}) = '')
            THEN array('{column}_{error_suffix}')
            ELSE {ColumnQualityRegistry.EMPTY_ARRAY_SQL}
        END
        """
        return sql, severity, error_suffix

    @staticmethod
    def regex_check(check: dict, column: str):
        error_suffix = "REGEX_ERROR"
        severity, when_cond, col_expr = ColumnQualityRegistry._extract_base_params(check, column)
        if "pattern" not in check:
            raise KeyError(f"Regex check on column '{column}' requires a 'pattern' key.")
        pattern = check["pattern"]
        sql = f"""
        CASE
            WHEN ({when_cond}) AND NOT ({col_expr} RLIKE '{pattern}')
            THEN array('{column}_{error_suffix}')
            ELSE {ColumnQualityRegistry.EMPTY_ARRAY_SQL}
        END
        """
        return sql, severity, error_suffix

    @staticmethod
    def accepted_values_check(check: dict, column: str):
        error_suffix = "VALUE_ERROR"
        severity, when_cond, col_expr = ColumnQualityRegistry._extract_base_params(check, column)
        if "values" not in check or not isinstance(check["values"], list):
            raise KeyError(f"Accepted values check on column '{column}' requires a 'values' list.")
        raw_values = check["values"]
        values = ",".join([f"'{v}'" if isinstance(v, str) else str(v) for v in raw_values])
        sql = f"""
        CASE
            WHEN ({when_cond}) AND {col_expr} NOT IN ({values})
            THEN array('{column}_{error_suffix}')
            ELSE {ColumnQualityRegistry.EMPTY_ARRAY_SQL}
        END
        """
        return sql, severity, error_suffix

    @staticmethod
    def range_check(check: dict, column: str):
        error_suffix = "RANGE_ERROR"
        severity, when_cond, col_expr = ColumnQualityRegistry._extract_base_params(check, column)
        ranges = check.get("ranges", check)
        minimum = ranges.get("min")
        maximum = ranges.get("max")

        conditions = []
        if minimum is not None:
            conditions.append(f"{col_expr} < {minimum}")
        if maximum is not None:
            conditions.append(f"{col_expr} > {maximum}")

        if not conditions:
            raise ValueError("Range check requires at least 'min' or 'max'.")

        condition = " OR ".join(conditions)
        sql = f"""
        CASE
            WHEN ({when_cond}) AND ({condition})
            THEN array('{column}_{error_suffix}')
            ELSE {ColumnQualityRegistry.EMPTY_ARRAY_SQL}
        END
        """
        return sql, severity, error_suffix

    @staticmethod
    def length_check(check: dict, column: str):
        error_suffix = "LENGTH_ERROR"
        severity, when_cond, col_expr = ColumnQualityRegistry._extract_base_params(check, column)
        length_cfg = check.get("ranges", check)
        minimum = length_cfg.get("min")
        maximum = length_cfg.get("max")

        conditions = []
        if minimum is not None:
            conditions.append(f"length({col_expr}) < {minimum}")
        if maximum is not None:
            conditions.append(f"length({col_expr}) > {maximum}")

        if not conditions:
            raise ValueError("Length check requires at least 'min' or 'max'.")

        condition = " OR ".join(conditions)
        sql = f"""
        CASE
            WHEN ({when_cond}) AND ({condition})
            THEN array('{column}_{error_suffix}')
            ELSE {ColumnQualityRegistry.EMPTY_ARRAY_SQL}
        END
        """
        return sql, severity, error_suffix

    @staticmethod
    def compare_columns_check(check: dict, column: str):
        error_suffix = "COMPARE_ERROR"
        severity, when_cond, col_expr = ColumnQualityRegistry._extract_base_params(check, column)
        if "target_column" not in check:
            raise KeyError(f"compare_columns check on column '{column}' requires 'target_column'.")

        target_column = str(check["target_column"]).strip()
        operator = check.get("operator", ">=").strip()

        if not AllowedOperator.is_valid(operator):
            raise ValueError(f"Invalid operator '{operator}' in compare_columns check.")

        sql = f"""
        CASE
            WHEN ({when_cond}) 
                 AND {col_expr} IS NOT NULL 
                 AND {target_column} IS NOT NULL 
                 AND NOT ({col_expr} {operator} {target_column})
            THEN array('{column}_{error_suffix}')
            ELSE {ColumnQualityRegistry.EMPTY_ARRAY_SQL}
        END
        """
        return sql, severity, error_suffix

    @staticmethod
    def is_type_check(check: dict, column: str):
        error_suffix = "TYPE_ERROR"
        severity, when_cond, col_expr = ColumnQualityRegistry._extract_base_params(check, column)
        if "expected_type" not in check or not check["expected_type"]:
            raise KeyError(f"is_type check on column '{column}' requires 'expected_type'.")

        raw_type = str(check["expected_type"]).lower().strip()
        target_type = DataTypeAlias.normalize(raw_type)
        diff_formats = check.get("diff_formats", False)

        if diff_formats and target_type == "date":
            parse_expr = Helper._get_date_formats_expr(col_expr)
            cast_cond = f"{parse_expr} IS NULL"
        elif diff_formats and target_type == "timestamp":
            parse_expr = Helper._get_timestamp_formats_expr(col_expr)
            cast_cond = f"{parse_expr} IS NULL"
        else:
            fmt = check.get("format")
            if not fmt or str(fmt).strip() == "":
                if target_type == "date":
                    fmt = "yyyy-MM-dd"
                elif target_type == "timestamp":
                    fmt = "yyyy-MM-dd HH:mm:ss"

            if target_type == "date":
                cast_cond = f"to_date({col_expr}, '{fmt}') IS NULL"
            elif target_type == "timestamp":
                cast_cond = f"to_timestamp({col_expr}, '{fmt}') IS NULL"
            else:
                cast_cond = f"try_cast({col_expr} AS {target_type}) IS NULL"

        sql = f"""
        CASE
            WHEN ({when_cond}) 
                 AND {col_expr} IS NOT NULL 
                 AND ({cast_cond})
            THEN array('{column}_{error_suffix}')
            ELSE {ColumnQualityRegistry.EMPTY_ARRAY_SQL}
        END
        """
        return sql, severity, error_suffix

    @staticmethod
    def not_future_date_check(check: dict, column: str):
        error_suffix = "FUTURE_DATE_ERROR"
        severity, when_cond, col_expr = ColumnQualityRegistry._extract_base_params(check, column)
        sql = f"""
        CASE
            WHEN ({when_cond}) AND {col_expr} > current_date()
            THEN array('{column}_{error_suffix}')
            ELSE {ColumnQualityRegistry.EMPTY_ARRAY_SQL}
        END
        """
        return sql, severity, error_suffix

    @staticmethod
    def not_future_time_check(check: dict, column: str):
        error_suffix = "FUTURE_TIME_ERROR"
        severity, when_cond, col_expr = ColumnQualityRegistry._extract_base_params(check, column)
        sql = f"""
        CASE
            WHEN ({when_cond}) AND {col_expr} > current_timestamp()
            THEN array('{column}_{error_suffix}')
            ELSE {ColumnQualityRegistry.EMPTY_ARRAY_SQL}
        END
        """
        return sql, severity, error_suffix

    @staticmethod
    def custom_check(check: dict, column: str):
        if "the_check" not in check:
            raise KeyError(f"Custom check on column '{column}' requires 'the_check' field.")

        check_name = check["the_check"]
        error_suffix = f"{check_name.upper()}_ERROR"
        params = check.get("params", [])
        severity, when_cond, _ = ColumnQualityRegistry._extract_base_params(check, column)

        if isinstance(params, dict):
            check_expr = ModuleLoader.custom_checks_loader(check_name, **params)
        elif isinstance(params, list):
            check_expr = ModuleLoader.custom_checks_loader(check_name, *params)
        else:
            check_expr = ModuleLoader.custom_checks_loader(check_name, params)

        sql = f"""
        CASE
            WHEN ({when_cond}) AND ({check_expr})
            THEN array('{column}_{error_suffix}')
            ELSE {ColumnQualityRegistry.EMPTY_ARRAY_SQL}
        END
        """
        return sql, severity, error_suffix


