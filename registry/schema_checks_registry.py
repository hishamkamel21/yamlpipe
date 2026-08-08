import logging
from typing import Dict, List, Tuple, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

logger = logging.getLogger("SchemaQualityRegistry")


class SchemaQualityRegistry:
    """
    Registry for schema-level data quality validation.
    Generates PySpark expressions to build a nested `schema_monitor` Struct column
    evaluating metadata lazily with zero Spark driver actions.
    """

    @classmethod
    def router(cls, df: DataFrame, check: dict) -> Tuple[str, str, str]:
        """
        Routes incoming schema checks to their corresponding generator methods.

        :param df: Target PySpark DataFrame.
        :param check: Dictionary containing schema check configuration.
        :return: Tuple of (check_type: str, struct_field_sql_expr: str, severity: str)
        """
        if not isinstance(check, dict):
            raise TypeError(f"[SchemaQualityRegistry] Check rule must be a dict, got '{type(check).__name__}'.")

        check_type = check.get("check_type",check.get("type","N/A")).lower()
        severity = check.get("severity", "error").lower() 

        handlers = {
            "required_missing": cls._handle_required_missing,
            "required_columns": cls._handle_required_missing,  # Alias support
            "type_mismatch": cls._handle_type_mismatch,
            "no_duplicate_columns": cls._handle_no_duplicate_columns,
            "no_duplicated_columns": cls._handle_no_duplicate_columns,
            "forbidden_exist": cls._handle_forbidden_exist,
        }

        handler = handlers.get(check_type)
        if not handler:
            raise ValueError(f"[SchemaQualityRegistry] Unsupported check_type: '{check_type}'.")

        # Standardize check key name for consistency
        std_key = "required_missing" if check_type == "required_columns" else check_type

        return std_key, handler(df=df, check=check), severity

    @classmethod
    def apply_schema_checks(cls, df: DataFrame, schema_checks: List[dict]) -> DataFrame:
        """
        Main entry point for evaluating schema rules.
        Combines check expressions into a single structured PySpark `schema_monitor` column.

        :param df: Input PySpark DataFrame.
        :param schema_checks: List of schema check configurations.
        :return: Updated DataFrame with `schema_monitor` Struct column attached.
        """
        if not schema_checks:
            return df

        struct_fields: Dict[str, str] = {}

        for check in schema_checks:
            try:
                key, field_expr, _ = cls.router(df, check)
                struct_fields[key] = field_expr
            except Exception as e:
                logger.error(f"Failed to compile schema check '{check.get('check_type')}': {str(e)}")
                raise RuntimeError(f"Schema compilation failed for rule: {check}") from e

        # Ensure all key fields exist in the output struct even if omitted from YAML
        default_fields = {
            "required_missing": "array().cast('array<string>')",
            "type_mismatch": "array().cast('array<struct<column:string,expected_type:string,actual_type:string>>')",
            "no_duplicate_columns": "struct(true AS condition, array().cast('array<string>') AS columns)",
            "forbidden_exist": "array().cast('array<string>')",
        }

        for key, default_expr in default_fields.items():
            if key not in struct_fields:
                struct_fields[key] = default_expr

        # Build single named struct expression
        struct_args = []
        for key in ["required_missing", "type_mismatch", "no_duplicate_columns", "forbidden_exist"]:
            struct_args.append(f"{struct_fields[key]} AS {key}")

        full_struct_expr = f"struct({','.join(struct_args)})"

        return df.withColumn("schema_monitor", expr(full_struct_expr))

    # -------------------------------------------------------------------------
    # Internal Rule Generators (Zero Spark Actions)
    # -------------------------------------------------------------------------

    @classmethod
    def _handle_required_missing(cls, df: DataFrame, check: dict) -> str:
        """
        Returns an array of column names that were marked required but are missing.
        Output: array<string>
        """
        required_cols = check.get("columns", [])
        if not required_cols:
            return "array().cast('array<string>')"

        existing_cols = set(df.columns)
        missing_cols = [c for c in required_cols if c not in existing_cols]

        if not missing_cols:
            return "array().cast('array<string>')"

        formatted_cols = [f"'{c}'" for c in missing_cols]
        return f"array({','.join(formatted_cols)})"

    @classmethod
    def _handle_type_mismatch(cls, df: DataFrame, check: dict) -> str:
        """
        Returns an array of structs identifying mismatched column types.
        Output: array<struct<column:string, expected_type:string, actual_type:string>>
        """
        expected_types: Dict[str, str] = check.get("columns", {})
        if not expected_types:
            return "array().cast('array<struct<column:string,expected_type:string,actual_type:string>>')"

        current_dtypes = dict(df.dtypes)
        mismatch_structs = []

        for col_name, expected_type in expected_types.items():
            if col_name in current_dtypes:
                actual_type = current_dtypes[col_name].lower()
                expected_type_norm = str(expected_type).lower()

                if actual_type != expected_type_norm:
                    struct_expr = (
                        f"struct('{col_name}' AS column, "
                        f"'{expected_type_norm}' AS expected_type, "
                        f"'{actual_type}' AS actual_type)"
                    )
                    mismatch_structs.append(struct_expr)

        if not mismatch_structs:
            return "array().cast('array<struct<column:string,expected_type:string,actual_type:string>>')"

        return f"array({','.join(mismatch_structs)})"

    @classmethod
    def _handle_no_duplicate_columns(cls, df: DataFrame, check: dict) -> str:
        """
        Validates duplicate column names in the schema definition.
        Output: struct<condition:boolean, columns:array<string>>
        """
        raw_cols = df.columns
        seen = set()
        duplicates = set()

        for c in raw_cols:
            c_lower = c.lower()
            if c_lower in seen:
                duplicates.add(c)
            else:
                seen.add(c_lower)

        if not duplicates:
            return "struct(true AS condition, array().cast('array<string>') AS columns)"

        formatted_dups = [f"'{c}'" for c in duplicates]
        return f"struct(false AS condition, array({','.join(formatted_dups)}) AS columns)"

    @classmethod
    def _handle_forbidden_exist(cls, df: DataFrame, check: dict) -> str:
        """
        Returns an array of forbidden column names found in the DataFrame.
        Output: array<string>
        """
        forbidden_cols = check.get("columns", [])
        if not forbidden_cols:
            return "array().cast('array<string>')"

        existing_cols = set(df.columns)
        found_forbidden = [c for c in forbidden_cols if c in existing_cols]

        if not found_forbidden:
            return "array().cast('array<string>')"

        formatted_forbidden = [f"'{c}'" for c in found_forbidden]
        return f"array({','.join(formatted_forbidden)})"