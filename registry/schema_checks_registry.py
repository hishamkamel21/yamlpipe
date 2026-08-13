from typing import Dict, List, Tuple, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr


class SchemaQualityRegistry:
    """
    Registry for schema-level data quality validation.
    Generates PySpark expressions to build a nested `schema_monitor` Struct column
    evaluating metadata lazily with zero Spark driver actions.
    """

    @staticmethod
    def _normalize_type(data_type: str) -> str:
        """
        Normalizes common SQL / PySpark type aliases for accurate comparison.
        """
        t = str(data_type).strip().lower()
        type_map = {
            "int": "int",
            "integer": "int",
            "bigint": "long",
            "long": "long",
            "smallint": "short",
            "tinyint": "byte",
            "double": "double",
            "float": "float",
            "str": "string",
            "string": "string",
            "bool": "boolean",
            "boolean": "boolean",
            "date": "date",
            "timestamp": "timestamp"
        }
        return type_map.get(t, t)

    @classmethod
    def router(cls, df: DataFrame, check: dict) -> Tuple[str, str, str]:
        if not isinstance(check, dict):
            raise TypeError(f"[SchemaQualityRegistry] Check rule must be a dict, got '{type(check).__name__}'.")

        check_type = check.get("check_type", check.get("type", "N/A")).lower()
        severity = check.get("severity", "error").lower() 

        handlers = {
            "required_missing": cls._handle_required_missing,
            "required_columns": cls._handle_required_missing,
            "type_mismatch": cls._handle_type_mismatch,
            "no_duplicate_columns": cls._handle_no_duplicate_columns,
            "no_duplicated_columns": cls._handle_no_duplicate_columns,
            "forbidden_exist": cls._handle_forbidden_exist,
            "forbidden_exists": cls._handle_forbidden_exist
        }

        handler = handlers.get(check_type)
        if not handler:
            raise ValueError(f"[SchemaQualityRegistry] Unsupported check_type: '{check_type}'.")

        std_key = "required_missing" if check_type == "required_columns" else check_type

        return std_key, handler(df=df, check=check), severity

    @classmethod
    def apply_schema_checks(cls, df: DataFrame, schema_checks: List[dict]) -> DataFrame:
        if not schema_checks:
            return df

        struct_fields: Dict[str, str] = {}

        for check in schema_checks:
            try:
                key, field_expr, _ = cls.router(df, check)
                struct_fields[key] = field_expr
            except Exception as e:
                raise RuntimeError(f"Schema compilation failed for rule: {check}") from e

        default_fields = {
            "required_missing": "CAST(ARRAY() AS ARRAY<STRING>)",
            "type_mismatch": "CAST(ARRAY() AS ARRAY<STRUCT<column:STRING, expected_type:STRING, actual_type:STRING>>)",
            "no_duplicate_columns": "STRUCT(true AS condition, CAST(ARRAY() AS ARRAY<STRING>) AS columns)",
            "forbidden_exist": "CAST(ARRAY() AS ARRAY<STRING>)",
        }

        for key, default_expr in default_fields.items():
            if key not in struct_fields:
                struct_fields[key] = default_expr

        status_expr = (
            f"CASE WHEN SIZE({struct_fields['required_missing']}) = 0 "
            f"AND SIZE({struct_fields['type_mismatch']}) = 0 "
            f"AND {struct_fields['no_duplicate_columns']}.condition = true "
            f"AND SIZE({struct_fields['forbidden_exist']}) = 0 "
            f"THEN 'PASSED' ELSE 'FAILED' END"
        )

        full_struct_expr = (
            f"NAMED_STRUCT("
            f"'status', {status_expr}, "
            f"'required_missing', {struct_fields['required_missing']}, "
            f"'type_mismatch', {struct_fields['type_mismatch']}, "
            f"'no_duplicate_columns', {struct_fields['no_duplicate_columns']}, "
            f"'forbidden_exist', {struct_fields['forbidden_exist']}"
            f")"
        )

        return df.withColumn("schema_monitor", expr(full_struct_expr))

    @classmethod
    def _handle_required_missing(cls, df: DataFrame, check: dict) -> str:
        required_cols = check.get("columns", [])
        if not required_cols:
            return "CAST(ARRAY() AS ARRAY<STRING>)"

        existing_cols = set(df.columns)
        missing_cols = [c for c in required_cols if c not in existing_cols]

        if not missing_cols:
            return "CAST(ARRAY() AS ARRAY<STRING>)"

        formatted_cols = [f"'{c}'" for c in missing_cols]
        return f"ARRAY({','.join(formatted_cols)})"

    @classmethod
    def _handle_type_mismatch(cls, df: DataFrame, check: dict) -> str:
        expected_types: Dict[str, str] = check.get("columns", {})
        if not expected_types:
            return "CAST(ARRAY() AS ARRAY<STRUCT<column:STRING, expected_type:STRING, actual_type:STRING>>)"

        # Normalizing current schema dtypes
        current_dtypes = {col_name: cls._normalize_type(dtype) for col_name, dtype in df.dtypes}
        mismatch_structs = []

        for col_name, expected_type in expected_types.items():
            expected_type_norm = cls._normalize_type(expected_type)

            if col_name in current_dtypes:
                actual_type_norm = current_dtypes[col_name]

                if actual_type_norm != expected_type_norm:
                    struct_expr = (
                        f"NAMED_STRUCT('column', '{col_name}', "
                        f"'expected_type', '{expected_type_norm}', "
                        f"'actual_type', '{actual_type_norm}')"
                    )
                    mismatch_structs.append(struct_expr)
            else:
                # Column is completely missing from DataFrame
                struct_expr = (
                    f"NAMED_STRUCT('column', '{col_name}', "
                    f"'expected_type', '{expected_type_norm}', "
                    f"'actual_type', 'MISSING_COLUMN')"
                )
                mismatch_structs.append(struct_expr)

        if not mismatch_structs:
            return "CAST(ARRAY() AS ARRAY<STRUCT<column:STRING, expected_type:STRING, actual_type:STRING>>)"

        return f"ARRAY({','.join(mismatch_structs)})"

    @classmethod
    def _handle_no_duplicate_columns(cls, df: DataFrame, check: dict) -> str:
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
            return "STRUCT(true AS condition, CAST(ARRAY() AS ARRAY<STRING>) AS columns)"

        formatted_dups = [f"'{c}'" for c in duplicates]
        return f"STRUCT(false AS condition, ARRAY({','.join(formatted_dups)}) AS columns)"

    @classmethod
    def _handle_forbidden_exist(cls, df: DataFrame, check: dict) -> str:
        forbidden_cols = check.get("columns", [])
        if not forbidden_cols:
            return "CAST(ARRAY() AS ARRAY<STRING>)"

        existing_cols = set(df.columns)
        found_forbidden = [c for c in forbidden_cols if c in existing_cols]

        if not found_forbidden:
            return "CAST(ARRAY() AS ARRAY<STRING>)"

        formatted_forbidden = [f"'{c}'" for c in found_forbidden]
        return f"ARRAY({','.join(formatted_forbidden)})"