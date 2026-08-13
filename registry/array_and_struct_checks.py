from typing import Tuple 
from yamlpipe.registry.struct_checks_registry import StructQualityChecks 
from yamlpipe.registry.array_checks_registry import ArrayQualityChecks 


class ArrayAndStructChecks:
    """Sub-router directing incoming Array and Struct quality checks."""

    @classmethod
    def router(cls, check: dict, column: str) -> Tuple[str, str, str]:
        check_type = check.get("check_type", check.get("type", "")).lower().strip()

        dispatch = {
            # ------------------------------------------------------------------
            # Array Checks
            # ------------------------------------------------------------------
            "array_not_empty": ArrayQualityChecks.not_empty_check,
            "arr_not_empty": ArrayQualityChecks.not_empty_check,

            "array_values_in_list": ArrayQualityChecks.values_in_list_check,
            "arr_values_in_list": ArrayQualityChecks.values_in_list_check,
            "array_accepeted_values": ArrayQualityChecks.values_in_list_check,
            "arr_accepeted_values": ArrayQualityChecks.values_in_list_check,

            "array_values_regex": ArrayQualityChecks.values_regex_check,
            "arr_values_regex": ArrayQualityChecks.values_regex_check,
            "arr_regex": ArrayQualityChecks.values_regex_check,
            "arr_regex_match":ArrayQualityChecks.values_regex_check,
            "values_regex": ArrayQualityChecks.values_regex_check,
            "values_regex_match": ArrayQualityChecks.values_regex_check,

            "array_values_range": ArrayQualityChecks.values_range_check,
            "arr_values_range": ArrayQualityChecks.values_range_check,
            "values_range": ArrayQualityChecks.values_range_check,

            "array_length": ArrayQualityChecks.length_check,
            "array_min_length": ArrayQualityChecks.length_check,
            "array_max_length": ArrayQualityChecks.length_check,

            "array_no_nulls": ArrayQualityChecks.no_nulls_check,
            "no_nulls": ArrayQualityChecks.no_nulls_check,

            "array_distinct_values": ArrayQualityChecks.distinct_values_check,
            "distinct_values": ArrayQualityChecks.distinct_values_check,
            "arr_distinct_values": ArrayQualityChecks.distinct_values_check,

            # ------------------------------------------------------------------
            # Struct Checks (Mapping user check_types and standard check_types)
            # ------------------------------------------------------------------
            "struct_not_empty": StructQualityChecks.not_empty_check,

            "feilds_not_null": StructQualityChecks.fields_not_null_check,
            "struct_fields_not_null": StructQualityChecks.fields_not_null_check,
            "fields_not_null": StructQualityChecks.fields_not_null_check,

            "feild_not_null": StructQualityChecks.field_not_null_check,
            "struct_field_not_null": StructQualityChecks.field_not_null_check,
            "field_not_null": StructQualityChecks.field_not_null_check,

            "feild_regex_match": StructQualityChecks.field_regex_check,
            "struct_field_regex": StructQualityChecks.field_regex_check,
            "field_regex_match": StructQualityChecks.field_regex_check,

            "feild_length": StructQualityChecks.field_length_check,
            "struct_field_length": StructQualityChecks.field_length_check,
            "field_length": StructQualityChecks.field_length_check,

            "feild_range": StructQualityChecks.field_range_check,
            "struct_field_range": StructQualityChecks.field_range_check,
            "field_range": StructQualityChecks.field_range_check,

            "feild_values_in_list": StructQualityChecks.field_values_in_list_check,
            "struct_field_values_in_list": StructQualityChecks.field_values_in_list_check,
            "field_values_in_list": StructQualityChecks.field_values_in_list_check,
        }

        handler = dispatch.get(check_type)
        if not handler:
            raise ValueError(f"Unsupported Array/Struct check type: '{check_type}' for column '{column}'")

        return handler(check, column)