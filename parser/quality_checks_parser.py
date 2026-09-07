import logging
from typing import Any, Dict, Set
from yamlpipe.parser.columns_quality_parser import ColumnQualityParser
from yamlpipe.parser.schema_checks_parser import SchemaQualityParser
from yamlpipe.parser.table_quality_parser import TableQualityParser

logger = logging.getLogger("QualityChecksParser")


class QualityChecksParser:

    @classmethod
    def parse_quality_checks(cls, yaml_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main entry point for parsing data quality rules from YAML configuration.
        """
        table_identifier = (
            yaml_config.get("table")
            or yaml_config.get("table_name")
            or yaml_config.get("target_table")
        )

        quality_config = yaml_config.get("quality_checks", yaml_config)

        # 1. Execute sub-parsers
        schema_results = SchemaQualityParser.parse_yaml_checks(quality_config)
        column_results = ColumnQualityParser.parse_yaml_checks(quality_config)
        table_results = TableQualityParser.parse_yaml_checks(quality_config)

        # 2. Aggregate custom check dependencies
        custom_checks_set: Set[str] = set()
        for res in (column_results, table_results, schema_results):
            sub_custom = res.get("ContainCustomChecksFrom", res.get("contain_custom_checks_from", []))
            if isinstance(sub_custom, list):
                custom_checks_set.update(sub_custom)

        # 3. Aggregate template dependencies across all sub-parsers
        templates_set: Set[str] = set()
        for res in (schema_results, column_results, table_results):
            sub_templates = res.get("ContainTemplatesFrom", [])
            if isinstance(sub_templates, list):
                templates_set.update(sub_templates)

        # 4. Extract existing ContainVarsFrom directly from config if already set
        contain_vars = yaml_config.get("ContainVarsFrom", quality_config.get("ContainVarsFrom", []))

        return {
            "table": table_identifier,
            "schema_checks": schema_results.get("schema_checks", []),
            "columns_checks": column_results.get("columns_checks", {
                "error_expr": [],
                "warn_expr": []
            }),
            "registered_error_suffixes": column_results.get("registered_error_suffixes", []),
            "table_checks": table_results.get("table_checks", {
                "checks": [],
                "temp_views_to_create": []
            }),
            "ContainVarsFrom": contain_vars,
            "ContainCustomChecksFrom": sorted(list(custom_checks_set)),
            "ContainTemplatesFrom": sorted(list(templates_set))
        }