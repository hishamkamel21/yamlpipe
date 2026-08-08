import logging
from typing import Dict, Any
from yamlpipe.parser.columns_quality_parser import ColumnQualityParser
from yamlpipe.parser.schema_checks_parser import SchemaQualityParser
from yamlpipe.parser.table_quality_parser import TableQualityParser

logger = logging.getLogger("QualityChecksParser")


class QualityChecksParser:

    @classmethod
    def parse_quality_checks(cls, yaml_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main entry point for parsing data quality rules from YAML configuration.
        Delegates to SchemaQualityParser, ColumnQualityParser, and TableQualityParser.

        Returns:
            dict: {
                "table": "catalog.schema.table_name",
                "schema_checks": [ ... raw yaml objects ... ],
                "columns_checks": {
                    "error_expr": [ ... list of SQL expressions ... ],
                    "warn_expr": [ ... list of SQL expressions ... ]
                },
                "registered_error_suffixes": [ ... list of error suffixes ... ],
                "table_checks": {
                    "expr": "...",
                    "temp_views_to_create": [...]
                }
            }
        """
        # 1. Extract target table identifier from the top-level YAML configuration
        table_identifier = (
            yaml_config.get("table")
            or yaml_config.get("table_name")
            or yaml_config.get("target_table")
        )

        quality_config = yaml_config.get("quality_checks", yaml_config)

        # 2. Parse Schema Checks (returns raw YAML objects)
        schema_results = SchemaQualityParser.parse_yaml_checks(quality_config)

        # 3. Parse Column Checks (returns SQL expressions & registered_error_suffixes)
        column_results = ColumnQualityParser.parse_yaml_checks(quality_config)

        # 4. Parse Table Checks (delegates to TableQualityParser)
        table_results = TableQualityParser.parse_yaml_checks(quality_config)

        return {
            "table": table_identifier,
            "schema_checks": schema_results.get("schema_checks", []),
            "columns_checks": column_results.get("columns_checks", {
                "error_expr": [],
                "warn_expr": []
            }),
            "registered_error_suffixes": column_results.get("registered_error_suffixes", []),
            "table_checks": table_results.get("table_checks", {
                "expr": "",
                "temp_views_to_create": []
            })
        }