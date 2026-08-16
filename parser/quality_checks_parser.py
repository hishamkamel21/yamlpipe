

import logging
from typing import Any, Dict, Generator, List, Set, Tuple
from yamlpipe.core.vars_manager import VariablesManager
from yamlpipe.registry.columns_quality_registry import ColumnQualityRegistry
from yamlpipe.parser.schema_checks_parser import SchemaQualityParser
from yamlpipe.parser.table_quality_parser import TableQualityParser


class QualityChecksParser:

    @classmethod
    def parse_quality_checks(cls, yaml_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main entry point for parsing data quality rules from YAML configuration.
        Delegates to SchemaQualityParser, ColumnQualityParser, and TableQualityParser.
        Aggregates variable dependencies ('contain_vars_from') and custom Python check 
        dependencies ('contain_custom_checks_from').

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
                },
                "contain_vars_from": [ ... list of referenced variable names ... ],
                "contain_custom_checks_from": [ ... list of custom .py check scripts ... ]
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

        # 3. Parse Column Checks (returns SQL expressions, error_suffixes, contain_vars_from, & contain_custom_checks_from)
        column_results = ColumnQualityParser.parse_yaml_checks(quality_config)

        # 4. Parse Table Checks (delegates to TableQualityParser)
        table_results = TableQualityParser.parse_yaml_checks(quality_config)

        # 5. Aggregate and deduplicate variable dependencies across all sub-parsers
        vars_set: Set[str] = set()
        for res in (schema_results, column_results, table_results):
            sub_vars = res.get("contain_vars_from", [])
            if isinstance(sub_vars, list):
                vars_set.update(sub_vars)

        # 6. Aggregate and deduplicate custom check python dependencies
        custom_checks_set: Set[str] = set()
        for res in (column_results, table_results):
            sub_custom = res.get("contain_custom_checks_from", [])
            if isinstance(sub_custom, list):
                custom_checks_set.update(sub_custom)

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
            }),
            "contain_vars_from": sorted(list(vars_set)),
            "contain_custom_checks_from": sorted(list(custom_checks_set))
        }