import logging
from typing import Dict, Any, List

logger = logging.getLogger("SchemaQualityParser")


class SchemaQualityParser:

    @classmethod
    def parse_yaml_checks(cls, yaml_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parses the 'schema_checks' section from the YAML config.
        Passes the YAML objects directly as-is without modification.

        Returns:
            dict: {
                "schema_checks": [
                    {
                        "check_type": "required_missing",
                        "columns": ["transaction_id", ...]
                    },
                    ...
                ]
            }
        """
        schema_checks_config = yaml_config.get("schema_checks", [])

        if not schema_checks_config:
            return {"schema_checks": []}

        # Return the exact list of schema check objects directly from YAML
        return {
            "schema_checks": schema_checks_config
        }