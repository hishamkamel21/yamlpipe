from typing import Dict, Any
from yamlpipe.core.vars_manager import VariablesManager



class SchemaQualityParser:

    @classmethod
    def parse_yaml_checks(cls, yaml_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parses the 'schema_checks' section from the YAML config.
        """
        schema_checks_config = yaml_config.get("schema_checks", [])

        if not schema_checks_config:
            return {"schema_checks": []}

        for check in schema_checks_config:
            check_type = check.get("check_type") or check.get("type")
            if VariablesManager.is_var(check_type):
                raise ValueError(f"Schema check type cannot be a variable placeholder: '{check_type}'")

        return {
            "schema_checks": schema_checks_config
        }