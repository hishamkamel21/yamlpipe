import copy
import logging
from typing import Any, Dict, List
from yamlpipe.getter import Getter
from yamlpipe.core.vars_manager import VariablesManager

logger = logging.getLogger("TemplateManager")


class TemplateManager:

    @classmethod
    def inject_handler(
        cls, template_name: str, with_vars: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        try:
            raw_template_data = Getter.get_templates(template_name)
            if not raw_template_data:
                raise ValueError(f"Template '{template_name}' was empty or not found.")

            template_items = None
            if isinstance(raw_template_data, dict):
                template_items = (
                    raw_template_data.get("columns_checks")
                    or raw_template_data.get("rules")
                    or raw_template_data.get("template")
                    or raw_template_data.get("checks")
                )
            elif isinstance(raw_template_data, list):
                template_items = raw_template_data

            if not isinstance(template_items, list):
                raise ValueError(f"Invalid template format for '{template_name}'.")

            # 1. Pre-resolve any variables inside with_vars dict
            resolved_with_vars = {}
            for k, v in with_vars.items():
                if isinstance(v, str) and VariablesManager.is_var(v):
                    resolved_with_vars[k] = VariablesManager.resolve_var(v)
                else:
                    resolved_with_vars[k] = v

            # 2. Replace set_var with actual values and return items to parser
            processed_rules: List[Dict[str, Any]] = []
            for rule_item in template_items:
                if not isinstance(rule_item, dict):
                    continue

                rule_copy = copy.deepcopy(rule_item)
                resolved_rule = cls._replace_set_var_with_values(rule_copy, resolved_with_vars)
                processed_rules.append(resolved_rule)

            return processed_rules

        except Exception as e:
            logger.error(f"[TemplateManager Error] Failed to inject template '{template_name}': {str(e)}")
            raise e

    @classmethod
    def _replace_set_var_with_values(cls, obj: Any, with_vars: Dict[str, Any]) -> Any:
        """Replaces {set_var: var_name} with the actual value from with_vars."""
        if isinstance(obj, dict):
            if len(obj) == 1 and "set_var" in obj:
                var_key = obj["set_var"]
                if var_key in with_vars:
                    val = with_vars[var_key]
                    if isinstance(val, str) and VariablesManager.is_var(val):
                        val = VariablesManager.resolve_var(val)
                    return val
                return obj

            return {
                k: cls._replace_set_var_with_values(v, with_vars)
                for k, v in obj.items()
            }

        elif isinstance(obj, list):
            return [cls._replace_set_var_with_values(item, with_vars) for item in obj]

        elif isinstance(obj, str):
            if VariablesManager.is_var(obj):
                return VariablesManager.resolve_var(obj)

            for key, val in with_vars.items():
                target_placeholder = f"${{{key}}}"
                if obj == target_placeholder:
                    return val
                elif target_placeholder in obj and isinstance(val, str):
                    obj = obj.replace(target_placeholder, val)
            return obj

        return obj