# yamlpipe/core/template_manager.py

import copy
import logging
from typing import Any, Dict, List
from yamlpipe.getter import Getter
from yamlpipe.core.vars_manager import VariablesManager
from yamlpipe.utility.placeholder_resolver import TemplateResolver

logger = logging.getLogger("TemplateManager")


class TemplateManager:

    @classmethod
    def inject_handler(
        cls, template_name: str, with_vars: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Loads a transformation or quality template by name, replaces all set_var
        and variable placeholders using the 'with' dictionary, expands loops (for_each),
        and returns the flattened list of rule dictionaries.
        """
        try:
            raw_template_data = Getter.get_templates(template_name)
            if not raw_template_data:
                raise ValueError(f"Template '{template_name}' was empty or not found.")

            # Extract list from 'rules', 'columns_checks', 'template', or root list
            template_items = None
            if isinstance(raw_template_data, dict):
                template_items = (
                    raw_template_data.get("rules")
                    or raw_template_data.get("columns_checks")
                    or raw_template_data.get("template")
                    or raw_template_data.get("checks")
                )
            elif isinstance(raw_template_data, list):
                template_items = raw_template_data

            if not isinstance(template_items, list):
                raise ValueError(
                    f"Invalid template format for '{template_name}'. "
                    f"Expected a list under 'rules', 'columns_checks', or 'template'."
                )

            expanded_rules: List[Dict[str, Any]] = []

            for rule_item in template_items:
                if not isinstance(rule_item, dict):
                    continue

                rule_copy = copy.deepcopy(rule_item)

                # Recursively resolve all set_var mappings and ${var} values
                resolved_rule = cls._resolve_all_set_vars_and_vars(rule_copy, with_vars)

                # Skip rule if a required set_var target was omitted in with_vars
                if cls._has_unresolved_set_var(resolved_rule):
                    logger.debug(
                        f"Skipping rule in template '{template_name}' because a required set_var target was not passed in 'with'."
                    )
                    continue

                # Expand loops (for_each / columns) into concrete rule instances
                if "for_each" in resolved_rule or "columns" in resolved_rule:
                    expanded_items = cls._expand_template_loop(resolved_rule, with_vars)
                    expanded_rules.extend(expanded_items)
                else:
                    expanded_rules.append(resolved_rule)

            return expanded_rules

        except Exception as e:
            logger.error(f"[TemplateManager Error] Failed to inject template '{template_name}': {str(e)}")
            raise e

    @classmethod
    def _resolve_all_set_vars_and_vars(cls, obj: Any, with_vars: Dict[str, Any]) -> Any:
        if isinstance(obj, dict):
            if len(obj) == 1 and "set_var" in obj:
                var_key = obj["set_var"]
                if var_key in with_vars:
                    return cls._resolve_all_set_vars_and_vars(with_vars[var_key], with_vars)
                return obj

            return {
                k: cls._resolve_all_set_vars_and_vars(v, with_vars)
                for k, v in obj.items()
            }

        elif isinstance(obj, list):
            return [cls._resolve_all_set_vars_and_vars(item, with_vars) for item in obj]

        elif isinstance(obj, str):
            for key, val in with_vars.items():
                target_placeholder = f"${{{key}}}"
                if obj == target_placeholder:
                    return val
                elif target_placeholder in obj and isinstance(val, str):
                    obj = obj.replace(target_placeholder, val)
            return obj

        return obj

    @classmethod
    def _has_unresolved_set_var(cls, obj: Any) -> bool:
        if isinstance(obj, dict):
            if "set_var" in obj and len(obj) == 1:
                return True
            return any(cls._has_unresolved_set_var(v) for v in obj.values())
        elif isinstance(obj, list):
            return any(cls._has_unresolved_set_var(item) for item in obj)
        return False

    @classmethod
    def _expand_template_loop(
        cls, check_entry: Dict[str, Any], with_vars: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        raw_targets = check_entry.get("for_each") or check_entry.get("columns")

        if isinstance(raw_targets, str):
            if VariablesManager.is_var(raw_targets):
                raw_targets = VariablesManager.resolve_var(raw_targets)
            elif raw_targets in with_vars:
                raw_targets = with_vars[raw_targets]

        if not isinstance(raw_targets, list):
            return [check_entry]

        for target_col in raw_targets:
            # Skip empty column placeholders if empty lists are passed in with:
            if not target_col:
                continue

            loop_item = copy.deepcopy(check_entry)
            loop_item.pop("for_each", None)
            loop_item.pop("columns", None)

            resolved_item = TemplateResolver.resolve_placeholders(loop_item, str(target_col))
            results.append(resolved_item)

        return results