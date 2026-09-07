
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
        Loads a quality or transformation template by name, resolves all 'with' 
        variables, replaces set_var definitions, expands for_each column loops, 
        and returns clean, unrolled rule dictionaries ready for the parser.
        """
        try:
            # 1. Fetch raw template content
            raw_template_data = Getter.get_templates(template_name)
            if not raw_template_data:
                raise ValueError(f"Template '{template_name}' was empty or not found.")

            # 2. Extract rules list from supported root keys
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
                raise ValueError(
                    f"Invalid template format for '{template_name}'. "
                    f"Expected a list under 'columns_checks', 'rules', or 'template'."
                )

            # 3. PRE-RESOLVE ${var...} references inside with_vars into actual lists/values
            resolved_with_vars: Dict[str, Any] = {}
            for k, v in with_vars.items():
                if isinstance(v, str) and VariablesManager.is_var(v):
                    resolved_with_vars[k] = VariablesManager.resolve_var(v)
                else:
                    resolved_with_vars[k] = v

            expanded_rules: List[Dict[str, Any]] = []

            # 4. Process each rule inside the template
            for rule_item in template_items:
                if not isinstance(rule_item, dict):
                    continue

                rule_copy = copy.deepcopy(rule_item)

                # Recursively replace all set_var maps and variable string placeholders
                resolved_rule = cls._resolve_all_set_vars_and_vars(rule_copy, resolved_with_vars)

                # Skip rule if a required set_var parameter was omitted in 'with'
                if cls._has_unresolved_set_var(resolved_rule):
                    logger.debug(
                        f"Skipping rule in template '{template_name}' because a required set_var target was not passed in 'with'."
                    )
                    continue

                # Expand for_each / columns loops into explicit per-column check dictionaries
                if "for_each" in resolved_rule or "columns" in resolved_rule:
                    expanded_items = cls._expand_template_loop(resolved_rule, resolved_with_vars)
                    expanded_rules.extend(expanded_items)
                else:
                    expanded_rules.append(resolved_rule)

            return expanded_rules

        except Exception as e:
            logger.error(f"[TemplateManager Error] Failed to inject template '{template_name}': {str(e)}")
            raise e

    @classmethod
    def _resolve_all_set_vars_and_vars(cls, obj: Any, with_vars: Dict[str, Any]) -> Any:
        """
        Recursively replaces set_var dictionaries and variable strings:
        - `{"set_var": "my_key"}` -> replaces with `with_vars["my_key"]`
        - `"${my_key}"` -> replaces with `with_vars["my_key"]`
        - `"${var.path.val}"` -> resolves via `VariablesManager.resolve_var`
        """
        if isinstance(obj, dict):
            # Resolve standalone set_var dicts: {"set_var": "var_name"}
            if len(obj) == 1 and "set_var" in obj:
                var_key = obj["set_var"]
                if var_key in with_vars:
                    val = with_vars[var_key]
                    if isinstance(val, str) and VariablesManager.is_var(val):
                        val = VariablesManager.resolve_var(val)
                    return cls._resolve_all_set_vars_and_vars(val, with_vars)
                return obj

            return {
                k: cls._resolve_all_set_vars_and_vars(v, with_vars)
                for k, v in obj.items()
            }

        elif isinstance(obj, list):
            return [cls._resolve_all_set_vars_and_vars(item, with_vars) for item in obj]

        elif isinstance(obj, str):
            # Direct variable reference like ${var.cust.on_null_error}
            if VariablesManager.is_var(obj):
                return VariablesManager.resolve_var(obj)

            # In-string template substitution like "column_${col_name}"
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
        """
        Checks if an unfulfilled `set_var` dict remains in the payload.
        """
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
        """
        Expands for_each / columns targets into concrete per-column dictionaries.
        """
        results: List[Dict[str, Any]] = []
        raw_targets = check_entry.get("for_each") or check_entry.get("columns")

        # Extract target if raw_targets is set_var dict
        if isinstance(raw_targets, dict) and "set_var" in raw_targets:
            var_key = raw_targets["set_var"]
            raw_targets = with_vars.get(var_key)

        # Extract target if raw_targets is a string variable
        if isinstance(raw_targets, str):
            if VariablesManager.is_var(raw_targets):
                raw_targets = VariablesManager.resolve_var(raw_targets)
            elif raw_targets in with_vars:
                raw_targets = with_vars[raw_targets]

        if not isinstance(raw_targets, list):
            return [check_entry]

        # Expand each column item and resolve ${col} placeholders
        for target_col in raw_targets:
            if not target_col:
                continue

            loop_item = copy.deepcopy(check_entry)
            loop_item.pop("for_each", None)
            loop_item.pop("columns", None)

            resolved_item = TemplateResolver.resolve_placeholders(loop_item, str(target_col))
            results.append(resolved_item)

        return results