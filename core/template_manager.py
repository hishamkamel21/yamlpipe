import copy
import logging
from typing import Any, Dict, List, Union
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
        Loads a template by name via Getter, recursively replaces ALL occurrences 
        of `set_var` and placeholders using the `with:` block dictionary, expands 
        loops (`for_each` / `columns`), and returns concrete check objects to the parser.
        """
        try:
            # 1. Fetch raw template data from storage/cache
            raw_template_data = Getter.get_templates(template_name)
            if not raw_template_data:
                raise ValueError(f"Template '{template_name}' was empty or not found.")

            # 2. Extract checks list regardless of root key naming (`columns_checks`, `template`, etc.)
            template_checks = None
            if isinstance(raw_template_data, dict):
                template_checks = (
                    raw_template_data.get("columns_checks")
                    or raw_template_data.get("template")
                    or raw_template_data.get("checks")
                )
            elif isinstance(raw_template_data, list):
                template_checks = raw_template_data

            if not isinstance(template_checks, list):
                raise ValueError(
                    f"Invalid template format for '{template_name}'. "
                    f"Expected a list under 'columns_checks' or 'template'."
                )

            expanded_checks: List[Dict[str, Any]] = []

            # 3. Iterate over checks inside the template
            for check_item in template_checks:
                if not isinstance(check_item, dict):
                    continue

                check_copy = copy.deepcopy(check_item)

                # Recursively replace ALL `set_var` blocks and placeholders across the whole check
                resolved_check = cls._resolve_all_set_vars_and_vars(check_copy, with_vars)

                # Skip checks that contained an unfulfilled set_var target
                if cls._has_unresolved_set_var(resolved_check):
                    logger.debug(
                        f"Skipping check in template '{template_name}' because a required set_var target was not provided in 'with'."
                    )
                    continue

                # 4. Expand loops (for_each / columns) into concrete checks
                if "for_each" in resolved_check or "columns" in resolved_check:
                    expanded_items = cls._expand_template_loop(resolved_check, with_vars)
                    expanded_checks.extend(expanded_items)
                else:
                    expanded_checks.append(resolved_check)

            return expanded_checks

        except Exception as e:
            logger.error(f"[TemplateManager Error] Failed to inject template '{template_name}': {str(e)}")
            raise e

    @classmethod
    def _resolve_all_set_vars_and_vars(cls, obj: Any, with_vars: Dict[str, Any]) -> Any:
        """
        Recursively replaces:
        1. Dicts like `{"set_var": "var_name"}` with the value of `with_vars["var_name"]`
        2. Placeholder strings like `${var_name}` with the value from `with_vars`
        """
        if isinstance(obj, dict):
            # If the dict is directly `{"set_var": "some_key"}`
            if len(obj) == 1 and "set_var" in obj:
                var_key = obj["set_var"]
                if var_key in with_vars:
                    return cls._resolve_all_set_vars_and_vars(with_vars[var_key], with_vars)
                return obj  # Return as-is if not found (will be caught by unresolved checker)

            # Otherwise recurse through keys and values
            return {
                k: cls._resolve_all_set_vars_and_vars(v, with_vars)
                for k, v in obj.items()
            }

        elif isinstance(obj, list):
            return [cls._resolve_all_set_vars_and_vars(item, with_vars) for item in obj]

        elif isinstance(obj, str):
            # Handle variable placeholders like `${var_name}`
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
        Checks if any `set_var` dictionary remains unreplaced inside the check payload.
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
        Expands template checks that iterate over column lists into individual checks with 'column'.
        """
        results: List[Dict[str, Any]] = []
        raw_targets = check_entry.get("for_each") or check_entry.get("columns")

        # Resolve list if specified via variable reference
        if isinstance(raw_targets, str):
            if VariablesManager.is_var(raw_targets):
                raw_targets = VariablesManager.resolve_var(raw_targets)
            elif raw_targets in with_vars:
                raw_targets = with_vars[raw_targets]

        if not isinstance(raw_targets, list):
            return [check_entry]

        for target_col in raw_targets:
            loop_item = copy.deepcopy(check_entry)
            loop_item.pop("for_each", None)
            loop_item.pop("columns", None)
            loop_item["column"] = str(target_col)

            resolved_item = TemplateResolver.resolve_placeholders(loop_item, str(target_col))
            results.append(resolved_item)

        return results