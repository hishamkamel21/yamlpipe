import copy
import logging
from typing import Any, Dict, List, Union
from yamlpipe.core.getter import Getter
from yamlpipe.core.vars_manager import VariablesManager
from yamlpipe.utility.placeholder_resolver import TemplateResolver

logger = logging.getLogger("TemplateManager")


class TemplateManager:

    @classmethod
    def inject_handler(
        cls, template_name: str, with_vars: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Loads a template by name via Getter, substitutes 'set_var' targets using

        the 'with' block payload, and returns expanded concrete check definitions.
        """
        try:
            # 1. Load template structure from cache / templates directory
            raw_template_data = Getter.get_templates(template_name)
            if not raw_template_data:
                raise ValueError(f"Template '{template_name}' was empty or not found.")

            # Extract list from 'columns_checks', 'template', or root list
            template_checks = (
                raw_template_data.get("columns_checks")
                or raw_template_data.get("template")
                or raw_template_data.get("checks")
            )

            if template_checks is None and isinstance(raw_template_data, list):
                template_checks = raw_template_data

            if not isinstance(template_checks, list):
                raise ValueError(
                    f"Invalid template format for '{template_name}'. "
                    f"Expected a list under 'columns_checks' or 'template'."
                )

            expanded_checks: List[Dict[str, Any]] = []

            # 2. Iterate through checks defined inside the template
            for check_item in template_checks:
                if not isinstance(check_item, dict):
                    continue

                check_copy = copy.deepcopy(check_item)

                # Check if this check uses `for_each: { set_var: <var_name> }`
                for_each_cfg = check_copy.get("for_each")
                
                target_var_name = None
                if isinstance(for_each_cfg, dict):
                    target_var_name = for_each_cfg.get("set_var")
                elif isinstance(for_each_cfg, str):
                    target_var_name = for_each_cfg

                # 3. Match `set_var` with supplied `with_vars` payload
                if target_var_name and target_var_name in with_vars:
                    provided_cols = with_vars[target_var_name]

                    # Replace 'for_each' dict with concrete list of columns
                    check_copy["for_each"] = provided_cols

                # If set_var wasn't provided in 'with', skip this check
                elif target_var_name and target_var_name not in with_vars:
                    logger.debug(
                        f"Skipping check in template '{template_name}' because '{target_var_name}' "
                        f"was not passed in 'with'."
                    )
                    continue

                # 4. Substitute standard placeholders like ${var_name}
                resolved_check = cls._substitute_with_context(check_copy, with_vars)

                # 5. Expand for_each / columns loops into final check items
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
    def _substitute_with_context(cls, obj: Any, with_vars: Dict[str, Any]) -> Any:
        """Recursively replaces placeholder keys like `${with_var_name}` or `${var_name}`

        using values passed in the `with:` payload.
        """
        if isinstance(obj, str):
            for key, val in with_vars.items():
                target_placeholder = f"${{{key}}}"
                if obj == target_placeholder:
                    return val
                elif target_placeholder in obj and isinstance(val, str):
                    obj = obj.replace(target_placeholder, val)
            return obj

        elif isinstance(obj, dict):
            return {
                k: cls._substitute_with_context(v, with_vars)
                for k, v in obj.items()
            }

        elif isinstance(obj, list):
            return [cls._substitute_with_context(item, with_vars) for item in obj]

        return obj

    @classmethod
    def _expand_template_loop(
        cls, check_entry: Dict[str, Any], with_vars: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Expands check entries that iterate over column lists into individual checks."""
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
            loop_item = copy.deepcopy(check_entry)
            loop_item.pop("for_each", None)
            loop_item.pop("columns", None)

            resolved_item = TemplateResolver.resolve_placeholders(loop_item, str(target_col))
            results.append(resolved_item)

        return results