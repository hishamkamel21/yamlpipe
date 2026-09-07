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
        """
        Loads a template by name via Getter, resolves variables passed in the 'with' block,
        handles inner list expansions (for_each/columns), and returns expanded checks.
        """
        try:
            # 1. Fetch template structure from cache / templates directory
            raw_template_data = Getter.get_templates(template_name)
            if not raw_template_data:
                raise ValueError(f"Template '{template_name}' was empty or not found.")

            template_checks = raw_template_data.get("template", [])
            if not isinstance(template_checks, list):
                if isinstance(template_checks, dict):
                    template_checks = [template_checks]
                else:
                    raise ValueError(f"Invalid template format for '{template_name}'. Expected list or dict.")

            expanded_checks: List[Dict[str, Any]] = []

            # 2. Iterate through checks inside the template body
            for check_item in template_checks:
                if not isinstance(check_item, dict):
                    continue

                # Deep copy to ensure thread-safe / clean mutation
                check_copy = copy.deepcopy(check_item)

                # 3. Apply 'with' variable context to check fields
                resolved_check = cls._substitute_with_context(check_copy, with_vars)

                # 4. Handle loops (for_each / columns) inside template definitions
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
    def _substitute_with_context(
        cls, obj: Any, with_vars: Dict[str, Any]
    ) -> Any:
        """
        Recursively replaces placeholder keys like `${with_var_name}` or `${var_name}`
        using values passed in the `with:` payload.
        """
        if isinstance(obj, str):
            # Check direct equality or substring matches for placeholders
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
        """
        Expands template checks that iterate over lists passed via `with` parameters.
        """
        results: List[Dict[str, Any]] = []

        raw_targets = check_entry.get("for_each") or check_entry.get("columns")

        # Resolve list if it was supplied via variable reference
        if isinstance(raw_targets, str):
            if VariablesManager.is_var(raw_targets):
                raw_targets = VariablesManager.resolve_var(raw_targets)
            elif raw_targets in with_vars:
                raw_targets = with_vars[raw_targets]

        if not isinstance(raw_targets, list):
            return [check_entry]

        # Produce independent check objects for every column in the resolved list
        for target_col in raw_targets:
            loop_item = copy.deepcopy(check_entry)
            # Cleanup iteration keys
            loop_item.pop("for_each", None)
            loop_item.pop("columns", None)

            # Substitute `{column}` placeholders using TemplateResolver
            resolved_item = TemplateResolver.resolve_placeholders(loop_item, str(target_col))
            results.append(resolved_item)

        return results