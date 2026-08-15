import re
import logging
from typing import Any, Dict, Set, Tuple

logger = logging.getLogger("VariablesManager")


class VariablesManager:
    """
    Parses and replaces variable references inside YAML configurations.
    Tracks referenced variable namespaces to build dynamic cache dependency graphs.
    """
    VAR_PATTERN = re.compile(r"\$\{var\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_\.-]+)\}")
    RESTRICTED_KEYS = {"column", "check_type", "type"}

    @classmethod
    def is_var(cls, value: Any) -> bool:
        if isinstance(value, str):
            return bool(cls.VAR_PATTERN.search(value))
        return False

    @classmethod
    def extract_vars_and_parse(cls, raw_config: Dict[str, Any], project_root: str) -> Tuple[Dict[str, Any], Set[str]]:
        """
        Main entry point: Resolves variables inside raw YAML and tracks all dependent variable namespaces.
        """
        referenced_vars: Set[str] = set()
        resolved_config = cls._parse_recursive(raw_config, project_root, referenced_vars)
        
        if isinstance(resolved_config, dict):
            resolved_config["ContainVarsFrom"] = sorted(list(referenced_vars))

        return resolved_config, referenced_vars

    @classmethod
    def _parse_recursive(cls, value: Any, project_root: str, var_set: Set[str], current_key: str = None) -> Any:
        if isinstance(value, str):
            if current_key in cls.RESTRICTED_KEYS:
                if cls.is_var(value):
                    raise ValueError(
                        f"[VariablesManager Error] Key '{current_key}' cannot use variable placeholders: '{value}'"
                    )
                return value
            return cls._resolve_string_var(value, project_root, var_set)

        elif isinstance(value, dict):
            parsed_dict = {}
            for k, v in value.items():
                if k == "severity" and "column" in value:
                    if isinstance(v, str) and cls.is_var(v):
                        raise ValueError(
                            f"[VariablesManager Error] 'severity' under column '{value.get('column')}' cannot be a variable: '{v}'"
                        )
                parsed_dict[k] = cls._parse_recursive(v, project_root, var_set, current_key=k)
            return parsed_dict

        elif isinstance(value, list):
            return [cls._parse_recursive(item, project_root, var_set, current_key=current_key) for item in value]

        return value

    @classmethod
    def _resolve_string_var(cls, text: str, project_root: str, var_set: Set[str]) -> Any:
        matches = list(cls.VAR_PATTERN.finditer(text))
        if not matches:
            return text

        if len(matches) == 1 and matches[0].group(0) == text.strip():
            var_file, var_key = matches[0].groups()
            var_set.add(var_file)
            return cls.get_variable(project_root, var_file, var_key)

        resolved_text = text
        for match in matches:
            full_placeholder = match.group(0)
            var_file, var_key = match.groups()
            var_set.add(var_file)
            resolved_val = cls.get_variable(project_root, var_file, var_key)
            resolved_text = resolved_text.replace(full_placeholder, str(resolved_val))

        return resolved_text

    @classmethod
    def get_variable(cls, project_root: str, var_file: str, var_key: str) -> Any:
        from yamlpipe.core.cache_manager import CacheManager
        var_data = CacheManager.get_or_compile(
            project_root=project_root,
            subfolder="vars",
            selector=var_file
        )

        # Strip root 'ContainVarsFrom' metadata if fetching directly
        if isinstance(var_data, dict) and "ContainVarsFrom" in var_data:
            var_data = {k: v for k, v in var_data.items() if k != "ContainVarsFrom"}

        keys = var_key.split(".")
        current_val = var_data
        for k in keys:
            if isinstance(current_val, dict) and k in current_val:
                current_val = current_val[k]
            else:
                raise KeyError(
                    f"[VariablesManager Error] Key '{var_key}' not found in variable file '{var_file}.yml'."
                )
        return current_val