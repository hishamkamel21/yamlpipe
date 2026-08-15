import re
import logging
from typing import Any, Dict

logger = logging.getLogger("VariablesManager")


class VariablesManager:
    """
    Parses and replaces variable references inside YAML configurations.
    Syntax supported: ${var.<var_file>.<var_key>}
    """
    VAR_PATTERN = re.compile(r"\$\{var\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_\.-]+)\}")

    # Keys that are never allowed to contain variable placeholders
    RESTRICTED_KEYS = {"column", "check_type", "type"}

    @classmethod
    def is_var(cls, value: Any) -> bool:
        """Checks if a string contains variable syntax."""
        if isinstance(value, str):
            return bool(cls.VAR_PATTERN.search(value))
        return False

    @classmethod
    def parse_value(cls, value: Any, project_root: str, current_key: str = None) -> Any:
        """
        Recursively inspects structures and replaces placeholders except for 
        restricted context keys.
        """
        if isinstance(value, str):
            # Block variable resolution if assigned directly under a restricted key
            if current_key in cls.RESTRICTED_KEYS:
                if cls.is_var(value):
                    raise ValueError(
                        f"[VariablesManager Error] Key '{current_key}' cannot use variable placeholders: '{value}'"
                    )
                return value
            return cls._resolve_string_var(value, project_root)

        elif isinstance(value, dict):
            parsed_dict = {}
            for k, v in value.items():
                # Enforce rule: severity inside column-first format cannot be a variable
                if k == "severity" and "column" in value:
                    if isinstance(v, str) and cls.is_var(v):
                        raise ValueError(
                            f"[VariablesManager Error] 'severity' under column '{value.get('column')}' cannot be a variable: '{v}'"
                        )
                parsed_dict[k] = cls.parse_value(v, project_root, current_key=k)
            return parsed_dict

        elif isinstance(value, list):
            return [cls.parse_value(item, project_root, current_key=current_key) for item in value]

        return value

    @classmethod
    def _resolve_string_var(cls, text: str, project_root: str) -> Any:
        matches = list(cls.VAR_PATTERN.finditer(text))
        if not matches:
            return text

        if len(matches) == 1 and matches[0].group(0) == text.strip():
            var_file, var_key = matches[0].groups()
            return cls.get_variable(project_root, var_file, var_key)

        resolved_text = text
        for match in matches:
            full_placeholder = match.group(0)
            var_file, var_key = match.groups()
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