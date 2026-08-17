
from typing import Any, Dict, List, Union


class TemplateResolver:

    @classmethod
    def resolve_placeholders(cls, data: Any, col_name: str) -> Any:
        """
        Recursively resolves placeholders like ${col} or ${column} 
        with the actual column name across strings, dicts, and lists.
        """
        if isinstance(data, str):
            return data.replace("${col}", col_name).replace("${column}", col_name)

        elif isinstance(data, dict):
            resolved_dict: Dict[str, Any] = {}
            for k, v in data.items():
                clean_key = cls.resolve_placeholders(k, col_name)
                resolved_dict[clean_key] = cls.resolve_placeholders(v, col_name)
            return resolved_dict

        elif isinstance(data, list):
            return [cls.resolve_placeholders(item, col_name) for item in data]

        return data