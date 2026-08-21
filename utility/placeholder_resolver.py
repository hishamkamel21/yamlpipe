from typing import Any, Dict, List, Tuple


class TemplateResolver:

    @classmethod
    def resolve_placeholders(cls, data: Any, col_name: str) -> Any:
        """
        Recursively replaces ${col} and ${column} placeholders with col_name inside 
        strings, dictionary values, dictionary keys, and list items.
        """
        if isinstance(data, str):
            return data.replace("${col}", col_name).replace("${column}", col_name)

        elif isinstance(data, dict):
            return {
                cls.resolve_placeholders(k, col_name): cls.resolve_placeholders(v, col_name)
                for k, v in data.items()
            }

        elif isinstance(data, list):
            return [cls.resolve_placeholders(item, col_name) for item in data]

        return data

    @classmethod
    def resolve_and_expand(cls, config: Dict[str, Any]) -> List[Tuple[Dict[str, Any], str]]:
        """
        Generic expansion: Checks for 'for_each' or 'columns' list.
        Duplicates the config dict per column and resolves all placeholders 
        (including 'expr', 'expression', 'when', etc.) generically.
        
        Returns a list of tuples: [(resolved_config_dict, column_name), ...]
        """
        # Accept either 'for_each' or 'columns' key
        target_columns = config.get("for_each") or config.get("columns") or []

        # Fallback if target_columns is a single string instead of a list
        if isinstance(target_columns, str):
            target_columns = [target_columns]

        # Handle case where no for_each/columns iteration list was provided
        if not isinstance(target_columns, list) or not target_columns:
            single_col = config.get("column")
            if single_col and isinstance(single_col, str):
                clean_col = single_col.strip()
                resolved = cls.resolve_placeholders(config, clean_col)
                return [(resolved, clean_col)]
            return [(config, "")]

        expanded = []
        # Exclude iteration keys from payload to keep resolved payload clean
        payload_keys = [k for k in config.keys() if k not in ("columns", "for_each")]

        for col in target_columns:
            if isinstance(col, str) and col.strip():
                clean_col = col.strip()
                payload = {k: config[k] for k in payload_keys}
                
                # Automatically inject the target column into the payload 
                # so registry checks know which column is being evaluated
                if "column" not in payload:
                    payload["column"] = clean_col

                resolved_payload = cls.resolve_placeholders(payload, clean_col)
                expanded.append((resolved_payload, clean_col))

        return expanded