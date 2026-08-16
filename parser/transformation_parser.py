from typing import Dict, Any, List, Set, Optional
from yamlpipe.registry.transformation_registry import TransformationRegistry


class TransformationParser:

    @classmethod
    def parse(
        cls,
        raw_config: Dict[str, Any],
        schemas: Optional[Dict[str, List[str]]] = None,
    ) -> Dict[str, Any]:
        if not isinstance(raw_config, dict):
            raise TypeError(
                f"[TransformationParser Error] Expected dict configuration, got {type(raw_config).__name__}"
            )

        clean_config = cls._sanitize_dict(raw_config)

        raw_table = clean_config.get("table", "unknown_table")
        main_alias = clean_config.get("alias", "c")

        raw_joins = clean_config.get("joins", [])
        parsed_joins: List[Dict[str, Any]] = []
        if isinstance(raw_joins, list):
            for join_item in raw_joins:
                if isinstance(join_item, dict):
                    parsed_joins.append(
                        cls._parse_join(cls._sanitize_dict(join_item))
                    )

        raw_rules = clean_config.get("rules", [])
        parsed_rules: List[Dict[str, Any]] = []

        if isinstance(raw_rules, list):
            for rule_item in raw_rules:
                if isinstance(rule_item, dict):
                    sanitized_rule = cls._sanitize_dict(rule_item)
                    expanded = TransformationRegistry.process_rule(sanitized_rule)
                    parsed_rules.extend(expanded)

        contained_functions = sorted(
            list(cls._extract_function_names(clean_config))
        )

        output = {
            "table": raw_table,
            "alias": main_alias,
            "joins": parsed_joins,
            "rules": parsed_rules,
            "ContainVarsFrom": clean_config.get("ContainVarsFrom", []),
            "ContainFunctionsFrom": contained_functions,
        }

        if schemas is not None:
            output["schemas"] = schemas

        return output

    @classmethod
    def _extract_function_names(cls, data: Any) -> Set[str]:
        found_funcs: Set[str] = set()
        if isinstance(data, dict):
            for k, v in data.items():
                if k == "call_function" and isinstance(v, str):
                    found_funcs.add(v.strip())
                else:
                    found_funcs.update(cls._extract_function_names(v))
        elif isinstance(data, list):
            for item in data:
                found_funcs.update(cls._extract_function_names(item))
        return found_funcs

    @classmethod
    def _sanitize_dict(cls, d: Dict[str, Any]) -> Dict[str, Any]:
        sanitized = {}
        for k, v in d.items():
            clean_k = str(k).replace("\xa0", "").strip()
            if isinstance(v, dict):
                sanitized[clean_k] = cls._sanitize_dict(v)
            elif isinstance(v, list):
                sanitized[clean_k] = [
                    cls._sanitize_dict(i) if isinstance(i, dict) else (i.replace("\xa0", "").strip() if isinstance(i, str) else i)
                    for i in v
                ]
            elif isinstance(v, str):
                sanitized[clean_k] = v.replace("\xa0", "").strip()
            else:
                sanitized[clean_k] = v
        return sanitized

    @classmethod
    def _parse_join(cls, join_cfg: Dict[str, Any]) -> Dict[str, Any]:
        target_table = join_cfg.get("table", "")
        tbl_alias = join_cfg.get("alias") or target_table
        how = str(join_cfg.get("how", join_cfg.get("type", "left"))).lower()
        broadcast = bool(join_cfg.get("broadcast", False))
        on_clause = join_cfg.get("on_clause") or join_cfg.get("on", "")
        sql_clause = (
            join_cfg.get("sql")
            or f"{how.upper()} JOIN {target_table} AS `{tbl_alias}` ON {on_clause}"
        )

        return {
            "table": target_table,
            "alias": tbl_alias,
            "how": how,
            "broadcast": broadcast,
            "on_clause": on_clause,
            "sql": sql_clause,
        }