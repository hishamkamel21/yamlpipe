import logging
from typing import Dict, Any, List
from yamlpipe.registry.transformation_registry import TransformationRegistry

logger = logging.getLogger("TransformationParser")


class TransformationParser:

    @classmethod
    def parse(cls, raw_config: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(raw_config, dict):
            raise TypeError(f"[TransformationParser Error] Expected dict configuration, got {type(raw_config).__name__}")

        # Clean non-breaking spaces (\xa0) and key whitespace
        clean_config = cls._sanitize_dict(raw_config)

        raw_table = clean_config.get("table", "unknown_table")
        main_alias = clean_config.get("alias", "c")

        # 1. Parse Joins Configuration
        raw_joins = clean_config.get("joins", [])
        parsed_joins: List[Dict[str, Any]] = []
        if isinstance(raw_joins, list):
            for join_item in raw_joins:
                if isinstance(join_item, dict):
                    parsed_joins.append(cls._parse_join(cls._sanitize_dict(join_item)))

        # 2. Parse Rules via TransformationRegistry
        raw_rules = clean_config.get("rules", [])
        parsed_rules: List[Dict[str, Any]] = []

        if isinstance(raw_rules, list):
            for rule_item in raw_rules:
                if isinstance(rule_item, dict):
                    sanitized_rule = cls._sanitize_dict(rule_item)
                    expanded = TransformationRegistry.process_rule(sanitized_rule)
                    parsed_rules.extend(expanded)

        return {
            "table": raw_table,
            "alias": main_alias,
            "joins": parsed_joins,
            "rules": parsed_rules,
            "ContainVarsFrom": clean_config.get("ContainVarsFrom", [])
        }

    @classmethod
    def _sanitize_dict(cls, d: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively removes non-breaking spaces (\xa0) and whitespace from keys."""
        sanitized = {}
        for k, v in d.items():
            clean_k = str(k).replace('\xa0', '').strip()
            if isinstance(v, dict):
                sanitized[clean_k] = cls._sanitize_dict(v)
            elif isinstance(v, list):
                sanitized[clean_k] = [cls._sanitize_dict(i) if isinstance(i, dict) else i for i in v]
            else:
                sanitized[clean_k] = v
        return sanitized

    @classmethod
    def _parse_join(cls, join_cfg: Dict[str, Any]) -> Dict[str, Any]:
        target_table = join_cfg.get("table", "")
        tbl_alias = join_cfg.get("alias") or target_table
        how = join_cfg.get("how", join_cfg.get("type", "left")).lower()
        broadcast = bool(join_cfg.get("broadcast", False))
        on_clause = join_cfg.get("on_clause") or join_cfg.get("on", "")
        sql_clause = join_cfg.get("sql") or f"{how.upper()} JOIN {target_table} AS `{tbl_alias}` ON {on_clause}"

        return {
            "table": target_table,
            "alias": tbl_alias,
            "how": how,
            "broadcast": broadcast,
            "on_clause": on_clause,
            "sql": sql_clause
        }