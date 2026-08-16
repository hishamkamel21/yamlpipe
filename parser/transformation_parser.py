import logging
from typing import Dict, Any, List

from yamlpipe.utility.helper import Helper

logger = logging.getLogger("TransformationParser")


class TransformationParser:

    @classmethod
    def parse(cls, raw_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parses flat transformation YAML configuration into the structured contract 
        expected by TransformationManager.
        """
        if not isinstance(raw_config, dict):
            raise TypeError(f"[TransformationParser Error] Expected dict configuration, got {type(raw_config).__name__}")

        raw_table = raw_config.get("table", "unknown_table")
        main_alias = raw_config.get("alias", "c")

        # 1. Parse Joins Configuration
        raw_joins = raw_config.get("joins", [])
        parsed_joins: List[Dict[str, Any]] = []
        if isinstance(raw_joins, list):
            for join_item in raw_joins:
                if isinstance(join_item, dict):
                    parsed_joins.append(cls._parse_join(join_item))

        # 2. Process Rules Array
        raw_rules = raw_config.get("rules", [])
        parsed_rules: List[Dict[str, Any]] = []
        if isinstance(raw_rules, list):
            for rule_item in raw_rules:
                if isinstance(rule_item, dict):
                    parsed_rules.append(cls._clean_rule(rule_item))

        parsed_config = {
            "table": raw_table,
            "alias": main_alias,
            "joins": parsed_joins,
            "rules": parsed_rules
        }

        logger.debug(f"Successfully parsed transformation configuration for table '{raw_table}'")
        return parsed_config

    @classmethod
    def _parse_join(cls, join_cfg: Dict[str, Any]) -> Dict[str, Any]:
        """Normalizes join metadata keys for PySpark join processing."""
        target_table = join_cfg.get("table", "")
        tbl_alias = join_cfg.get("alias") or target_table
        how = join_cfg.get("how", join_cfg.get("type", "left")).lower()
        broadcast = bool(join_cfg.get("broadcast", False))
        on_clause = join_cfg.get("on_clause") or join_cfg.get("on", "")

        # Optional SQL fallback construction
        sql_clause = f"{how.upper()} JOIN {target_table}"
        if tbl_alias:
            sql_clause += f" AS `{tbl_alias}`"
        if on_clause:
            sql_clause += f" ON {on_clause}"

        return {
            "table": target_table,
            "alias": tbl_alias,
            "how": how,
            "broadcast": broadcast,
            "on_clause": on_clause,
            "sql": sql_clause
        }

    @classmethod
    def _clean_rule(cls, rule_cfg: Dict[str, Any]) -> Dict[str, Any]:
        """Trims whitespace from string expressions inside the rule definitions."""
        cleaned = rule_cfg.copy()
        if "expression" in cleaned and isinstance(cleaned["expression"], str):
            cleaned["expression"] = cleaned["expression"].strip()
        if "run" in cleaned and isinstance(cleaned["run"], str):
            cleaned["run"] = cleaned["run"].strip()
        return cleaned