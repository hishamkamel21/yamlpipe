import logging
from typing import Dict, Any

logger = logging.getLogger("TransformationRegistry")


class TransformationRegistry:

    @classmethod
    def build_join_clause(cls, join_cfg: Dict[str, Any]) -> str:
        """
        Builds a standard SQL JOIN clause without injecting inline hints into the join text.
        """
        if not isinstance(join_cfg, dict):
            raise TypeError(f"[TransformationRegistry Error] Expected dict for join_cfg, got {type(join_cfg).__name__}")

        # Sanitize keys from non-breaking spaces and whitespace
        clean_cfg = {str(k).replace('\xa0', '').strip(): v for k, v in join_cfg.items()}

        table = clean_cfg.get("table")
        if not table:
            raise ValueError("[TransformationRegistry Error] Join clause missing 'table' property.")

        alias = clean_cfg.get("alias", "").strip()
        how = str(clean_cfg.get("how", "left")).upper().strip()

        raw_on = (
            clean_cfg.get("on")
            or clean_cfg.get("on_clause")
            or clean_cfg.get("using")
        )

        if raw_on is None:
            raise ValueError(f"[Transformation Error] Join clause missing 'on' condition for table '{table}'.")

        if isinstance(raw_on, list):
            on_clause = " AND ".join([str(cond).strip() for cond in raw_on])
        else:
            on_clause = str(raw_on).strip()

        alias_clause = f" AS `{alias}`" if alias else ""

        return f"{how} JOIN {table}{alias_clause} ON {on_clause}"

    @classmethod
    def build_rule_expr(cls, rule: Dict[str, Any]) -> str:
        """
        Builds SQL SELECT expressions from rule configurations.
        """
        expr = rule.get("expression") or rule.get("expr")
        col = rule.get("column")

        if not expr:
            if col:
                return f"{col} AS `{col.split('.')[-1]}`"
            return ""

        expr_str = str(expr).strip()

        if " AS " in expr_str.upper():
            return expr_str

        if col:
            alias_name = col.split(".")[-1]
            return f"{expr_str} AS `{alias_name}`"

        return expr_str