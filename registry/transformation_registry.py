import logging
from typing import Dict, Any, List
from yamlpipe.utility.module_loader import ModuleLoader
from yamlpipe.utility.helper import Helper

logger = logging.getLogger("TransformationRegistry")


class TransformationRegistry:

    @classmethod
    def build_rule_expr(cls, rule: Dict[str, Any]) -> str:
        """Translates a single column transformation rule into a SQL select expression."""
        column_name = rule.get("column")
        if not column_name:
            raise ValueError(f"[Transformation Error] Rule missing required 'column' key: {rule}")

        # 1. Direct SQL Expression
        if "expression" in rule:
            clean_expr = Helper.clean_multiline_sql(rule["expression"])
            return f"{clean_expr} AS `{column_name}`"

        # 2. Dynamic Function Call
        if "call_function" in rule:
            func_name = rule["call_function"]
            params = rule.get("params", {})

            try:
                if isinstance(params, dict):
                    raw_expr = ModuleLoader.functions_loader(func_name, **params)
                elif isinstance(params, list):
                    raw_expr = ModuleLoader.functions_loader(func_name, *params)
                else:
                    raw_expr = ModuleLoader.functions_loader(func_name, params)

                clean_expr = Helper.clean_multiline_sql(str(raw_expr))
                return f"{clean_expr} AS `{column_name}`"

            except Exception as e:
                logger.error(f"Failed executing function '{func_name}' for column '{column_name}': {str(e)}")
                raise RuntimeError(f"Transformation function '{func_name}' error: {str(e)}") from e

        raise ValueError(f"Rule for column '{column_name}' must specify either 'expression' or 'call_function'.")

    @classmethod
    def build_join_clause(cls, join_cfg: Dict[str, Any]) -> str:
        """Constructs SQL JOIN clause with catalog support and optional BROADCAST hint."""
        tbl_meta = join_cfg.get("table", {})
        
        # Support both dict and full string table names
        if isinstance(tbl_meta, dict):
            catalog = tbl_meta.get("catalog")
            schema = tbl_meta.get("schema")
            tbl_name = tbl_meta.get("table")
            
            full_table_path = ".".join(filter(None, [catalog, schema, tbl_name]))
        else:
            full_table_path = str(tbl_meta)

        alias = join_cfg.get("alias", "").strip()
        alias_str = f" AS `{alias}`" if alias else ""
        how = join_cfg.get("how", "left").upper()
        # 1. Fetch raw value and check multiple possible keys ('on', 'on_clause', 'using')
        raw_on = join_cfg.get("on") or join_cfg.get("on_clause") or join_cfg.get("using")

        if raw_on is None:
            raise ValueError(f"[Transformation Error] Join clause missing 'on' condition for table '{full_table_path}'")

        # 2. Convert list of conditions to string if passed as a list
        if isinstance(raw_on, list):
            raw_on = " AND ".join(raw_on)

        # 3. Clean multiline SQL safely
        on_clause = Helper.clean_multiline_sql(str(raw_on))

        # 4. Strict emptiness check
        if not on_clause or not on_clause.strip():
            raise ValueError(f"[Transformation Error] Join clause missing 'on' condition for table '{full_table_path}'")
        
        # Handle Broadcast Hint
        should_broadcast = join_cfg.get("broadcast", False)
        broadcast_hint = "/*+ BROADCAST */ " if should_broadcast else ""

        # Optional Join Filter Condition
        join_filter = join_cfg.get("filter")
        if join_filter:
            cleaned_filter = Helper.clean_multiline_sql(join_filter)
            on_clause = f"({on_clause}) AND ({cleaned_filter})"

        return f"{how} JOIN {broadcast_hint}{full_table_path}{alias_str} ON {on_clause}"