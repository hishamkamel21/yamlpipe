import logging
from typing import Dict, Any, Union
from yamlpipe.utility.module_loader import ModuleLoader
from yamlpipe.utility.helper import Helper

logger = logging.getLogger("TransformationRegistry")


class TransformationRegistry:

    @classmethod
    def build_rule_expr(cls, rule: Dict[str, Any]) -> str:
        """
        Translates a single column transformation rule into a SQL select expression string.
        """
        column_name = rule.get("column")
        if not column_name:
            raise ValueError(f"Rule missing required 'column' key: {rule}")

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