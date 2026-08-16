import logging
from typing import Dict, Any, List
from yamlpipe.utility.module_loader import ModuleLoader

logger = logging.getLogger("TransformationRegistry")


class TransformationRegistry:

    @classmethod
    def process_rule(cls, rule_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Routes rule structures to dedicated handlers.
        Returns a list of standardized rule definitions.
        """
        if "call_function" in rule_cfg:
            return cls._handle_call_function(rule_cfg)
        elif "expression" in rule_cfg and "columns" in rule_cfg:
            return cls._handle_template_expression(rule_cfg)
        elif "select_the_rest" in rule_cfg:
            return cls._handle_select_the_rest(rule_cfg)
        elif "run" in rule_cfg:
            return cls._handle_run(rule_cfg)
        elif "column" in rule_cfg and "expression" in rule_cfg:
            return cls._handle_column_expr(rule_cfg)
        else:
            return [rule_cfg]

    @classmethod
    def _handle_template_expression(cls, rule_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Handles inline YAML template expressions like:
        - expression: upper(trim(${col}))
          columns: [customer_id, customer_name]
        """
        raw_expr = rule_cfg.get("expression", "")
        columns = rule_cfg.get("columns", [])

        resolved_rules = []
        for col in columns:
            # Substitute ${col} or ${column} placeholders with the column name
            resolved_expr = raw_expr.replace("${col}", col).replace("${column}", col)
            resolved_rules.append({
                "column": col,
                "expression": resolved_expr.strip()
            })

        return resolved_rules

    @classmethod
    def _handle_call_function(cls, rule_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Loads python functions via ModuleLoader and substitutes ${col} parameter references.
        """
        func_name = rule_cfg.get("call_function")
        raw_params = rule_cfg.get("params", {})
        columns = rule_cfg.get("columns", [])

        resolved_rules = []

        for col in columns:
            resolved_params = {}
            for param_key, param_val in raw_params.items():
                if isinstance(param_val, str):
                    resolved_params[param_key] = param_val.replace("${col}", col).replace("${column}", col)
                else:
                    resolved_params[param_key] = param_val

            try:
                sql_expr = ModuleLoader.functions_loader(func_name, **resolved_params)

                if not isinstance(sql_expr, str):
                    raise TypeError(
                        f"Function '{func_name}' returned {type(sql_expr).__name__}. Expected str SQL expression."
                    )

                resolved_rules.append({
                    "column": col,
                    "expression": sql_expr
                })

            except Exception as e:
                logger.error(f"Error evaluating call_function '{func_name}' for column '{col}': {str(e)}")
                raise e

        return resolved_rules

    @classmethod
    def _handle_select_the_rest(cls, rule_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        rest_cfg = rule_cfg.get("select_the_rest", {})
        return [{
            "select_the_rest": {
                "enable": bool(rest_cfg.get("enable", False)),
                "except": rest_cfg.get("except", [])
            }
        }]

    @classmethod
    def _handle_run(cls, rule_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [{
            "run": str(rule_cfg.get("run", "")).strip()
        }]

    @classmethod
    def _handle_column_expr(cls, rule_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [{
            "column": str(rule_cfg.get("column", "")).strip(),
            "expression": str(rule_cfg.get("expression", "")).strip()
        }]