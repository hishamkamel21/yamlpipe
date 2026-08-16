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
        elif "select_the_rest" in rule_cfg:
            return cls._handle_select_the_rest(rule_cfg)
        elif "run" in rule_cfg:
            return cls._handle_run(rule_cfg)
        elif "column" in rule_cfg and "expression" in rule_cfg:
            return cls._handle_column_expr(rule_cfg)
        else:
            return [rule_cfg]

    @classmethod
    def _handle_call_function(cls, rule_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Loads function via ModuleLoader, substitutes ${col} parameter references,
        executes custom Python function per column, and generates 'column'/'expression' dictionaries.
        """
        func_name = rule_cfg.get("call_function")
        raw_params = rule_cfg.get("params", {})
        columns = rule_cfg.get("columns", [])

        resolved_rules = []

        for col in columns:
            # Substitute ${col} or ${column} placeholders in function parameters
            resolved_params = {}
            for param_key, param_val in raw_params.items():
                if isinstance(param_val, str):
                    resolved_params[param_key] = param_val.replace("${col}", col).replace("${column}", col)
                else:
                    resolved_params[param_key] = param_val

            try:
                # Load python file from functions/ and run handle_strings(col_name=col)
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
        """Parses select_the_rest directives into normalized structure."""
        rest_cfg = rule_cfg.get("select_the_rest", {})
        return [{
            "select_the_rest": {
                "enable": bool(rest_cfg.get("enable", False)),
                "except": rest_cfg.get("except", [])
            }
        }]

    @classmethod
    def _handle_run(cls, rule_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parses run expressions (e.g., location.*)."""
        return [{
            "run": str(rule_cfg.get("run", "")).strip()
        }]

    @classmethod
    def _handle_column_expr(cls, rule_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parses standard column expression rules."""
        return [{
            "column": str(rule_cfg.get("column", "")).strip(),
            "expression": str(rule_cfg.get("expression", "")).strip()
        }]