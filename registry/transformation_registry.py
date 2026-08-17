import logging
from typing import Dict, Any, List
from yamlpipe.utility.template_resolver import TemplateResolver

logger = logging.getLogger("TransformationRegistry")


class TransformationRegistry:

    @classmethod
    def process_rule(cls, rule_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        has_multi_col = "columns" in rule_cfg or "for_each" in rule_cfg

        if "call_function" in rule_cfg:
            return cls._handle_call_function(rule_cfg)
        elif "expression" in rule_cfg and has_multi_col:
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
        expanded = TemplateResolver.resolve_and_expand(rule_cfg)
        resolved_rules = []
        for payload, col in expanded:
            resolved_rules.append({
                "column": col,
                "expression": str(payload.get("expression", "")).strip()
            })
        return resolved_rules

    @classmethod
    def _handle_call_function(cls, rule_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        func_name = rule_cfg.get("call_function")
        expanded = TemplateResolver.resolve_and_expand(rule_cfg)
        resolved_rules = []

        for payload, col in expanded:
            resolved_params = payload.get("params", {})
            try:
                from yamlpipe.utility.module_loader import ModuleLoader
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