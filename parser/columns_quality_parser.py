import logging
from typing import Any, Dict, Generator, List, Set, Tuple
from yamlpipe.core.vars_manager import VariablesManager
from yamlpipe.registry.columns_quality_registry import ColumnQualityRegistry

logger = logging.getLogger("ColumnQualityParser")


class ColumnQualityParser:

    @classmethod
    def parse_yaml_checks(cls, yaml_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parses the 'columns_checks' section from the YAML config.
        Tracks custom python check scripts used in Column Checks.
        """
        columns_checks_config = yaml_config.get("columns_checks", [])

        if not columns_checks_config:
            return {
                "columns_checks": {
                    "error_expr": [],
                    "warn_expr": []
                },
                "registered_error_suffixes": [],
                "ContainCustomChecksFrom": []
            }

        error_expressions: List[str] = []
        warn_expressions: List[str] = []
        registered_error_suffixes: Set[str] = set()
        custom_checks_used: Set[str] = set()

        for col_entry in columns_checks_config:
            if not isinstance(col_entry, dict):
                logger.warning(f"Skipping invalid column entry: {col_entry}")
                continue

            for check, column_name in cls._for_each_column(col_entry):
                try:
                    check_type = check.get("check_type") or check.get("type")

                    # تجميع أسماء الـ custom checks
                    if check_type == "custom" or check.get("is_custom"):
                        custom_script = (
                            check.get("custom_check_name")
                            or check.get("script")
                            or check.get("check_name")
                            or check.get("the_check")
                        )
                        if custom_script:
                            custom_checks_used.add(custom_script)

                    sql_expr, severity, suffix = ColumnQualityRegistry.router(check, column_name)
                    cleaned_sql = sql_expr.strip()

                    if suffix:
                        registered_error_suffixes.add(suffix)

                    if severity == "error":
                        error_expressions.append(cleaned_sql)
                    elif severity in ("warn", "warning"):
                        warn_expressions.append(cleaned_sql)
                    else:
                        logger.warning(
                            f"Unknown severity '{severity}' for column '{column_name}'. Defaulting to 'warn'."
                        )
                        warn_expressions.append(cleaned_sql)

                except Exception as e:
                    check_identifier = check.get("check_type") or check.get("type") or "unknown"
                    logger.error(
                        f"Failed to parse check '{check_identifier}' for column '{column_name}': {str(e)}"
                    )
                    raise e

        return {
            "columns_checks": {
                "error_expr": error_expressions,
                "warn_expr": warn_expressions
            },
            "registered_error_suffixes": sorted(list(registered_error_suffixes)),
            "ContainCustomChecksFrom": sorted(list(custom_checks_used))
        }

    @classmethod
    def _for_each_column(cls, entry: Dict[str, Any]) -> Generator[Tuple[Dict[str, Any], str], None, None]:
        column_name = entry.get("column")
        check_type = entry.get("check_type") or entry.get("type")

        # Column-First Format
        if column_name:
            if VariablesManager.is_var(column_name):
                raise ValueError(f"Column-First format cannot use variables for 'column' name: '{column_name}'")

            checks = entry.get("checks", [])
            for check in checks:
                if not isinstance(check, dict):
                    logger.warning(f"Skipping invalid check structure under column '{column_name}': {check}")
                    continue

                sub_check_type = check.get("check_type") or check.get("type")
                sub_severity = check.get("severity")

                if VariablesManager.is_var(sub_check_type):
                    raise ValueError(f"Check type inside Column-First format cannot be a variable: '{sub_check_type}'")
                if VariablesManager.is_var(sub_severity):
                    raise ValueError(f"Severity inside Column-First format cannot be a variable: '{sub_severity}'")

                yield check, column_name

        # Check-First Format
        elif check_type:
            if VariablesManager.is_var(check_type):
                raise ValueError(f"Check-First format cannot use variables for 'check_type': '{check_type}'")

            severity = entry.get("severity")
            if VariablesManager.is_var(severity):
                raise ValueError(f"Check-First format cannot use variables for 'severity': '{severity}'")

            target_columns = entry.get("columns", [])
            if not isinstance(target_columns, list) or not target_columns:
                logger.warning(f"Check-First entry for '{check_type}' missing 'columns' list.")
                return

            check_payload = {k: v for k, v in entry.items() if k != "columns"}

            for col in target_columns:
                if isinstance(col, str) and col.strip():
                    yield check_payload, col.strip()
                else:
                    logger.warning(f"Skipping invalid column name '{col}' in check '{check_type}'.")

        else:
            logger.warning("Skipping entry that matches neither Column-First nor Check-First format.")