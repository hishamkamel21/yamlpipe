import logging
from typing import Dict, Any, List, Set
from yamlpipe.registry.columns_quality_registry import ColumnQualityRegistry

logger = logging.getLogger("ColumnQualityParser")


class ColumnQualityParser:

    @classmethod
    def parse_yaml_checks(cls, yaml_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parses the 'columns_checks' section from the YAML config.
        Extracts SQL expressions into Python lists per severity and collects suffixes returned by the registry router.

        Returns:
            dict: {
                "columns_checks": {
                    "error_expr": ["CASE WHEN ... THEN array(...) ELSE array() END", ...],
                    "warn_expr": ["CASE WHEN ... THEN array(...) ELSE array() END", ...]
                },
                "registered_error_suffixes": ["NULL_ERROR", "REGEX_ERROR", ...]
            }
        """
        columns_checks_config = yaml_config.get("columns_checks", [])

        if not columns_checks_config:
            return {
                "columns_checks": {
                    "error_expr": [],
                    "warn_expr": []
                },
                "registered_error_suffixes": []
            }

        error_expressions: List[str] = []
        warn_expressions: List[str] = []
        registered_error_suffixes: Set[str] = set()

        for col_entry in columns_checks_config:
            column_name = col_entry.get("column")
            checks = col_entry.get("checks", [])

            if not column_name:
                logger.warning("Skipping column entry missing 'column' field.")
                continue

            for check in checks:
                try:
                    # 1. Router returns sql_expr, severity, and the exact error suffix
                    sql_expr, severity, suffix = ColumnQualityRegistry.router(check, column_name)
                    cleaned_sql = sql_expr.strip()

                    if suffix:
                        registered_error_suffixes.add(suffix)

                    # 2. Append SQL string directly to corresponding python list
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
                    check_identifier = check.get('check_type') or check.get('type') or 'unknown'
                    logger.error(
                        f"Failed to parse check '{check_identifier}' for column '{column_name}': {str(e)}"
                    )
                    raise e

        return {
            "columns_checks": {
                "error_expr": error_expressions,
                "warn_expr": warn_expressions
            },
            "registered_error_suffixes": sorted(list(registered_error_suffixes))
        }