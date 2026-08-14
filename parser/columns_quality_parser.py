import logging
from typing import Dict, Any, List, Set, Tuple, Generator
from yamlpipe.registry.columns_quality_registry import ColumnQualityRegistry

logger = logging.getLogger("ColumnQualityParser")


class ColumnQualityParser:

    @classmethod
    def parse_yaml_checks(cls, yaml_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parses the 'columns_checks' section from the YAML config.
        Supports both traditional Column-First format and Check-First bulk checks format.

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
            if not isinstance(col_entry, dict):
                logger.warning(f"Skipping invalid column entry: {col_entry}")
                continue

            # Process all (check, column) pairs derived from the entry
            for check, column_name in cls._for_each_column(col_entry):
                try:
                    # Router returns sql_expr, severity, and error suffix
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

    @classmethod
    def _for_each_column(cls, entry: Dict[str, Any]) -> Generator[Tuple[Dict[str, Any], str], None, None]:
        """
        Normalizes both YAML syntax structures into (check_dict, column_name) pairs.

        1. Column-First Format:
           - column: customer_id
             checks:
               - check_type: not_null
                 severity: error

        2. Check-First Format:
           - check_type: not_null  # or type: not_null
             severity: error
             columns:
               - customer_id
               - age
        """
        column_name = entry.get("column")
        check_type = entry.get("check_type") or entry.get("type")

        # Format 1: Column-First syntax
        if column_name:
            checks = entry.get("checks", [])
            for check in checks:
                if isinstance(check, dict):
                    yield check, column_name
                else:
                    logger.warning(f"Skipping invalid check structure under column '{column_name}': {check}")

        # Format 2: Check-First syntax
        elif check_type:
            target_columns = entry.get("columns", [])
            if not isinstance(target_columns, list) or not target_columns:
                logger.warning(f"Check-First entry for '{check_type}' missing 'columns' list.")
                return

            # Extract check payload without the 'columns' key to avoid router pollution
            check_payload = {k: v for k, v in entry.items() if k != "columns"}

            for col in target_columns:
                if isinstance(col, str) and col.strip():
                    yield check_payload, col.strip()
                else:
                    logger.warning(f"Skipping invalid column name '{col}' in check '{check_type}'.")

        else:
            logger.warning("Skipping entry that matches neither Column-First nor Check-First format.")