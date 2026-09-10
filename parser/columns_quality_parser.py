import logging
from typing import Any, Dict, Generator, List, Set, Tuple
from yamlpipe.core.vars_manager import VariablesManager
from yamlpipe.registry.columns_quality_registry import ColumnQualityRegistry
from yamlpipe.utility.placeholder_resolver import TemplateResolver 
from yamlpipe.utility.logger import get_logger

logger = get_logger("[ ColumnQualityParser ]")


class ColumnQualityParser:

    ALLOWED_MULTI_COLUMN_CHECK_TYPES: Set[str] = {
        "custom",
        "compare",
        "compare_columns",
    }

    @classmethod
    def parse_yaml_checks(cls, yaml_config: Dict[str, Any]) -> Dict[str, Any]:
        columns_checks_config = yaml_config.get("columns_checks", [])

        if not columns_checks_config:
            logger.info("No 'columns_checks' configuration found. Returning empty check results.")
            return {
                "columns_checks": {"error_expr": [], "warn_expr": []},
                "registered_error_suffixes": [],
                "ContainCustomChecksFrom": []
            }

        logger.info(f"Starting parsing for {len(columns_checks_config)} column check entries...")

        error_expressions: List[str] = []
        warn_expressions: List[str] = []
        registered_error_suffixes: Set[str] = set()
        custom_checks_used: Set[str] = set()

        # Parse all entries directly without templating expansion
        for idx, col_entry in enumerate(columns_checks_config, start=1):
            if not isinstance(col_entry, dict):
                logger.warning(f"Skipping entry #{idx}: Expected dict, got {type(col_entry).__name__}")
                continue

            for check, column_name in cls._for_each_column(col_entry):
                try:
                    check_type = check.get("check_type") or check.get("type")

                    if check_type == "custom" or check.get("is_custom"):
                        custom_script = (
                            check.get("custom_check_name")
                            or check.get("script")
                            or check.get("check_name")
                            or check.get("the_check")
                        )
                        if custom_script:
                            logger.debug(f"Registered custom check script '{custom_script}' for column '{column_name}'")
                            custom_checks_used.add(custom_script)

                    sql_expr, severity, suffix = ColumnQualityRegistry.router(check, column_name)
                    cleaned_sql = sql_expr.strip()

                    if suffix:
                        registered_error_suffixes.add(suffix)

                    if severity == "error":
                        logger.debug(f"Added ERROR expression for '{column_name}': {cleaned_sql}")
                        error_expressions.append(cleaned_sql)
                    elif severity in ("warn", "warning"):
                        logger.debug(f"Added WARN expression for '{column_name}': {cleaned_sql}")
                        warn_expressions.append(cleaned_sql)
                    else:
                        logger.debug(f"Added WARN expression (default) for '{column_name}': {cleaned_sql}")
                        warn_expressions.append(cleaned_sql)

                except Exception as e:
                    check_identifier = check.get("check_type") or check.get("type") or "unknown"
                    logger.error(
                        f"Failed to parse check '{check_identifier}' for column '{column_name}': {str(e)}",
                        exc_info=True
                    )
                    raise e

        logger.info(
            f"Parsing complete: {len(error_expressions)} error expressions, "
            f"{len(warn_expressions)} warn expressions, "
            f"{len(registered_error_suffixes)} error suffixes registered."
        )

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

        # 1. Column-First Format
        if column_name:
            if VariablesManager.is_var(column_name):
                err_msg = f"Column-First format cannot use variables for 'column': '{column_name}'"
                logger.error(err_msg)
                raise ValueError(err_msg)

            checks = entry.get("checks", [])
            if isinstance(checks, list) and checks:
                logger.debug(f"Processing Column-First format for '{column_name}' with {len(checks)} sub-checks.")
                for check in checks:
                    if not isinstance(check, dict):
                        logger.warning(f"Skipping non-dict sub-check in column '{column_name}': {check}")
                        continue
                    resolved_check = TemplateResolver.resolve_placeholders(check, column_name)
                    yield resolved_check, column_name
            else:
                logger.debug(f"Processing single Column-First entry for '{column_name}'")
                resolved_check = TemplateResolver.resolve_placeholders(entry, column_name)
                yield resolved_check, column_name

        # 2. Check-First Format
        elif check_type:
            if VariablesManager.is_var(check_type):
                err_msg = f"Check-First format cannot use variables for 'check_type': '{check_type}'"
                logger.error(err_msg)
                raise ValueError(err_msg)

            check_type_str = str(check_type).lower().strip()
            has_iteration_list = bool(entry.get("columns") or entry.get("for_each"))

            if not has_iteration_list and check_type_str not in cls.ALLOWED_MULTI_COLUMN_CHECK_TYPES:
                err_msg = (
                    f"Check-First entry with check_type '{check_type}' requires a 'columns' or "
                    f"'for_each' list, or a single 'column'."
                )
                logger.error(err_msg)
                raise ValueError(err_msg)

            logger.debug(f"Expanding Check-First entry for check_type '{check_type}'")
            expanded_checks = TemplateResolver.resolve_and_expand(entry)
            for resolved_payload, col in expanded_checks:
                target_col = col or resolved_payload.get("column")
                
                # Skip generation if no real column was resolved (prevents generating 'table_check')
                if not target_col or target_col == "table_check":
                    logger.debug(f"Skipping check expansion for pseudo/table column target: '{target_col}'")
                    continue
                    
                yield resolved_payload, target_col

        else:
            logger.warning(f"Invalid YAML check entry format (missing both 'column' and 'check_type'/'type'): {entry}")