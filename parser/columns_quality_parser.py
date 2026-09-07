import logging
from typing import Any, Dict, Generator, List, Set, Tuple
from yamlpipe.core.template_manager import TemplateManager
from yamlpipe.core.vars_manager import VariablesManager
from yamlpipe.registry.columns_quality_registry import ColumnQualityRegistry
from yamlpipe.utility.placeholder_resolver import TemplateResolver

logger = logging.getLogger("ColumnQualityParser")


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
            return {
                "columns_checks": {"error_expr": [], "warn_expr": []},
                "registered_error_suffixes": [],
                "ContainCustomChecksFrom": [],
                "ContainTemplatesFrom": []
            }

        templates_used: Set[str] = set()
        expanded_columns_checks: List[Dict[str, Any]] = []

        # 1. Expand inject_template entries using TemplateManager
        for col_entry in columns_checks_config:
            if not isinstance(col_entry, dict):
                continue

            if "inject_template" in col_entry:
                inject_info = col_entry["inject_template"]
                template_name = inject_info.get("name")
                raw_with_vars = inject_info.get("with", {})

                if template_name:
                    templates_used.add(template_name)

                    resolved_checks = TemplateManager.inject_handler(
                        template_name=template_name,
                        with_vars=raw_with_vars
                    )

                    if isinstance(resolved_checks, list):
                        expanded_columns_checks.extend(resolved_checks)
            else:
                expanded_columns_checks.append(col_entry)

        error_expressions: List[str] = []
        warn_expressions: List[str] = []
        registered_error_suffixes: Set[str] = set()
        custom_checks_used: Set[str] = set()

        # 2. Parse all entries
        for col_entry in expanded_columns_checks:
            if not isinstance(col_entry, dict):
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
            "ContainCustomChecksFrom": sorted(list(custom_checks_used)),
            "ContainTemplatesFrom": sorted(list(templates_used))
        }

    @classmethod
    def _for_each_column(cls, entry: Dict[str, Any]) -> Generator[Tuple[Dict[str, Any], str], None, None]:
        column_name = entry.get("column")
        check_type = entry.get("check_type") or entry.get("type")

        # 1. Column-First Format
        if column_name:
            if VariablesManager.is_var(column_name):
                raise ValueError(f"Column-First format cannot use variables for 'column': '{column_name}'")

            checks = entry.get("checks", [])
            if isinstance(checks, list) and checks:
                for check in checks:
                    if not isinstance(check, dict):
                        continue
                    resolved_check = TemplateResolver.resolve_placeholders(check, column_name)
                    yield resolved_check, column_name
            else:
                resolved_check = TemplateResolver.resolve_placeholders(entry, column_name)
                yield resolved_check, column_name

        # 2. Check-First Format
        elif check_type:
            if VariablesManager.is_var(check_type):
                raise ValueError(f"Check-First format cannot use variables for 'check_type': '{check_type}'")

            check_type_str = str(check_type).lower().strip()
            has_iteration_list = bool(entry.get("columns") or entry.get("for_each"))

            if not has_iteration_list and check_type_str not in cls.ALLOWED_MULTI_COLUMN_CHECK_TYPES:
                raise ValueError(
                    f"Check-First entry with check_type '{check_type}' requires a 'columns' or 'for_each' list, or a single 'column'."
                )

            expanded_checks = TemplateResolver.resolve_and_expand(entry)
            for resolved_payload, col in expanded_checks:
                fallback_col = col or resolved_payload.get("column") or "table_check"
                yield resolved_payload, fallback_col