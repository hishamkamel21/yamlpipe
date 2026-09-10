import logging
import uuid
from typing import Any, Dict, List
from yamlpipe.registry.table_quality_checks import TableQualityRegistry
from yamlpipe.core.vars_manager import VariablesManager
from yamlpipe.utility.helper import Helper
from yamlpipe.utility.logger import get_logger

logger = get_logger("[ TableQualityParser ]") 


class TableQualityParser:

    @classmethod
    def parse_yaml_checks(cls, yaml_config: Dict[str, Any]) -> Dict[str, Any]:
        checks = yaml_config.get("table_checks", yaml_config.get("checks", []))

        if not checks:
            logger.info("No 'table_checks' or 'checks' configuration found. Returning empty table check results.")
            return {
                "table_checks": {
                    "checks": [],
                    "temp_views_to_create": []
                }
            }

        logger.info(f"Starting parsing for {len(checks)} table quality check entries...")

        parsed_checks: List[Dict[str, Any]] = []
        temp_views_to_create: List[Dict[str, Any]] = []

        for idx, check in enumerate(checks, start=1):
            if not isinstance(check, dict):
                logger.warning(f"Skipping table check entry #{idx}: Expected dict, got {type(check).__name__}")
                continue

            check_type = str(check.get("check_type") or check.get("type") or "").strip().lower()

            if VariablesManager.is_var(check_type):
                err_msg = f"Table check type cannot be a variable placeholder: '{check_type}'"
                logger.error(err_msg)
                raise ValueError(err_msg)

            logger.debug(f"Parsing table check entry #{idx} with type '{check_type}'")

            try:
                if check_type == "duplicate":
                    expr, on_split_keep, is_freshness = TableQualityRegistry.build_duplicate_expr(check)
                    logger.debug(f"Successfully generated DUPLICATE check expression.")

                elif check_type in ("lookup", "foreign_key"):
                    ref_view = f"tmp_ref_{uuid.uuid4().hex[:8]}"
                    logger.debug(f"Generated temp view name '{ref_view}' for check type '{check_type}'")

                    ref_meta = check.get("ref") if isinstance(check.get("ref"), dict) else check
                    
                    table_cfg = ref_meta.get("table") or ref_meta.get("lookup_table")
                    path_source = ref_meta.get("path") or ref_meta.get("lookup_path")

                    parsed_table = None
                    if table_cfg:
                        parsed_table = Helper.parse_table_name(table_cfg)

                    if not parsed_table and not path_source:
                        err_msg = (
                            f"[{check_type.upper()} Error] Reference config must specify either a valid "
                            f"'table' or 'path'. Config: {check}"
                        )
                        logger.error(err_msg)
                        raise ValueError(err_msg)

                    view_metadata = {
                        "view_name": ref_view,
                        "table": parsed_table,
                        "path": path_source,
                        "format": ref_meta.get("format", "delta"),
                        "filter": ref_meta.get("filter") or check.get("filter")
                    }
                    temp_views_to_create.append(view_metadata)
                    logger.debug(f"Registered temp view metadata for '{ref_view}': table='{parsed_table}', path='{path_source}'")

                    if check_type == "lookup":
                        expr, on_split_keep, is_freshness = TableQualityRegistry.build_lookup_expr(check, ref_view)
                    else:
                        expr, on_split_keep, is_freshness = TableQualityRegistry.build_foreign_key_expr(check, ref_view)

                    logger.debug(f"Successfully generated {check_type.upper()} check expression.")

                elif check_type == "freshness":
                    expr, on_split_keep, is_freshness = TableQualityRegistry.build_freshness_expr(check)
                    logger.debug(f"Successfully generated FRESHNESS check expression.")

                else:
                    logger.warning(f"Unrecognized or unsupported table check type '{check_type}'. Skipping entry.")
                    continue

                parsed_checks.append({
                    "expr": expr,
                    "on_split_keep": on_split_keep,
                    "is_freshness": is_freshness
                })

            except Exception as e:
                logger.error(
                    f"Failed to parse table check of type '{check_type}' at entry #{idx}: {str(e)}",
                    exc_info=True
                )
                raise e

        logger.info(
            f"Table quality checks parsing complete: {len(parsed_checks)} check expression(s) generated, "
            f"{len(temp_views_to_create)} temp view(s) registered."
        )

        return {
            "table_checks": {
                "checks": parsed_checks,
                "temp_views_to_create": temp_views_to_create
            }
        }