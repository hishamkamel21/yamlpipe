import uuid
from typing import Dict, Any, List
from yamlpipe.registry.table_quality_checks import TableQualityRegistry
from yamlpipe.core.vars_manager import VariablesManager
from yamlpipe.utility.helper import Helper


class TableQualityParser:

    @classmethod
    def parse_yaml_checks(cls, yaml_config: Dict[str, Any]) -> Dict[str, Any]:
        checks = yaml_config.get("table_checks", yaml_config.get("checks", []))

        if not checks:
            return {
                "table_checks": {
                    "checks": [],
                    "temp_views_to_create": []
                }
            }

        parsed_checks: List[Dict[str, Any]] = []
        temp_views_to_create: List[Dict[str, Any]] = []

        for check in checks:
            check_type = str(check.get("check_type") or check.get("type") or "").strip().lower()

            if VariablesManager.is_var(check_type):
                raise ValueError(f"Table check type cannot be a variable placeholder: '{check_type}'")

            if check_type == "duplicate":
                expr, on_split_keep, is_freshness = TableQualityRegistry.build_duplicate_expr(check)

            elif check_type in ("lookup", "foreign_key"):
                ref_view = f"tmp_ref_{uuid.uuid4().hex[:8]}"

                ref_meta = check.get("ref") if isinstance(check.get("ref"), dict) else check
                
                table_cfg = ref_meta.get("table") or ref_meta.get("lookup_table")
                path_source = ref_meta.get("path") or ref_meta.get("lookup_path")

                parsed_table = None
                if table_cfg:
                    # تحويل الكائن (dict) إلى اسم جدول كامل بالشكل catalog.schema.table
                    parsed_table = Helper.parse_table_name(table_cfg)

                if not parsed_table and not path_source:
                    raise ValueError(
                        f"[{check_type.upper()} Error] Reference config must specify either a valid 'table' or 'path'. Config: {check}"
                    )

                view_metadata = {
                    "view_name": ref_view,
                    "table": parsed_table,
                    "path": path_source,
                    "format": ref_meta.get("format", "delta"),
                    "filter": ref_meta.get("filter") or check.get("filter")
                }
                temp_views_to_create.append(view_metadata)

                if check_type == "lookup":
                    expr, on_split_keep, is_freshness = TableQualityRegistry.build_lookup_expr(check, ref_view)
                else:
                    expr, on_split_keep, is_freshness = TableQualityRegistry.build_foreign_key_expr(check, ref_view)

            elif check_type == "freshness":
                expr, on_split_keep, is_freshness = TableQualityRegistry.build_freshness_expr(check)

            else:
                continue

            parsed_checks.append({
                "expr": expr,
                "on_split_keep": on_split_keep,
                "is_freshness": is_freshness
            })

        return {
            "table_checks": {
                "checks": parsed_checks,
                "temp_views_to_create": temp_views_to_create
            }
        }