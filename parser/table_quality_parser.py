import uuid
from typing import Dict, Any, List
from yamlpipe.registry.table_quality_checks import TableQualityRegistry

class TableQualityParser:

    @classmethod
    def parse_yaml_checks(cls, yaml_config: Dict[str, Any]) -> Dict[str, Any]:
        checks = yaml_config.get("checks", [])
        if not checks:
            return {
                "table_checks": {
                    "expr": "",
                    "temp_views_to_create": []
                }
            }

        expressions: List[str] = []
        temp_views_to_create: List[Dict[str, Any]] = []

        for check in checks:
            check_type = str(check.get("check_type", "")).strip().lower()

            if check_type == "duplicate":
                expr = TableQualityRegistry.build_duplicate_expr(check)
                expressions.append(expr)

            elif check_type in ("lookup", "foreign_key"):
                ref_view = f"tmp_ref_{uuid.uuid4().hex[:8]}"
                
                ref_meta = check.get("ref") if isinstance(check.get("ref"), dict) else check
                view_metadata = {
                    "view_name": ref_view,
                    "table": ref_meta.get("table") or ref_meta.get("lookup_table"),
                    "path": ref_meta.get("path") or ref_meta.get("lookup_path"),
                    "format": ref_meta.get("format", "delta")
                }
                temp_views_to_create.append(view_metadata)

                if check_type == "lookup":
                    expr = TableQualityRegistry.build_lookup_expr(check, ref_view)
                else:
                    expr = TableQualityRegistry.build_foreign_key_expr(check, ref_view)
                
                expressions.append(expr)

            elif check_type == "freshness":
                expr = TableQualityRegistry.build_freshness_expr(check)
                expressions.append(expr)

        return {
            "table_checks": {
                "expr": ",\n".join(expressions),
                "temp_views_to_create": temp_views_to_create
            }
        }