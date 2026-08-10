from typing import Dict, Any, Tuple
from yamlpipe.utility.helper import Helper


class TableQualityRegistry:

    # -------------------------------------------------------------------------
    # 1. DUPLICATE CHECK EXPR
    # -------------------------------------------------------------------------
    @staticmethod
    def build_duplicate_expr(check: Dict[str, Any]) -> Tuple[str, bool, bool]:
        output_column = check.get("output_column", "is_duplicate")
        raw_keys = check.get("keys", [])

        if not raw_keys:
            raise ValueError(f"[Duplicate Check Error] Missing required 'keys'. Config: {check}")

        partition_cols = ", ".join([Helper.clean_multiline_sql(k) for k in raw_keys])
        
        orderby_expr = (
            check.get("orderby") 
            or check.get("order_by") 
            or check.get("orderby_column") 
            or check.get("order_by_column") 
        )

        order_by_clause = ""
        if orderby_expr:
            cleaned_order = Helper.clean_multiline_sql(str(orderby_expr))
            order_by_clause = f"ORDER BY {cleaned_order} DESC"

        expr = f"CASE WHEN ROW_NUMBER() OVER (PARTITION BY {partition_cols} {order_by_clause}) > 1 THEN 1 ELSE 0 END AS `{output_column}`"
        on_split_keep = check.get("on_split_keep", False)
        is_freshness = False

        return expr, on_split_keep, is_freshness

    # -------------------------------------------------------------------------
    # 2. LOOKUP CHECK EXPR
    # -------------------------------------------------------------------------
    @classmethod
    def build_lookup_expr(cls, check: Dict[str, Any], ref_view: str) -> Tuple[str, bool, bool]:
        output_column = check.get("output_column", "is_lookup_failed")
        join_keys = check.get("keys", check.get("join_keys", []))

        if not join_keys:
            raise ValueError(f"[Lookup Check Error] Missing join keys under 'keys' or 'join_keys'. Config: {check}")

        filter_sql = ""
        if check.get("filter"):
            filter_sql = f"AND ({Helper.clean_multiline_sql(check['filter'])})"

        join_conditions = " AND ".join([f"tmp_src.`{k}` = r.`{k}`" for k in join_keys])

        should_broadcast = check.get("broadcast", True)
        broadcast_hint = "/*+ BROADCAST(r) */" if should_broadcast else ""

        expr = f"""CASE WHEN NOT EXISTS (
            SELECT {broadcast_hint} 1 
            FROM {ref_view} r 
            WHERE {join_conditions} {filter_sql}
        ) THEN 1 ELSE 0 END AS `{output_column}`"""
        on_split_keep = check.get("on_split_keep", False)
        is_freshness = False

        return expr, on_split_keep, is_freshness

    # -------------------------------------------------------------------------
    # 3. FOREIGN KEY CHECK EXPR
    # -------------------------------------------------------------------------
    @classmethod
    def build_foreign_key_expr(cls, check: Dict[str, Any], ref_view: str) -> Tuple[str, bool, bool]:
        ref_cfg = check.get("ref", {})
        if isinstance(ref_cfg, str):
            ref_cfg = {"table": ref_cfg}

        # دعم output_column سواء كان بداخل ref أو بالمستوى الرئيسي للـ check
        output_column = (
            check.get("output_column") 
            or ref_cfg.get("output_column") 
            or "is_fk_violation"
        )
        
        fk_col = check.get("foreign_key")
        if not fk_col:
            raise ValueError(f"[Foreign Key Error] Missing 'foreign_key' specification in config: {check}")

        ref_key = ref_cfg.get("key", fk_col)
        cleaned_fk_col = Helper.clean_multiline_sql(str(fk_col))
        cleaned_ref_key = Helper.clean_multiline_sql(str(ref_key))

        ref_filter_sql = ""
        if ref_cfg.get("filter"):
            ref_filter_sql = f"WHERE ({Helper.clean_multiline_sql(ref_cfg['filter'])})"

        should_broadcast = ref_cfg.get("broadcast", True)
        broadcast_hint = "/*+ BROADCAST */" if should_broadcast else ""

        # استخدام ARRAY_CONTAINS مع BROADCAST(COLLECT_SET): 
        # يحمي الأداء ومضمون 100% مع الـ Window Functions
        expr = f"""CASE 
            WHEN tmp_src.`{cleaned_fk_col}` IS NOT NULL 
                 AND NOT ARRAY_CONTAINS(
                    (SELECT {broadcast_hint} COLLECT_SET(`{cleaned_ref_key}`) FROM {ref_view} {ref_filter_sql}), 
                    tmp_src.`{cleaned_fk_col}`
                 ) 
            THEN 1 
            ELSE 0 
        END AS `{output_column}`"""

        on_split_keep = check.get("on_split_keep", False)
        is_freshness = False

        return expr, on_split_keep, is_freshness

    # -------------------------------------------------------------------------
    # 4. FRESHNESS CHECK EXPR
    # -------------------------------------------------------------------------
    @staticmethod
    def build_freshness_expr(check: Dict[str, Any]) -> Tuple[str, bool, bool]:
        output_column = check.get("output_column", "freshness_lag_seconds")
        
        ts_column = (
            check.get("freshness_column") 
            or check.get("timestamp_column") 
            or check.get("ts_column")
        )
        unit = str(check.get("unit", "seconds")).lower()

        if not ts_column:
            raise ValueError(f"[Freshness Check Error] Missing timestamp column specification. Config: {check}")

        ts_column_expr = Helper.clean_multiline_sql(str(ts_column))

        if unit == "max_timestamp":
            expr = f"MAX({ts_column_expr}) OVER () AS `{output_column}`"
        else:
            ref_ts_sql = Helper.clean_multiline_sql(check["ref_timestamp"]) if check.get("ref_timestamp") else "CURRENT_TIMESTAMP()"

            divisor = 1.0
            if unit == "hours":
                divisor = 3600.0
            elif unit == "days":
                divisor = 86400.0
            elif unit != "seconds":
                raise ValueError(f"[Freshness Check Error] Unsupported freshness unit '{unit}'.")

            expr = f"(UNIX_TIMESTAMP({ref_ts_sql}) - UNIX_TIMESTAMP(MAX({ts_column_expr}) OVER ())) / {divisor} AS `{output_column}`"

        return expr, True, True