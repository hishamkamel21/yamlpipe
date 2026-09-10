from typing import Dict, Any, Tuple
from yamlpipe.utility.helper import Helper


class TableQualityRegistry:

    # -------------------------------------------------------------------------
    # 1. DUPLICATE CHECK EXPR
    # -------------------------------------------------------------------------
    @staticmethod
    def build_duplicate_expr(check: Dict[str, Any]) -> Tuple[str, bool, bool]:
        output_column = check.get("output_col") or check.get("output_column") or "is_duplicate"
        raw_keys = check.get("keys", [])

        if not raw_keys:
            raise ValueError(f"[Duplicate Check Error] Missing required 'keys'. Config: {check}")

        partition_cols = ", ".join([Helper.clean_multiline_sql(str(k)) for k in raw_keys])
        
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
        """
        Builds SQL expressions for table lookup validation and attribute enrichment.
        Supports both simple existence checks (EXISTS) and value retrieval (Scalar Subquery / STRUCT).
        """
        source_key = check.get("key") or check.get("column")
        if not source_key:
            raise ValueError(f"[Lookup Error] Missing 'key' or 'column' specification in config: {check}")

        ref_meta = check.get("ref", {}) if isinstance(check.get("ref"), dict) else {}
        target_key = ref_meta.get("key") or check.get("target_key") or source_key
        
        output_col = (
            check.get("output_col") 
            or check.get("output_column") 
            or ref_meta.get("output_col") 
            or ref_meta.get("output_column") 
            or "lookup_invalid"
        )
        
        cleaned_source_key = Helper.clean_multiline_sql(str(source_key))
        cleaned_target_key = Helper.clean_multiline_sql(str(target_key))

        should_broadcast = ref_meta.get("broadcast", check.get("broadcast", True))
        broadcast_hint = "/*+ BROADCAST(ref_tbl) */" if should_broadcast else ""

        filter_str = ref_meta.get("filter") or check.get("filter")
        filter_sql = f"AND ({Helper.clean_multiline_sql(str(filter_str))})" if filter_str else ""

        target_expr = f"`ref_tbl`.`{cleaned_target_key}`" if " " not in cleaned_target_key and "(" not in cleaned_target_key else cleaned_target_key
        source_expr = f"`tmp_src`.`{cleaned_source_key}`" if " " not in cleaned_source_key and "(" not in cleaned_source_key else cleaned_source_key

        raw_select = ref_meta.get("select") or check.get("select")

        # ---------------------------------------------------------------------
        # CASE A: User requested column enrichment via 'select'
        # ---------------------------------------------------------------------
        if raw_select:
            select_cols = [
                Helper.clean_multiline_sql(c.strip()) 
                for c in str(raw_select).replace("\n", ",").split(",") 
                if c.strip()
            ]

            struct_fields = ", ".join([
                f"`ref_tbl`.`{col}`" if " " not in col and "(" not in col else col 
                for col in select_cols
            ])

            # Correlated scalar subquery building a STRUCT of selected fields
            lookup_subquery = f"""(
                SELECT {broadcast_hint} STRUCT({struct_fields})
                FROM `{ref_view}` AS `ref_tbl`
                WHERE {target_expr} = {source_expr} {filter_sql}
                LIMIT 1
            )"""

            # Build expression projecting structural fields and computing invalid flag
            field_projections = [
                f"`_lookup_struct`.`{col}` AS `{col}`" for col in select_cols
            ]
            validation_flag = f"CASE WHEN `_lookup_struct` IS NULL THEN 1 ELSE 0 END AS `{output_col}`"

            expr = f"""
                WITH `_lookup_struct` AS {lookup_subquery}
                SELECT {', '.join(field_projections)}, {validation_flag}
            """.strip()

            # Simplified single inline projection expression for pipeline generators
            select_projections = ", ".join([
                f"{lookup_subquery}.`{col}` AS `{col}`" for col in select_cols
            ])
            expr = f"{select_projections}, (CASE WHEN {lookup_subquery} IS NULL THEN 1 ELSE 0 END) AS `{output_col}`"

        # ---------------------------------------------------------------------
        # CASE B: Standard Existence Check (No extra columns selected)
        # ---------------------------------------------------------------------
        else:
            expr = f"""(CASE WHEN EXISTS (
                SELECT {broadcast_hint} 1 
                FROM `{ref_view}` AS `ref_tbl` 
                WHERE {target_expr} = {source_expr} {filter_sql}
            ) THEN 0 ELSE 1 END) AS `{output_col}`""".strip()

        on_split_keep = check.get("on_split_keep", False)
        is_freshness = False

        return expr, on_split_keep, is_freshness

    # -------------------------------------------------------------------------
    # 3. FOREIGN KEY CHECK EXPR
    # -------------------------------------------------------------------------
    @classmethod
    def build_foreign_key_expr(cls, check: Dict[str, Any], ref_view: str) -> Tuple[str, bool, bool]:
        ref_cfg = check.get("ref", {}) if isinstance(check.get("ref"), dict) else {}
        if isinstance(check.get("ref"), str):
            ref_cfg = {"table": check.get("ref")}

        output_column = (
            check.get("output_col") 
            or check.get("output_column") 
            or ref_cfg.get("output_col") 
            or ref_cfg.get("output_column") 
            or "is_fk_violation"
        )
        
        fk_col = check.get("foreign_key") or check.get("column") or check.get("key")
        if not fk_col:
            raise ValueError(f"[Foreign Key Error] Missing 'foreign_key' specification in config: {check}")

        ref_key = ref_cfg.get("key", fk_col)
        cleaned_fk_col = Helper.clean_multiline_sql(str(fk_col))
        cleaned_ref_key = Helper.clean_multiline_sql(str(ref_key))

        filter_str = ref_cfg.get("filter") or check.get("filter")
        filter_sql = f"AND ({Helper.clean_multiline_sql(str(filter_str))})" if filter_str else ""

        should_broadcast = ref_cfg.get("broadcast", check.get("broadcast", True))
        broadcast_hint = "/*+ BROADCAST(r) */" if should_broadcast else ""

        fk_expr = f"tmp_src.`{cleaned_fk_col}`" if " " not in cleaned_fk_col and "(" not in cleaned_fk_col else cleaned_fk_col
        ref_expr = f"r.`{cleaned_ref_key}`" if " " not in cleaned_ref_key and "(" not in cleaned_ref_key else cleaned_ref_key

        expr = f"""CASE 
            WHEN {fk_expr} IS NOT NULL 
                 AND NOT EXISTS (
                     SELECT {broadcast_hint} 1 
                     FROM `{ref_view}` r 
                     WHERE {ref_expr} = {fk_expr} {filter_sql}
                 ) 
            THEN 1 
            ELSE 0 
        END AS `{output_column}`""".strip()

        on_split_keep = check.get("on_split_keep", False)
        is_freshness = False

        return expr, on_split_keep, is_freshness

    # -------------------------------------------------------------------------
    # 4. FRESHNESS CHECK EXPR
    # -------------------------------------------------------------------------
    @staticmethod
    def build_freshness_expr(check: Dict[str, Any]) -> Tuple[str, bool, bool]:
        output_column = check.get("output_col") or check.get("output_column") or "freshness_lag_seconds"
        
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
            ref_ts_sql = Helper.clean_multiline_sql(str(check["ref_timestamp"])) if check.get("ref_timestamp") else "CURRENT_TIMESTAMP()"

            divisor = 1.0
            if unit == "hours":
                divisor = 3600.0
            elif unit == "days":
                divisor = 86400.0
            elif unit != "seconds":
                raise ValueError(f"[Freshness Check Error] Unsupported freshness unit '{unit}'.")

            expr = f"(UNIX_TIMESTAMP({ref_ts_sql}) - UNIX_TIMESTAMP(MAX({ts_column_expr}) OVER ())) / {divisor} AS `{output_column}`"

        return expr, True, True