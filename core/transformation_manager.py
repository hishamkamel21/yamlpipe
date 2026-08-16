import logging
import re
from typing import Dict, Any, List, Set, Tuple
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from yamlpipe.utility.helper import Helper

logger = logging.getLogger("TransformationManager")


class TransformationManager:

    def __init__(self, parsed_config: Dict[str, Any], df: DataFrame):
        if not isinstance(parsed_config, dict):
            raise TypeError(f"[TransformationManager Error] Expected dict, got {type(parsed_config).__name__}")
        if not isinstance(df, DataFrame):
            raise TypeError(f"[TransformationManager Error] Expected PySpark DataFrame, got {type(df).__name__}")

        self.parsed_config = parsed_config
        self.df = df
        self.spark = df.sparkSession
        self.main_alias = parsed_config.get("alias") or "c"
        raw_table = parsed_config.get("table", "unknown_table")
        self.table_name = re.sub(r"[^a-zA-Z0-9_]", "_", Helper.parse_table_name(raw_table))

    def apply_transformations(self) -> DataFrame:
        logger.info(f"Applying DataFrame transformations for table '{self.table_name}'...")
        
        joined_dfs: Dict[str, DataFrame] = {}
        
        current_df = self.df.alias(self.main_alias)
        joined_dfs[self.main_alias] = current_df

        joins_meta = self.parsed_config.get("joins", [])
        if joins_meta:
            current_df, joined_dfs = self._apply_joins(current_df, joins_meta, joined_dfs)

        rules_meta = self.parsed_config.get("rules", [])
        if rules_meta:
            current_df = self._apply_rules(current_df, rules_meta, joined_dfs)

        logger.info(f"Successfully applied transformations for '{self.table_name}'.")
        return current_df

    def _apply_joins(
        self, df: DataFrame, joins_meta: List[Dict[str, Any]], joined_dfs: Dict[str, DataFrame]
    ) -> Tuple[DataFrame, Dict[str, DataFrame]]:
        for j in joins_meta:
            if not isinstance(j, dict):
                continue

            target_table = j.get("table")
            tbl_alias = j.get("alias") or target_table
            how = j.get("how", "left")

            right_df = self.spark.table(target_table)
            if tbl_alias:
                right_df = right_df.alias(tbl_alias)

            joined_dfs[tbl_alias] = right_df

            if j.get("broadcast"):
                right_df = F.broadcast(right_df)

            on_clause = j.get("on_clause")
            if on_clause:
                df = df.join(right_df, on=F.expr(on_clause), how=how)
            else:
                df = df.join(right_df, how=how)

        return df, joined_dfs

    def _apply_rules(
        self, df: DataFrame, rules_meta: List[Dict[str, Any]], joined_dfs: Dict[str, DataFrame]
    ) -> DataFrame:
        already_selected_cols: Set[str] = set()
        raw_except_list: List[str] = []
        select_rest_enabled = False

        for rule in rules_meta:
            if not isinstance(rule, dict):
                continue

            if "column" in rule and "expression" in rule:
                raw_col = rule["column"].strip()
                # 1. شيل الـ Prefix عشان اسم العمود الناتج بيبقى بدون Prefix
                target_col = self._strip_prefix(raw_col)
                expr_str = self._sanitize_expression_quotes(rule["expression"].strip())
                
                df = df.withColumn(target_col, F.expr(expr_str))
                
                # تسجيل العمود باسمه الصريح وباسمه قبل القطع
                already_selected_cols.add(target_col)
                already_selected_cols.add(raw_col)

            elif "run" in rule:
                run_expr = rule["run"].strip()
                if run_expr.endswith(".*"):
                    col_prefix = run_expr[:-2]
                    matching_cols = [c for c in df.columns if c == col_prefix or c.endswith(f".{col_prefix}")]
                    if matching_cols:
                        target_col = matching_cols[0]
                        schema_field = next((f for f in df.schema.fields if f.name == target_col), None)
                        if schema_field and hasattr(schema_field.dataType, "names"):
                            for field in schema_field.dataType.names:
                                out_col = f"{self._strip_prefix(col_prefix)}_{field}"
                                df = df.withColumn(out_col, F.col(f"`{target_col}`.{field}"))
                                already_selected_cols.add(out_col)

            elif "select_the_rest" in rule:
                rest_cfg = rule["select_the_rest"]
                select_rest_enabled = rest_cfg.get("enable", False)
                raw_except_list = rest_cfg.get("except", [])

        if select_rest_enabled:
            df = self.resolve_select_the_rest(
                df=df,
                except_list=raw_except_list,
                already_selected_cols=already_selected_cols,
                joined_dfs=joined_dfs,
            )

        return df

    def resolve_select_the_rest(
    self,
    df: DataFrame,
    except_list: List[str],
    already_selected_cols: Set[str],
    joined_dfs: Dict[str, DataFrame],
    ) -> DataFrame:
        excluded_qualified: Set[str] = set()
        excluded_simple: Set[str] = set()

        # 1. تفكيك قائمة الاستثناءات
        for item in except_list:
            item_clean = item.replace("`", "").strip()
            if "." in item_clean:
                excluded_qualified.add(item_clean)
            else:
                excluded_simple.add(item_clean)

        # 2. تحديد بالضبط الأعمدة المطلوب إسقاطها بحسب جدولها (Ref-based Drop)
        # مسح أي عمود مستثنى جاي من Right Tables (مثل s.status أو s.nickname)
        for qual in list(excluded_qualified):
            alias_part, col_part = qual.split(".", 1)
            if alias_part in joined_dfs and alias_part != self.main_alias:
                right_df = joined_dfs[alias_part]
                if col_part in right_df.columns:
                    try:
                        df = df.drop(right_df[col_part])
                    except Exception as e:
                        logger.warning(f"Failed to drop right-table column {qual}: {e}")

        # 3. بناء الـ Selected Columns مع التمييز بين c.status و s.status
        main_df = joined_dfs.get(self.main_alias)
        cols_to_select = []
        seen_output_names = set()

        for col_name in df.columns:
            plain_name = self._strip_prefix(col_name)

            # فحص هل هذا العمود بالذات هو القادم من الـ Main Table (c)؟
            is_from_main = main_df is not None and col_name in main_df.columns

            # حالة 1: استثناء c.status بشكل محدد من الجدول الرئيسي
            if is_from_main and f"{self.main_alias}.{plain_name}" in excluded_qualified:
                continue

            # حالة 2: استثناء الأعمدة المكتوبة بدون Alias (Simple Except)
            if col_name in excluded_simple or plain_name in excluded_simple:
                continue

            # حالة 3: الأولوية للأعمدة المنشأة في الـ Rules
            if col_name in already_selected_cols or plain_name in already_selected_cols:
                if plain_name not in seen_output_names:
                    cols_to_select.append(col_name)
                    seen_output_names.add(plain_name)
                continue

            # حالة 4: إضافة الأعمدة المتبقية مع ضمان عدم تكرار الاسم في الناتج
            if plain_name not in seen_output_names:
                if is_from_main:
                    cols_to_select.append(main_df[col_name])
                else:
                    cols_to_select.append(col_name)
                seen_output_names.add(plain_name)

        return df.select(*cols_to_select)

    def _strip_prefix(self, col_ref: str) -> str:
        clean = col_ref.replace("`", "").strip()
        return clean.split(".")[-1]

    def _sanitize_expression_quotes(self, expr: str) -> str:
        return re.sub(r'\"([^\"]*)\"', r"'\1'", expr)