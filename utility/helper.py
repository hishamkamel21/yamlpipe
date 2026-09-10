import os
import re
import yaml
import logging
from typing import Any, Optional
from pyspark.sql.functions import current_timestamp

# Auto-initialized logger - writes to console & project_root/pipe.log instantly
from yamlpipe.utility.logger import get_logger, find_project_root

logger = get_logger("Helper")


class Helper:

    @staticmethod
    def find_project_root(explicit_project_dir: Optional[str] = None) -> str:
        """
        Delegates project root discovery to logger core finder.
        """
        return find_project_root(explicit_project_dir)

    @staticmethod
    def parse_table_name(table_cfg: Any) -> str:
        try:
            if isinstance(table_cfg, str):
                cleaned_name = table_cfg.strip()
                if not cleaned_name:
                    raise ValueError("Table name string is empty or contains only whitespace.")
                return cleaned_name

            elif isinstance(table_cfg, dict):
                catalog = table_cfg.get("catalog")
                schema = table_cfg.get("schema")
                table = table_cfg.get("table")

                parts = [str(p).strip() for p in [catalog, schema, table] if p and str(p).strip()]
                
                if not parts:
                    raise ValueError(
                        f"Table configuration dictionary {table_cfg} contains no valid components "
                        f"('catalog', 'schema', or 'table')."
                    )
                return ".".join(parts)

            raise TypeError(
                f"Unsupported table configuration type: Expected 'str' or 'dict', "
                f"got '{type(table_cfg).__name__}' with value: {table_cfg}"
            )

        except Exception as e:
            error_msg = f"[Table Parsing Error] Failed to parse table reference from config '{table_cfg}': {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg) from e

    @staticmethod
    def clean_multiline_sql(sql_expr: Any) -> str:
        if sql_expr is None:
            return ""
        
        if isinstance(sql_expr, list):
            sql_expr = " AND ".join([str(x) for x in sql_expr if x])
        elif not isinstance(sql_expr, str):
            sql_expr = str(sql_expr)

        cleaned = re.sub(r'\s+', ' ', sql_expr).strip()
        return cleaned

    @staticmethod
    def _get_date_formats_expr(column: str, use_try_fn: bool = True) -> str:
        formats = [
            "yyyy-MM-dd",
            "MM/dd/yyyy",
            "dd-MM-yyyy",
            "yyyy/MM/dd",
            "dd/MM/yyyy",
            "yyyyMMdd",
            "MM-dd-yyyy",
            "dd MMM yyyy",
            "dd MMMM yyyy",
        ]

        fn_name = "try_to_date" if use_try_fn else "to_date"
        date_lines = [f'  {fn_name}({column}, "{f}")' for f in formats]
        inner_expr = ",\n".join(date_lines)

        return f"coalesce(\n{inner_expr}\n)"

    @staticmethod
    def _get_timestamp_formats_expr(column: str, use_try_fn: bool = True) -> str:
        formats = [
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss.SSS",
            "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
            "yyyy-MM-dd'T'HH:mm:ssXXX",
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            "MM/dd/yyyy HH:mm:ss",
            "dd/MM/yyyy HH:mm:ss",
            "yyyy/MM/dd HH:mm:ss",
            "MM/dd/yyyy hh:mm:ss a",
            "dd-MM-yyyy HH:mm:ss",
            "yyyyMMddHHmmss",
            "dd MMM yyyy HH:mm:ss",
            "dd MMMM yyyy HH:mm:ss",
        ]

        fn_name = "try_to_timestamp" if use_try_fn else "to_timestamp"
        ts_lines = [f'  {fn_name}({column}, "{f}")' for f in formats]
        inner_expr = ",\n".join(ts_lines)

        return f"coalesce(\n{inner_expr}\n)"