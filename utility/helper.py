import os
import re
import logging
from typing import Any
from pyspark.sql.functions import current_timestamp

logger = logging.getLogger("Helper")


class Helper:

    @staticmethod
    def parse_table_name(table_cfg: Any) -> str:
        """
        Parses a table string or dictionary object (catalog/schema/table) 
        into a fully qualified table name string (e.g., 'catalog.schema.table').
        """
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
    def clean_multiline_sql(sql_expr: str) -> str:
        """
        Cleans and normalizes multiline YAML strings (e.g. from literal block scalar '|')
        into a single-line or clean Spark SQL expression.
        """
        if not sql_expr or not isinstance(sql_expr, str):
            return sql_expr
        
        # Replace newlines and multiple whitespaces with a single space
        cleaned = re.sub(r'\s+', ' ', sql_expr).strip()
        return cleaned

    @staticmethod 
    def _get_date_formats_expr(column: str) -> str:
        formats = [
            "yyyy-MM-dd",        
            "MM/dd/yyyy",          
            "dd-MM-yyyy",          
            "yyyy/MM/dd",          
            "dd/MM/yyyy",
            "yyyyMMdd",          
            "MM-dd-yyyy",   
            "dd MMM yyyy",        
            "dd MMMM yyyy"        
        ]
        
        to_date_lines = [f'  to_date({column}, "{f}")' for f in formats]
        inner_expr = ",\n".join(to_date_lines)
        
        return f"coalesce(\n{inner_expr}\n)"

    @staticmethod 
    def _get_timestamp_formats_expr(column: str) -> str:
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
            "dd MMMM yyyy HH:mm:ss"
        ]
        
        to_timestamp_lines = [f'  to_timestamp({column}, "{f}")' for f in formats]
        inner_expr = ",\n".join(to_timestamp_lines)
        
        return f"coalesce(\n{inner_expr}\n)"