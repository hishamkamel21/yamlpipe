import os
import re
import yaml
import logging
from typing import Any , Optional
from pyspark.sql.functions import current_timestamp

logger = logging.getLogger("Helper")



class Helper:

    @staticmethod
    def find_project_root(explicit_project_dir: Optional[str] = None) -> str:
        """
        Dynamically locates the active project directory root.
        
        Resolution Priority:
        1. Explicitly provided path (if project.yml exists or directory exists).
        2. Upward hierarchy search starting from CWD looking for project.yml.
        3. Single valid subdirectory containing project.yml relative to CWD.
        4. Fallback to current working directory (CWD).
        """
        cwd = os.path.abspath(os.getcwd())

        # 1. Handle explicit project directory argument
        if explicit_project_dir:
            resolved_path = (
                explicit_project_dir
                if os.path.isabs(explicit_project_dir)
                else os.path.abspath(os.path.join(cwd, explicit_project_dir))
            )

            if os.path.exists(os.path.join(resolved_path, "project.yml")):
                return resolved_path
            elif os.path.exists(resolved_path):
                logger.warning(
                    f"Directory '{resolved_path}' specified, but 'project.yml' was not found."
                )
                return resolved_path
            else:
                raise FileNotFoundError(
                    f"Specified project directory does not exist: {resolved_path}"
                )

        # 2. Search upward in directory hierarchy for project.yml
        current_dir = cwd
        while True:
            candidate_config = os.path.join(current_dir, "project.yml")
            if os.path.exists(candidate_config):
                try:
                    with open(candidate_config, "r", encoding="utf-8") as f:
                        config = yaml.safe_load(f)
                        return config.get("project", {}).get("project_dir", current_dir)
                except Exception as e:
                    logger.warning(
                        f"Could not parse project.yml at {candidate_config}: {str(e)}"
                    )
                    return current_dir

            parent_dir = os.path.dirname(current_dir)
            if parent_dir == current_dir:  # Filesystem root reached
                break
            current_dir = parent_dir

        # 3. Check direct subdirectories for a single project root
        try:
            subdirs = [
                os.path.join(cwd, d)
                for d in os.listdir(cwd)
                if os.path.isdir(os.path.join(cwd, d)) and not d.startswith((".", "_"))
            ]

            projects_found = [
                d for d in subdirs if os.path.exists(os.path.join(d, "project.yml"))
            ]

            if len(projects_found) == 1:
                logger.info(
                    f"Dynamically resolved project root to: '{projects_found[0]}'"
                )
                return projects_found[0]

            elif len(projects_found) > 1:
                project_names = [os.path.basename(p) for p in projects_found]
                raise RuntimeError(
                    f"Multiple projects detected in subdirectories: {project_names}. "
                    f"Please specify 'project_dir' explicitly."
                )

        except Exception as e:
            if isinstance(e, RuntimeError):
                raise e
            logger.debug(f"Subdirectory search skipped: {e}")

        # 4. Default fallback
        return cwd

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
    def clean_multiline_sql(sql_expr: Any) -> str:
        """
        Cleans and normalizes multiline YAML strings (e.g. from literal block scalar '|')
        into a single-line or clean Spark SQL expression safely.
        """
        if sql_expr is None:
            return ""
        
        if isinstance(sql_expr, list):
            sql_expr = " AND ".join([str(x) for x in sql_expr if x])
        elif not isinstance(sql_expr, str):
            sql_expr = str(sql_expr)

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