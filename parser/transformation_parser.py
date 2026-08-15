import logging
import re
import copy  # <--- Added missing import
from typing import Dict, Any, List, Set, Tuple, Generator
from yamlpipe.registry.transformation_registry import TransformationRegistry
from yamlpipe.utility.helper import Helper

logger = logging.getLogger("TransformationParser")


class TransformationParser:

    # Match explicit column variables: ${col} or ${column}
    PLACEHOLDER_PATTERN = re.compile(r"\$\{(column|col)\}", re.IGNORECASE)

    @classmethod
    def parse(cls, raw_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parses raw YAML configuration into job execution specs for PySpark SQL execution.
        """
        if not isinstance(raw_config, dict):
            raise TypeError(f"[TransformationParser Error] Expected dict, got {type(raw_config).__name__}")

        raw_table = raw_config.get("table") or raw_config.get("table_name", "unknown_table")
        cleaned_table_name = Helper.parse_table_name(raw_table)
        main_alias = raw_config.get("alias", "main_tbl")
        raw_jobs = raw_config.get("jobs", [])

        if not isinstance(raw_jobs, list):
            raise TypeError(f"[TransformationParser Error] 'jobs' must be a list, got {type(raw_jobs).__name__}")

        parsed_jobs: Dict[str, Dict[str, Any]] = {}
        defined_job_names: Set[str] = set()

        for idx, job_entry in enumerate(raw_jobs):
            if not isinstance(job_entry, dict) or not job_entry:
                continue

            job_name, job_details = next(iter(job_entry.items()))

            if not isinstance(job_details, dict):
                raise TypeError(f"[TransformationParser Error] Details for job '{job_name}' must be a dict.")

            if job_name in defined_job_names:
                raise ValueError(f"[TransformationParser Error] Duplicate job name detected: '{job_name}'")

            source_step = job_details.get("depend_on")

            # Validate Dependency Chain
            if idx > 0:
                if not source_step:
                    raise ValueError(
                        f"[TransformationParser Error] Job '{job_name}' (Step #{idx + 1}) must specify 'depend_on'."
                    )
                if source_step not in defined_job_names:
                    raise ValueError(
                        f"[TransformationParser Error] Job '{job_name}' depends on unknown job '{source_step}'."
                    )

            exprs: List[str] = []
            explicitly_handled_cols: Set[str] = set()
            select_the_rest_config = None

            # Parse Joins
            parsed_joins: List[Dict[str, Any]] = []
            joins_list = job_details.get("joins", [])
            if isinstance(joins_list, list):
                for join_cfg in joins_list:
                    if isinstance(join_cfg, dict):
                        join_sql = TransformationRegistry.build_join_clause(join_cfg)
                        parsed_joins.append({
                            "sql": join_sql,
                            "table": join_cfg.get("table"),
                            "alias": join_cfg.get("alias")
                        })

            # Parse Rules
            rules_list = job_details.get("rules", [])
            if isinstance(rules_list, list):
                for rule in rules_list:
                    if not isinstance(rule, dict):
                        continue

                    if "select_the_rest" in rule:
                        select_the_rest_config = cls._parse_select_the_rest(rule["select_the_rest"])
                        continue

                    if "run" in rule:
                        run_exprs = cls._parse_run_directive(rule["run"], job_name)
                        exprs.extend(run_exprs)
                        continue

                    for expanded_rule, col_name in cls._expand_rule(rule, main_alias):
                        try:
                            explicitly_handled_cols.add(col_name)
                            expr_str = TransformationRegistry.build_rule_expr(expanded_rule)
                            if expr_str and expr_str.strip():
                                exprs.append(expr_str.strip())
                        except Exception as e:
                            raise RuntimeError(
                                f"[TransformationParser Error] Job '{job_name}', Column '{col_name}': {str(e)}"
                            ) from e

            if select_the_rest_config and select_the_rest_config.get("enable"):
                existing_except = set(select_the_rest_config.get("except", []))
                merged_except = sorted(list(existing_except.union(explicitly_handled_cols)))
                select_the_rest_config["except"] = merged_except

            parsed_jobs[job_name] = {
                "depend_on": source_step,
                "exprs": exprs,
                "joins": parsed_joins,
                "explicitly_handled_cols": sorted(list(explicitly_handled_cols)),
                "select_the_rest": select_the_rest_config
            }

            defined_job_names.add(job_name)

        return {
            "table": cleaned_table_name,
            "alias": main_alias,
            "jobs": parsed_jobs,
            "ContainVarsFrom": []
        }

    @classmethod
    def _parse_run_directive(cls, run_val: Any, job_name: str) -> List[str]:
        raw_statements: List[str] = []
        if isinstance(run_val, str) and run_val.strip():
            raw_statements.append(run_val.strip())
        elif isinstance(run_val, list):
            for stmt in run_val:
                if isinstance(stmt, str) and stmt.strip():
                    raw_statements.append(stmt.strip())
        elif isinstance(run_val, dict):
            expr = run_val.get("expr") or run_val.get("expression")
            if expr and isinstance(expr, str):
                raw_statements.append(expr.strip())
        return raw_statements

    @classmethod
    def _expand_rule(cls, rule: Dict[str, Any], table_alias: str = "") -> Generator[Tuple[Dict[str, Any], str], None, None]:
        single_col = rule.get("column")
        multi_cols = rule.get("columns")

        cols_to_process = []
        if single_col:
            cols_to_process.append(str(single_col).strip())
        elif multi_cols and isinstance(multi_cols, list):
            cols_to_process.extend([str(c).strip() for c in multi_cols if c and str(c).strip()])

        for col in cols_to_process:
            # Format alias prefixing
            qualified_col = f"{table_alias}.{col}" if table_alias and not col.startswith(f"{table_alias}.") else col

            rule_copy = copy.deepcopy(rule)
            rule_copy.pop("columns", None)
            rule_copy["column"] = qualified_col
            rule_copy = cls._replace_column_placeholders(rule_copy, col)
            yield rule_copy, qualified_col

    @classmethod
    def _replace_column_placeholders(cls, item: Any, column_name: str) -> Any:
        if isinstance(item, str):
            return cls.PLACEHOLDER_PATTERN.sub(column_name, item)
        elif isinstance(item, dict):
            return {k: cls._replace_column_placeholders(v, column_name) for k, v in item.items()}
        elif isinstance(item, list):
            return [cls._replace_column_placeholders(elem, column_name) for elem in item]
        return item

    @staticmethod
    def _parse_select_the_rest(config: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(config, dict) or not config.get("enable", False):
            return {"enable": False, "except": []}

        raw_except = config.get("except", [])
        if raw_except is None:
            except_list = []
        elif isinstance(raw_except, str):
            except_list = [raw_except]
        elif isinstance(raw_except, list):
            except_list = raw_except
        else:
            raise TypeError(f"Invalid type for 'except': {type(raw_except)}")

        return {
            "enable": True,
            "except": except_list,
            "include_joined_tables": config.get("include_joined_tables", True)
        }