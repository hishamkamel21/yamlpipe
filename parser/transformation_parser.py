import copy
import logging
import re
from typing import Dict, Any, List, Set, Tuple, Generator
from yamlpipe.registry.transformation_registry import TransformationRegistry
from yamlpipe.utility.helper import Helper

logger = logging.getLogger("TransformationParser")


class TransformationParser:

    # Regex pattern to match ${column}, ${col}, and ${c} (case-insensitive)
    PLACEHOLDER_PATTERN = re.compile(r"\$\{(column|col|c)\}", re.IGNORECASE)

    @classmethod
    def parse(cls, raw_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parses raw YAML transformation configuration into an intermediate structure.
        Validates job dependency lineage and extracts SQL expressions.
        Supports single-column and multi-column bulk rule definitions.
        """
        if not isinstance(raw_config, dict):
            raise TypeError(f"[TransformationParser Error] Expected dict, got {type(raw_config).__name__}")

        raw_table = raw_config.get("table") or raw_config.get("table_name", "unknown_table")
        cleaned_table_name = Helper.parse_table_name(raw_table)
        raw_jobs = raw_config.get("jobs", [])

        parsed_jobs: Dict[str, Dict[str, Any]] = {}
        defined_job_names: Set[str] = set()

        for idx, job_entry in enumerate(raw_jobs):
            if not isinstance(job_entry, dict) or not job_entry:
                continue

            # Extract job_name and job_body (e.g., 'base': {...})
            job_name, job_details = next(iter(job_entry.items()))

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

            # Parse rules inside the job
            exprs: List[str] = []
            explicitly_handled_cols: Set[str] = set()
            select_the_rest_config = None

            for rule in job_details.get("rules", []):
                if not isinstance(rule, dict):
                    logger.warning(f"Skipping non-dict rule in job '{job_name}': {rule}")
                    continue

                if "select_the_rest" in rule:
                    select_the_rest_config = cls._parse_select_the_rest(rule["select_the_rest"])
                    continue

                # Expand rules (handles single 'column' vs multi 'columns')
                for expanded_rule, col_name in cls._expand_rule(rule):
                    try:
                        explicitly_handled_cols.add(col_name)
                        expr_str = TransformationRegistry.build_rule_expr(expanded_rule)
                        exprs.append(expr_str)
                    except Exception as e:
                        logger.error(
                            f"Failed to build transformation expression for column '{col_name}' "
                            f"in job '{job_name}'. Rule details: {expanded_rule}"
                        )
                        raise RuntimeError(
                            f"[TransformationParser Error] Job '{job_name}', Column '{col_name}': {str(e)}"
                        ) from e

            # Update 'except' list inside select_the_rest to include explicitly handled columns automatically
            if select_the_rest_config and select_the_rest_config.get("enable"):
                existing_except = set(select_the_rest_config.get("except", []))
                merged_except = sorted(list(existing_except.union(explicitly_handled_cols)))
                select_the_rest_config["except"] = merged_except

            parsed_jobs[job_name] = {
                "depend_on": source_step,
                "exprs": exprs,
                "explicitly_handled_cols": sorted(list(explicitly_handled_cols)),
                "select_the_rest": select_the_rest_config
            }

            defined_job_names.add(job_name)

        return {
            "table": cleaned_table_name,
            "jobs": parsed_jobs
        }

    @classmethod
    def _expand_rule(cls, rule: Dict[str, Any]) -> Generator[Tuple[Dict[str, Any], str], None, None]:
        """
        Yields (single_column_rule, column_name) pairs by expanding bulk 'columns' lists 
        and replacing ${column}, ${col}, or ${c} placeholders dynamically.
        """
        single_col = rule.get("column")
        multi_cols = rule.get("columns")

        # Format 1: Single column rule
        if single_col:
            target_col = str(single_col).strip()
            rule_copy = copy.deepcopy(rule)
            rule_copy = cls._replace_column_placeholders(rule_copy, target_col)
            yield rule_copy, target_col

        # Format 2: Multi column bulk rule
        elif multi_cols:
            if not isinstance(multi_cols, list) or not multi_cols:
                logger.warning(f"Rule with 'columns' key missing valid column list: {rule}")
                return

            for col in multi_cols:
                if not col or not str(col).strip():
                    continue
                target_col = str(col).strip()
                
                # Copy rule, assign target column, and remove bulk 'columns' key
                rule_copy = copy.deepcopy(rule)
                rule_copy.pop("columns", None)
                rule_copy["column"] = target_col
                
                # Replace ${column}, ${col}, and ${c} placeholders
                rule_copy = cls._replace_column_placeholders(rule_copy, target_col)
                yield rule_copy, target_col

        else:
            logger.warning(f"Rule skipped because it lacks both 'column' and 'columns' targets: {rule}")

    @classmethod
    def _replace_column_placeholders(cls, item: Any, column_name: str) -> Any:
        """
        Recursively replaces '${column}', '${col}', and '${c}' occurrences with the target column name.
        """
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
            raise TypeError(
                f"[TransformationParser Error] Invalid type for 'except' in select_the_rest: {type(raw_except)}"
            )

        return {
            "enable": True,
            "except": except_list
        }