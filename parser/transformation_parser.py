import logging
from typing import Dict, Any, List
from yamlpipe.registries.transformation_registry import TransformationRegistry
from yamlpipe.utility.helper import Helper

logger = logging.getLogger("TransformationParser")


class TransformationParser:

    @classmethod
    def parse(cls, raw_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parses raw YAML transformation configuration into an intermediate structure.
        Validates job dependency lineage and extracts SQL expressions.
        """
        if not isinstance(raw_config, dict):
            raise TypeError(f"[TransformationParser Error] Expected dict, got {type(raw_config).__name__}")

        raw_table = raw_config.get("table") or raw_config.get("table_name", "unknown_table")
        cleaned_table_name = Helper.parse_table_name(raw_table)
        raw_jobs = raw_config.get("jobs", [])

        parsed_jobs: Dict[str, Dict[str, Any]] = {}
        defined_job_names = set()

        for idx, job_entry in enumerate(raw_jobs):
            if not isinstance(job_entry, dict) or not job_entry:
                continue

            # Extract job_name and job_body (e.g., 'first_stage': {...})
            job_name, job_details = next(iter(job_entry.items()))

            if job_name in defined_job_names:
                raise ValueError(f"Duplicate job name detected: '{job_name}'")

            source_step = job_details.get("depend_on")

            # Validate Dependency Chain
            if idx > 0:
                if not source_step:
                    raise ValueError(f"Job '{job_name}' (Step #{idx + 1}) must specify 'depend_on'.")
                if source_step not in defined_job_names:
                    raise ValueError(f"Job '{job_name}' depends on unknown job '{source_step}'.")

            # Parse rules inside the job
            exprs = []
            explicitly_handled_cols = set()
            select_the_rest_config = None

            for rule in job_details.get("rules", []):
                if "select_the_rest" in rule:
                    select_the_rest_config = cls._parse_select_the_rest(rule["select_the_rest"])
                    continue

                column_name = rule.get("column")
                if not column_name:
                    continue

                explicitly_handled_cols.add(column_name)
                expr_str = TransformationRegistry.build_rule_expr(rule)
                exprs.append(expr_str)

            # Update 'except' list inside select_the_rest to include explicitly handled columns automatically
            if select_the_rest_config and select_the_rest_config.get("enable"):
                existing_except = set(select_the_rest_config.get("except", []))
                merged_except = list(existing_except.union(explicitly_handled_cols))
                select_the_rest_config["except"] = merged_except

            parsed_jobs[job_name] = {
                "depend_on": source_step,
                "exprs": exprs,
                "explicitly_handled_cols": list(explicitly_handled_cols),
                "select_the_rest": select_the_rest_config
            }

            defined_job_names.add(job_name)

        return {
            "table": cleaned_table_name,
            "jobs": parsed_jobs
        }

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
            raise TypeError(f"Invalid type for 'except' in select_the_rest: {type(raw_except)}")

        return {
            "enable": True,
            "except": except_list
        }