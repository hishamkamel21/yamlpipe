import os
import json
import hashlib
import yaml
import logging
from typing import Dict, Any, List

from yamlpipe.parser.transformation_parser import TransformationParser
from yamlpipe.parser.quality_checks_parser import QualityChecksParser
from yamlpipe.core.vars_manager import VariablesManager

logger = logging.getLogger("CacheManager")


class CacheManager:

    @staticmethod
    def _compute_md5(file_path: str) -> str:
        """Calculates MD5 hash of a file to detect content changes."""
        hasher = hashlib.md5()
        with open(file_path, "rb") as f:
            buf = f.read()
            hasher.update(buf)
        return hasher.hexdigest()

    @staticmethod
    def _load_hashes(hash_file_path: str) -> Dict[str, Any]:
        """Reads parsed_hash.yml cleanly."""
        if not os.path.exists(hash_file_path):
            return {"transformation_rules": {}, "quality_gate": {}, "vars": {}}
        try:
            with open(hash_file_path, "r", encoding="utf-8") as f:
                content = yaml.safe_load(f) or {}
                content.setdefault("transformation_rules", {})
                content.setdefault("quality_gate", {})
                content.setdefault("vars", {})
                return content
        except Exception as e:
            logger.warning(f"Could not read hash file at {hash_file_path}, resetting cache index. Error: {e}")
            return {"transformation_rules": {}, "quality_gate": {}, "vars": {}}

    @staticmethod
    def _save_hashes(hash_file_path: str, hash_data: Dict[str, Any]) -> None:
        """Writes updated hash definitions back to parsed_hash.yml."""
        try:
            with open(hash_file_path, "w", encoding="utf-8") as f:
                yaml.dump(hash_data, f, default_flow_style=False, sort_keys=False)
        except Exception as e:
            logger.error(f"Failed to write hash index to '{hash_file_path}': {str(e)}")

    @classmethod
    def get_or_compile(
        cls, 
        project_root: str, 
        subfolder: str, 
        selector: str
    ) -> Dict[str, Any]:
        """
        Orchestrates cache validation, dependency graph checks, and dynamic compilation.
        """
        raw_yaml_path = cls._resolve_yaml_file(project_root, subfolder, selector)
        parsed_dir = os.path.join(project_root, "parsed", subfolder)
        os.makedirs(parsed_dir, exist_ok=True)

        json_cache_path = os.path.join(parsed_dir, f"{selector}.json")
        hash_file_path = os.path.join(project_root, "parsed", "parsed_hash.yml")

        current_hash = cls._compute_md5(raw_yaml_path)
        all_hashes = cls._load_hashes(hash_file_path)
        cached_hash = all_hashes.get(subfolder, {}).get(selector)

        # 1. Base File Hash Check
        if cached_hash == current_hash and os.path.exists(json_cache_path):
            try:
                with open(json_cache_path, "r", encoding="utf-8") as f:
                    cached_data = json.load(f)

                # 2. Check Variable Dependencies Hash Consistency
                var_dependencies = cached_data.get("ContainVarsFrom", []) if isinstance(cached_data, dict) else []
                dependencies_valid = True

                for var_selector in var_dependencies:
                    var_yaml_path = cls._resolve_yaml_file(project_root, "vars", var_selector)
                    current_var_hash = cls._compute_md5(var_yaml_path)
                    cached_var_hash = all_hashes.get("vars", {}).get(var_selector)

                    if current_var_hash != cached_var_hash:
                        logger.info(
                            f"Cache MISS: Dependent variable '{var_selector}' changed for '{subfolder}/{selector}'."
                        )
                        dependencies_valid = False
                        break

                if dependencies_valid:
                    logger.info(f"Cache HIT: Returning compiled JSON for '{subfolder}/{selector}'")
                    return cached_data

            except Exception as e:
                logger.warning(f"Corrupted cache JSON detected at '{json_cache_path}'. Recompiling... Error: {e}")

        # Cache MISS: Reparse and Recompile
        logger.info(f"Cache MISS: Compiling YAML rule '{subfolder}/{selector}'...")

        with open(raw_yaml_path, "r", encoding="utf-8") as f:
            raw_config = yaml.safe_load(f) or {}

        # Resolve variables and track dependencies
        referenced_vars = []
        if subfolder in ("transformation_rules", "quality_gate"):
            raw_config, referenced_vars_set = VariablesManager.extract_vars_and_parse(raw_config, project_root)
            referenced_vars = sorted(list(referenced_vars_set))

        # Compile Rule via registered Parsers
        if subfolder == "vars":
            compiled_result = raw_config
        elif subfolder == "transformation_rules":
            compiled_result = TransformationParser.parse(raw_config)
        elif subfolder == "quality_gate":
            compiled_result = QualityChecksParser.parse_quality_checks(raw_config)
        else:
            raise ValueError(f"Unsupported rule type subfolder: '{subfolder}'")

        # Inject ContainVarsFrom dependency list into top-level compiled output
        if subfolder in ("transformation_rules", "quality_gate") and isinstance(compiled_result, dict):
            compiled_result["ContainVarsFrom"] = referenced_vars

        # Save JSON Cache
        with open(json_cache_path, "w", encoding="utf-8") as f:
            json.dump(compiled_result, f, indent=4, ensure_ascii=False)

        # Update parsed_hash.yml
        if subfolder not in all_hashes:
            all_hashes[subfolder] = {}
        all_hashes[subfolder][selector] = current_hash

        # Update variable hashes for tracked dependencies
        if subfolder == "vars":
            all_hashes["vars"][selector] = current_hash
        else:
            for var_selector in referenced_vars:
                try:
                    var_yaml_path = cls._resolve_yaml_file(project_root, "vars", var_selector)
                    all_hashes["vars"][var_selector] = cls._compute_md5(var_yaml_path)
                except FileNotFoundError:
                    logger.warning(f"Referenced variable file '{var_selector}' not found on disk.")

        cls._save_hashes(hash_file_path, all_hashes)

        logger.info(f"Successfully compiled and cached '{selector}' to '{json_cache_path}'")
        return compiled_result

    @staticmethod
    def _resolve_yaml_file(project_root: str, subfolder: str, selector: str) -> str:
        clean_selector = selector.rsplit(".", 1)[0] if selector.endswith((".yaml", ".yml")) else selector
        
        target_dir = (
            os.path.join(project_root, "vars")
            if subfolder == "vars"
            else os.path.join(project_root, "yaml_configs", subfolder)
        )

        candidates = [
            os.path.join(target_dir, f"{clean_selector}.yaml"),
            os.path.join(target_dir, f"{clean_selector}.yml")
        ]

        for candidate in candidates:
            if os.path.exists(candidate):
                return candidate

        raise FileNotFoundError(
            f"Could not find source YAML for '{clean_selector}' in '{subfolder}'.\n"
            f"Searched paths:\n" + "\n".join(f"  - {c}" for c in candidates)
        )