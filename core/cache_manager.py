import os
import json
import hashlib
import yaml
import logging
from typing import Dict, Any, Tuple

from yamlpipe.parser.transformation_parser import TransformationParser
from yamlpipe.parser.quality_checks_parser import QualityChecksParser

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
            return {"transformation_rules": {}, "quality_gate": {}}
        try:
            with open(hash_file_path, "r", encoding="utf-8") as f:
                content = yaml.safe_load(f) or {}
                # Ensure base keys exist
                content.setdefault("transformation_rules", {})
                content.setdefault("quality_gate", {})
                return content
        except Exception as e:
            logger.warning(f"Could not read hash file at {hash_file_path}, resetting cache index. Error: {e}")
            return {"transformation_rules": {}, "quality_gate": {}}

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
        Orchestrates cache validation, static compilation, and JSON cache retrieval.
        
        Args:
            project_root: Absolute path to project root.
            subfolder: 'transformation_rules' or 'quality_gate'.
            selector: Clean config name without extension (e.g., 'customer_checks').
        """
        # 1. Resolve Path Directories
        raw_yaml_path = cls._resolve_yaml_file(project_root, subfolder, selector)
        parsed_dir = os.path.join(project_root, "parsed", subfolder)
        os.makedirs(parsed_dir, exist_ok=True)

        json_cache_path = os.path.join(parsed_dir, f"{selector}.json")
        hash_file_path = os.path.join(project_root, "parsed", "parsed_hash.yml")

        # 2. Check File Hashes
        current_hash = cls._compute_md5(raw_yaml_path)
        all_hashes = cls._load_hashes(hash_file_path)
        cached_hash = all_hashes.get(subfolder, {}).get(selector)

        # 3. Cache HIT: Return cached JSON directly
        if cached_hash == current_hash and os.path.exists(json_cache_path):
            logger.info(f"Cache HIT: Returning compiled JSON for '{subfolder}/{selector}'")
            try:
                with open(json_cache_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Corrupted cache JSON detected at '{json_cache_path}'. Recompiling... Error: {e}")

        # 4. Cache MISS: Parse raw YAML -> Compile -> Cache JSON -> Update Hashes
        logger.info(f"Cache MISS: Compiling YAML rule '{subfolder}/{selector}'...")

        with open(raw_yaml_path, "r", encoding="utf-8") as f:
            raw_config = yaml.safe_load(f) or {}

        # Delegate parsing based on subfolder type
        if subfolder == "transformation_rules":
            compiled_result = TransformationParser.parse(raw_config)
        elif subfolder == "quality_gate":
            compiled_result = QualityChecksParser.parse_quality_checks(raw_config)
        else:
            raise ValueError(f"Unsupported rule type subfolder: '{subfolder}'")

        # Write Parsed Output to JSON
        with open(json_cache_path, "w", encoding="utf-8") as f:
            json.dump(compiled_result, f, indent=4, ensure_ascii=False)

        # Update and persist hash registry
        if subfolder not in all_hashes:
            all_hashes[subfolder] = {}
        all_hashes[subfolder][selector] = current_hash
        cls._save_hashes(hash_file_path, all_hashes)

        logger.info(f"Successfully compiled and cached '{selector}' to '{json_cache_path}'")
        return compiled_result

    @staticmethod
    def _resolve_yaml_file(project_root: str, subfolder: str, selector: str) -> str:
        """Finds raw YAML file (.yaml or .yml) inside source configs."""
        clean_selector = selector.rsplit(".", 1)[0] if selector.endswith((".yaml", ".yml")) else selector
        target_dir = os.path.join(project_root, "yaml_configs", subfolder)

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