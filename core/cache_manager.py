import os
import json
import hashlib
import yaml
import logging
import uuid
import threading
from typing import Dict, Any, List
from filelock import FileLock, Timeout

logger = logging.getLogger("CacheManager")


class ReentrantFileLock:
    """
    Process-safe and Thread-safe Re-entrant File Lock keyed by file path.
    Prevents Deadlocks when get_or_compile recurses to fetch dependent variables.
    """
    _thread_local = threading.local()

    def __init__(self, lock_file_path: str, timeout: int = 30):
        self.lock_file_path = lock_file_path
        self.timeout = timeout
        self.lock = FileLock(lock_file_path, timeout=timeout)

    def __enter__(self):
        if not hasattr(self._thread_local, "locks"):
            self._thread_local.locks = {}

        if self.lock_file_path in self._thread_local.locks:
            self._thread_local.locks[self.lock_file_path] += 1
            return self

        self.lock.acquire(timeout=self.timeout)
        self._thread_local.locks[self.lock_file_path] = 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.lock_file_path in self._thread_local.locks:
            self._thread_local.locks[self.lock_file_path] -= 1
            if self._thread_local.locks[self.lock_file_path] == 0:
                del self._thread_local.locks[self.lock_file_path]
                self.lock.release()


class CacheManager:

    @staticmethod
    def _compute_md5(file_path: str) -> str:
        """Calculates MD5 hash of a file to detect content changes with detailed error handling."""
        hasher = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                buf = f.read()
                hasher.update(buf)
            return hasher.hexdigest()
        except FileNotFoundError:
            raise FileNotFoundError(
                f"[CacheManager Error] Hash calculation failed. Target file directly missing: '{file_path}'"
            )
        except PermissionError:
            raise PermissionError(
                f"[CacheManager Error] Permission denied when reading file for MD5 computation: '{file_path}'"
            )
        except Exception as e:
            raise RuntimeError(
                f"[CacheManager Error] Unexpected error computing MD5 for '{file_path}': {type(e).__name__} - {str(e)}"
            )

    @staticmethod
    def _load_hashes(hash_file_path: str) -> Dict[str, Any]:
        """Reads parsed_hash.yml cleanly with defensive checks."""
        if not os.path.exists(hash_file_path):
            return {"transformation_rules": {}, "quality_gate": {}, "vars": {}}
        try:
            with open(hash_file_path, "r", encoding="utf-8") as f:
                content = yaml.safe_load(f) or {}
                if not isinstance(content, dict):
                    logger.warning(
                        f"[CacheManager Warning] Hash file at '{hash_file_path}' contains non-dict structure. Resetting index."
                    )
                    return {"transformation_rules": {}, "quality_gate": {}, "vars": {}}
                
                content.setdefault("transformation_rules", {})
                content.setdefault("quality_gate", {})
                content.setdefault("vars", {})
                return content
        except Exception as e:
            logger.warning(
                f"[CacheManager Warning] Could not read or parse hash index at '{hash_file_path}'. Resetting cache index. Details: {type(e).__name__} - {str(e)}"
            )
            return {"transformation_rules": {}, "quality_gate": {}, "vars": {}}

    @staticmethod
    def _save_hashes_atomic(hash_file_path: str, hash_data: Dict[str, Any]) -> None:
        """Writes updated hash definitions safely using a thread-and-process-safe atomic temp-file replace strategy."""
        unique_id = uuid.uuid4().hex
        temp_hash_path = f"{hash_file_path}.tmp.{os.getpid()}_{unique_id}"
        
        try:
            with open(temp_hash_path, "w", encoding="utf-8") as f:
                yaml.dump(hash_data, f, default_flow_style=False, sort_keys=False)
            os.replace(temp_hash_path, hash_file_path)
        except Exception as e:
            if os.path.exists(temp_hash_path):
                try:
                    os.remove(temp_hash_path)
                except OSError:
                    pass
            logger.error(
                f"[CacheManager Error] Failed to atomically write hash index to '{hash_file_path}'. Details: {type(e).__name__} - {str(e)}"
            )

    @classmethod
    def get_or_compile(
        cls, 
        project_root: str, 
        subfolder: str, 
        selector: str
    ) -> Dict[str, Any]:
        """
        Orchestrates cache validation, dependency graph checks, and dynamic compilation using Per-Resource Locks.
        """
        # Lazy imports inside method if needed to prevent circular import issues
        from yamlpipe.parser.transformation_parser import TransformationParser
        from yamlpipe.parser.quality_checks_parser import QualityChecksParser
        from yamlpipe.core.vars_manager import VariablesManager

        raw_yaml_path = cls._resolve_yaml_file(project_root, subfolder, selector)
        parsed_dir = os.path.join(project_root, "parsed", subfolder)
        os.makedirs(parsed_dir, exist_ok=True)

        json_cache_path = os.path.join(parsed_dir, f"{selector}.json")
        hash_file_path = os.path.join(project_root, "parsed", "parsed_hash.yml")
        
        # Per-Resource Lock file unique to the specific subfolder and selector
        resource_lock_path = os.path.join(parsed_dir, f".lock_{selector}")
        # Dedicated lock for atomic updates to the central hash registry
        global_hash_lock_path = os.path.join(project_root, "parsed", ".hash_registry.lock")

        resource_lock = ReentrantFileLock(resource_lock_path, timeout=30)
        hash_lock = ReentrantFileLock(global_hash_lock_path, timeout=30)

        try:
            with resource_lock:
                current_hash = cls._compute_md5(raw_yaml_path)
                
                # Fetch current hashes safely under global hash lock
                with hash_lock:
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
                        logger.warning(
                            f"[CacheManager Warning] Read error or corrupted JSON detected at '{json_cache_path}'. Recompiling... Details: {type(e).__name__} - {str(e)}"
                        )

                # Cache MISS: Reparse and Recompile
                logger.info(f"Cache MISS: Compiling YAML rule '{subfolder}/{selector}'...")

                try:
                    with open(raw_yaml_path, "r", encoding="utf-8") as f:
                        raw_config = yaml.safe_load(f) or {}
                except Exception as e:
                    raise IOError(
                        f"[CacheManager Error] Unable to read source YAML configuration at '{raw_yaml_path}'. Details: {type(e).__name__} - {str(e)}"
                    )

                # Resolve variables and track dependencies
                referenced_vars = []
                if subfolder in ("transformation_rules", "quality_gate"):
                    try:
                        raw_config, referenced_vars_set = VariablesManager.extract_vars_and_parse(raw_config, project_root)
                        referenced_vars = sorted(list(referenced_vars_set))
                    except Exception as e:
                        raise RuntimeError(
                            f"[CacheManager Error] Variable parsing failed for '{subfolder}/{selector}'. Details: {type(e).__name__} - {str(e)}"
                        )

                # Compile Rule via registered Parsers
                try:
                    if subfolder == "vars":
                        compiled_result = raw_config
                    elif subfolder == "transformation_rules":
                        compiled_result = TransformationParser.parse(raw_config)
                    elif subfolder == "quality_gate":
                        compiled_result = QualityChecksParser.parse_quality_checks(raw_config)
                    else:
                        raise ValueError(f"Unsupported rule type subfolder: '{subfolder}'")
                except Exception as e:
                    raise RuntimeError(
                        f"[CacheManager Error] Parser step failed for '{subfolder}/{selector}' using parser '{subfolder}'. Details: {type(e).__name__} - {str(e)}"
                    )

                # Inject ContainVarsFrom dependency list into top-level compiled output
                if subfolder in ("transformation_rules", "quality_gate") and isinstance(compiled_result, dict):
                    compiled_result["ContainVarsFrom"] = referenced_vars

                # Save JSON Cache Atomically
                unique_id = uuid.uuid4().hex
                temp_json_path = f"{json_cache_path}.tmp.{os.getpid()}_{unique_id}"

                try:
                    with open(temp_json_path, "w", encoding="utf-8") as f:
                        json.dump(compiled_result, f, indent=4, ensure_ascii=False)
                    os.replace(temp_json_path, json_cache_path)
                except Exception as e:
                    if os.path.exists(temp_json_path):
                        try:
                            os.remove(temp_json_path)
                        except OSError:
                            pass
                    raise IOError(
                        f"[CacheManager Error] Failed to write compiled output JSON to '{json_cache_path}'. Details: {type(e).__name__} - {str(e)}"
                    )

                # Atomic Update to Global Hash Registry
                with hash_lock:
                    latest_hashes = cls._load_hashes(hash_file_path)
                    if subfolder not in latest_hashes:
                        latest_hashes[subfolder] = {}
                    latest_hashes[subfolder][selector] = current_hash

                    if subfolder == "vars":
                        latest_hashes["vars"][selector] = current_hash
                    else:
                        for var_selector in referenced_vars:
                            try:
                                var_yaml_path = cls._resolve_yaml_file(project_root, "vars", var_selector)
                                latest_hashes["vars"][var_selector] = cls._compute_md5(var_yaml_path)
                            except FileNotFoundError:
                                logger.warning(
                                    f"[CacheManager Warning] Referenced variable file '{var_selector}' not found on disk during hash update."
                                )

                    cls._save_hashes_atomic(hash_file_path, latest_hashes)

                logger.info(f"Successfully compiled and cached '{selector}' to '{json_cache_path}'")
                return compiled_result

        except Timeout:
            raise TimeoutError(
                f"[CacheManager Error] Process timed out waiting for lock while accessing '{subfolder}/{selector}'."
            )

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

        searched_paths_str = "\n".join(f"  - {c}" for c in candidates)
        raise FileNotFoundError(
            f"[CacheManager Error] Could not find source YAML for '{clean_selector}' in '{subfolder}'.\n"
            f"Searched paths:\n{searched_paths_str}"
        )