import hashlib
import json
import logging
import os
import threading
import uuid
from typing import Any, Dict, List, Union
from filelock import FileLock, Timeout

logger = logging.getLogger("CacheManager")


class ReentrantFileLock:
    """Process-safe and Thread-safe Re-entrant File Lock keyed by file path."""
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
        """Calculates MD5 hash of a single file to detect content changes."""
        hasher = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                while chunk := f.read(8192):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except FileNotFoundError:
            raise FileNotFoundError(
                f"[CacheManager Error] File missing for hashing: '{file_path}'"
            )

    @classmethod
    def _get_target_hashes(cls, target_path: str) -> Union[str, Dict[str, str]]:
        """
        Computes MD5 hash for a single file or a dictionary of filename-to-MD5 mappings 
        for all YAML files inside a folder.
        """
        if os.path.isfile(target_path):
            return cls._compute_md5(target_path)

        if os.path.isdir(target_path):
            folder_hashes: Dict[str, str] = {}
            yaml_files = [
                f for f in sorted(os.listdir(target_path))
                if f.endswith((".yaml", ".yml"))
            ]
            if not yaml_files:
                raise FileNotFoundError(
                    f"[CacheManager Error] No YAML configurations found in folder: '{target_path}'"
                )

            for fname in yaml_files:
                file_stem = fname.rsplit(".", 1)[0]
                full_path = os.path.join(target_path, fname)
                folder_hashes[file_stem] = cls._compute_md5(full_path)

            return folder_hashes

        raise FileNotFoundError(
            f"[CacheManager Error] Target path does not exist: '{target_path}'"
        )

    @staticmethod
    def _resolve_function_path(project_root: str, func_name: str) -> str:
        """Resolves Python file path inside project_root/functions/."""
        func_path = os.path.join(project_root, "functions", f"{func_name}.py")
        if os.path.exists(func_path):
            return func_path
        raise FileNotFoundError(
            f"[CacheManager Error] Function file missing: '{func_path}'"
        )

    @staticmethod
    def _resolve_custom_check_path(project_root: str, check_name: str) -> str:
        """Resolves Python file path inside project_root/custom_checks/."""
        check_path = os.path.join(project_root, "custom_checks", f"{check_name}.py")
        if os.path.exists(check_path):
            return check_path
        raise FileNotFoundError(
            f"[CacheManager Error] Custom check file missing: '{check_path}'"
        )

    @staticmethod
    def _load_hashes(hash_file_path: str) -> Dict[str, Any]:
        default_structure = {
            "transformation_rules": {},
            "quality_gate": {},
            "vars": {},
            "functions": {},
            "custom_checks": {},
        }
        if not os.path.exists(hash_file_path):
            return default_structure
        try:
            import yaml
            with open(hash_file_path, "r", encoding="utf-8") as f:
                content = yaml.safe_load(f) or {}
                if not isinstance(content, dict):
                    return default_structure
                for k in default_structure:
                    content.setdefault(k, {})
                return content
        except Exception as e:
            logger.warning(f"[CacheManager Warning] Failed to read hash index: {str(e)}")
            return default_structure

    @staticmethod
    def _save_hashes_atomic(hash_file_path: str, hash_data: Dict[str, Any]) -> None:
        import yaml
        unique_id = uuid.uuid4().hex
        temp_hash_path = f"{hash_file_path}.tmp.{os.getpid()}_{unique_id}"
        try:
            with open(temp_hash_path, "w", encoding="utf-8") as f:
                yaml.dump(hash_data, f, default_flow_style=False, sort_keys=True)
            os.replace(temp_hash_path, hash_file_path)
        except Exception as e:
            if os.path.exists(temp_hash_path):
                os.remove(temp_hash_path)
            logger.error(f"[CacheManager Error] Failed writing hash index: {str(e)}")

    @classmethod
    def get_or_compile(
        cls, project_root: str, subfolder: str, selector: str
    ) -> Dict[str, Any]:

        import yaml
        from yamlpipe.core.vars_manager import VariablesManager
        from yamlpipe.parser.quality_checks_parser import QualityChecksParser
        from yamlpipe.parser.transformation_parser import TransformationParser

        raw_target_path = cls._resolve_yaml_target(project_root, subfolder, selector)
        parsed_dir = os.path.join(project_root, "parsed", subfolder)
        os.makedirs(parsed_dir, exist_ok=True)

        parsed_locks_dir = os.path.join(parsed_dir, ".locks")
        global_locks_dir = os.path.join(project_root, "parsed", ".locks")
        
        os.makedirs(parsed_locks_dir, exist_ok=True)
        os.makedirs(global_locks_dir, exist_ok=True)

        clean_selector_name = os.path.basename(selector).rsplit(".", 1)[0]
        
        json_cache_path = os.path.join(parsed_dir, f"{clean_selector_name}.json")
        hash_file_path = os.path.join(project_root, "parsed", "parsed_hash.yml")

        resource_lock = ReentrantFileLock(os.path.join(parsed_locks_dir, f".lock_{clean_selector_name}"), timeout=30)
        hash_lock = ReentrantFileLock(os.path.join(global_locks_dir, ".hash_registry.lock"), timeout=30)

        try:
            with resource_lock:
                current_raw_hash = cls._get_target_hashes(raw_target_path)

                with hash_lock:
                    all_hashes = cls._load_hashes(hash_file_path)

                cached_hash = all_hashes.get(subfolder, {}).get(selector)

                # Validate existing JSON cache
                if cached_hash == current_raw_hash and os.path.exists(json_cache_path):
                    try:
                        with open(json_cache_path, "r", encoding="utf-8") as f:
                            cached_data = json.load(f)

                        var_deps = cached_data.get("ContainVarsFrom", [])
                        func_deps = cached_data.get("ContainFunctionsFrom", [])
                        custom_check_deps = cached_data.get("ContainCustomChecksFrom", [])
                        valid = True

                        # 1. Check Var Dependencies
                        for var_sel in var_deps:
                            var_path = cls._resolve_yaml_target(project_root, "vars", var_sel)
                            if cls._get_target_hashes(var_path) != all_hashes.get("vars", {}).get(var_sel):
                                valid = False
                                break

                        # 2. Check Custom Function Dependencies (from functions/)
                        if valid:
                            for func_name in func_deps:
                                func_path = cls._resolve_function_path(project_root, func_name)
                                if cls._compute_md5(func_path) != all_hashes.get("functions", {}).get(func_name):
                                    valid = False
                                    break

                        # 3. Check Custom Quality Check Dependencies (from custom_checks/)
                        if valid:
                            for check_name in custom_check_deps:
                                check_path = cls._resolve_custom_check_path(project_root, check_name)
                                if cls._compute_md5(check_path) != all_hashes.get("custom_checks", {}).get(check_name):
                                    valid = False
                                    break

                        if valid:
                            logger.info(f"Cache HIT: Loaded compiled JSON for '{subfolder}/{selector}'")
                            return cached_data

                    except Exception as e:
                        logger.warning(f"[CacheManager Warning] Cache validation failed: {str(e)}. Recompiling...")

                # Cache MISS -> Parse and re-index
                logger.info(f"Cache MISS: Compiling YAML configuration '{subfolder}/{selector}'...")

                # Compilation logic handling directory vs single file
                if os.path.isdir(raw_target_path):
                    file_map: Dict[str, Dict[str, Any]] = {}
                    for fname in sorted(os.listdir(raw_target_path)):
                        if fname.endswith((".yaml", ".yml")):
                            key_name = fname.rsplit(".", 1)[0]
                            full_file_path = os.path.join(raw_target_path, fname)
                            with open(full_file_path, "r", encoding="utf-8") as f:
                                file_map[key_name] = yaml.safe_load(f) or {}

                    # Variable replacement across all folder YAMLs
                    if subfolder in ("transformation_rules", "quality_gate"):
                        all_referenced_vars = set()
                        for k, single_cfg in file_map.items():
                            parsed_cfg, ref_vars = VariablesManager.extract_vars_and_parse(single_cfg, project_root)
                            file_map[k] = parsed_cfg
                            all_referenced_vars.update(ref_vars)

                    # Determine DAG terminal node
                    referenced_keys = {cfg.get("run_ref") for cfg in file_map.values() if cfg.get("run_ref")}
                    terminal_keys = set(file_map.keys()) - referenced_keys
                    
                    if not terminal_keys:
                        raise ValueError(f"[CacheManager Error] Circular reference or missing root in folder '{selector}'")
                    
                    entry_key = next(iter(terminal_keys))
                    entry_config = file_map[entry_key]

                    if subfolder == "transformation_rules":
                        compiled_result = TransformationParser.parse(entry_config, file_map=file_map)
                    else:
                        raise ValueError(f"Folder resolution not supported for subfolder '{subfolder}'")

                    if subfolder in ("transformation_rules", "quality_gate"):
                        compiled_result["ContainVarsFrom"] = sorted(list(all_referenced_vars))

                else:
                    # Single-file fallback mode
                    with open(raw_target_path, "r", encoding="utf-8") as f:
                        raw_config = yaml.safe_load(f) or {}

                    if subfolder in ("transformation_rules", "quality_gate"):
                        raw_config, referenced_vars_set = VariablesManager.extract_vars_and_parse(raw_config, project_root)
                        raw_config["ContainVarsFrom"] = sorted(list(referenced_vars_set))

                    if subfolder == "vars":
                        compiled_result = raw_config
                    elif subfolder == "transformation_rules":
                        compiled_result = TransformationParser.parse(raw_config)
                    elif subfolder == "quality_gate":
                        compiled_result = QualityChecksParser.parse_quality_checks(raw_config)
                    else:
                        raise ValueError(f"Unsupported subfolder: '{subfolder}'")

                # Atomic cache file write
                unique_id = uuid.uuid4().hex
                temp_json_path = f"{json_cache_path}.tmp.{os.getpid()}_{unique_id}"
                with open(temp_json_path, "w", encoding="utf-8") as f:
                    json.dump(compiled_result, f, indent=4, ensure_ascii=False)
                os.replace(temp_json_path, json_cache_path)

                # Atomic update to global hashes registry
                with hash_lock:
                    latest_hashes = cls._load_hashes(hash_file_path)
                    latest_hashes.setdefault(subfolder, {})[selector] = current_raw_hash

                    # Sync Variable MD5s
                    for var_sel in compiled_result.get("ContainVarsFrom", []):
                        var_path = cls._resolve_yaml_target(project_root, "vars", var_sel)
                        latest_hashes["vars"][var_sel] = cls._get_target_hashes(var_path)

                    # Sync Function MD5s
                    for func_name in compiled_result.get("ContainFunctionsFrom", []):
                        func_path = cls._resolve_function_path(project_root, func_name)
                        latest_hashes["functions"][func_name] = cls._compute_md5(func_path)

                    # Sync Custom Check MD5s (from custom_checks/)
                    for check_name in compiled_result.get("ContainCustomChecksFrom", []):
                        check_path = cls._resolve_custom_check_path(project_root, check_name)
                        latest_hashes["custom_checks"][check_name] = cls._compute_md5(check_path)

                    cls._save_hashes_atomic(hash_file_path, latest_hashes)

                return compiled_result

        except Timeout:
            raise TimeoutError(f"[CacheManager Error] Lock timeout for '{subfolder}/{selector}'.")

    @staticmethod
    def _resolve_yaml_target(project_root: str, subfolder: str, selector: str) -> str:
        """Resolves target path for either a file or folder in the project layout."""
        clean_selector = selector.rsplit(".", 1)[0] if selector.endswith((".yaml", ".yml")) else selector
        
        target_base = (
            os.path.join(project_root, "vars")
            if subfolder == "vars"
            else os.path.join(project_root, "yaml_configs", subfolder)
        )

        if not os.path.exists(target_base):
            raise FileNotFoundError(
                f"[CacheManager Error] Target base directory does not exist: '{target_base}'"
            )

        # 1. Folder match check
        folder_path = os.path.join(target_base, clean_selector)
        if os.path.isdir(folder_path):
            return folder_path

        # 2. File match check
        for ext in (".yaml", ".yml"):
            file_path = os.path.join(target_base, f"{clean_selector}{ext}")
            if os.path.isfile(file_path):
                return file_path

        # 3. Recursive directory search
        target_filenames = {f"{clean_selector}.yaml", f"{clean_selector}.yml"}
        for root, dirs, files in os.walk(target_base):
            if clean_selector in dirs:
                return os.path.join(root, clean_selector)
            for file in files:
                if file in target_filenames:
                    return os.path.join(root, file)

        raise FileNotFoundError(
            f"[CacheManager Error] YAML source file or directory not found for '{clean_selector}' inside '{target_base}'."
        )