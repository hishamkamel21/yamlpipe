import hashlib
import json
import logging
import os
import threading
import uuid
from typing import Any, Dict, List, Union
from filelock import FileLock, Timeout
from yamlpipe.utility.logger import get_logger

logger = get_logger("[ CacheManager ]")


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
            md5_hash = hasher.hexdigest()
            logger.debug(f"Computed MD5 for '{file_path}': {md5_hash}")
            return md5_hash
        except FileNotFoundError:
            err_msg = f"[CacheManager Error] File missing for hashing: '{file_path}'"
            logger.error(err_msg)
            raise FileNotFoundError(err_msg)

    @classmethod
    def _get_target_hashes(cls, target_path: str) -> Union[str, Dict[str, str]]:
        if os.path.isfile(target_path):
            logger.debug(f"Target is a single file: '{target_path}'")
            return cls._compute_md5(target_path)

        if os.path.isdir(target_path):
            logger.debug(f"Target is a folder: '{target_path}'. Computing hashes for contained YAML files...")
            folder_hashes: Dict[str, str] = {}
            yaml_files = [
                f for f in sorted(os.listdir(target_path))
                if f.endswith((".yaml", ".yml"))
            ]
            if not yaml_files:
                err_msg = f"[CacheManager Error] No YAML configurations found in folder: '{target_path}'"
                logger.error(err_msg)
                raise FileNotFoundError(err_msg)

            for fname in yaml_files:
                file_stem = fname.rsplit(".", 1)[0]
                full_path = os.path.join(target_path, fname)
                folder_hashes[file_stem] = cls._compute_md5(full_path)

            logger.debug(f"Computed folder hashes for '{target_path}': {len(folder_hashes)} file(s) hashed.")
            return folder_hashes

        err_msg = f"[CacheManager Error] Target path does not exist: '{target_path}'"
        logger.error(err_msg)
        raise FileNotFoundError(err_msg)

    @staticmethod
    def _resolve_function_path(project_root: str, func_name: str) -> str:
        func_path = os.path.join(project_root, "functions", f"{func_name}.py")
        if os.path.exists(func_path):
            return func_path
        err_msg = f"[CacheManager Error] Function file missing: '{func_path}'"
        logger.error(err_msg)
        raise FileNotFoundError(err_msg)

    @staticmethod
    def _resolve_custom_check_path(project_root: str, check_name: str) -> str:
        check_path = os.path.join(project_root, "custom_checks", f"{check_name}.py")
        if os.path.exists(check_path):
            return check_path
        err_msg = f"[CacheManager Error] Custom check file missing: '{check_path}'"
        logger.error(err_msg)
        raise FileNotFoundError(err_msg)

    @staticmethod
    def _load_hashes(hash_file_path: str) -> Dict[str, Any]:
        default_structure = {
            "transformation_rules": {},
            "quality_gate": {},
            "vars": {},
            "templates": {},
            "functions": {},
            "custom_checks": {},
        }
        if not os.path.exists(hash_file_path):
            logger.info(f"Hash index file not found at '{hash_file_path}'. Initializing empty hash structure.")
            return default_structure
        try:
            import yaml
            with open(hash_file_path, "r", encoding="utf-8") as f:
                content = yaml.safe_load(f) or {}
                if not isinstance(content, dict):
                    logger.warning(f"Invalid format in hash index '{hash_file_path}'. Re-initializing structure.")
                    return default_structure
                for k in default_structure:
                    content.setdefault(k, {})
                logger.debug(f"Loaded existing hash registry from '{hash_file_path}'")
                return content
        except Exception as e:
            logger.warning(f"Failed to read hash index at '{hash_file_path}': {str(e)}. Falling back to default.")
            return default_structure

    @staticmethod
    def _save_hashes_atomic(hash_file_path: str, hash_data: Dict[str, Any]) -> None:
        import yaml
        unique_id = uuid.uuid4().hex
        temp_hash_path = f"{hash_file_path}.tmp.{os.getpid()}_{unique_id}"
        try:
            logger.debug(f"Saving updated hash index atomically to '{hash_file_path}'...")
            with open(temp_hash_path, "w", encoding="utf-8") as f:
                yaml.dump(hash_data, f, default_flow_style=False, sort_keys=True)
            os.replace(temp_hash_path, hash_file_path)
            logger.debug("Successfully saved hash registry.")
        except Exception as e:
            if os.path.exists(temp_hash_path):
                os.remove(temp_hash_path)
            logger.error(f"Failed writing hash index to '{hash_file_path}': {str(e)}", exc_info=True)

    @classmethod
    def get_or_compile(
        cls, project_root: str, subfolder: str, selector: str
    ) -> Dict[str, Any]:

        import yaml
        from yamlpipe.core.vars_manager import VariablesManager
        from yamlpipe.parser.quality_checks_parser import QualityChecksParser
        from yamlpipe.parser.transformation_parser import TransformationParser

        logger.info(f"Cache check initiated for subfolder='{subfolder}', selector='{selector}'")

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
                recompile_reason = None

                # Check if primary target raw YAML content changed
                if cached_hash != current_raw_hash:
                    recompile_reason = f"Source YAML file/folder content changed for '{subfolder}/{selector}'"
                elif not os.path.exists(json_cache_path):
                    recompile_reason = f"Compiled JSON cache file missing at '{json_cache_path}'"

                # Validate dependencies if target YAML itself hasn't changed
                if not recompile_reason:
                    try:
                        with open(json_cache_path, "r", encoding="utf-8") as f:
                            cached_data = json.load(f)

                        var_deps = cached_data.get("ContainVarsFrom", [])
                        template_deps = cached_data.get("ContainTemplatesFrom", [])
                        func_deps = cached_data.get("ContainFunctionsFrom", [])
                        custom_check_deps = cached_data.get("ContainCustomChecksFrom", [])

                        # 1. Check Var Dependencies
                        for var_sel in var_deps:
                            var_path = cls._resolve_yaml_target(project_root, "vars", var_sel)
                            curr_var_hash = cls._get_target_hashes(var_path)
                            prev_var_hash = all_hashes.get("vars", {}).get(var_sel)
                            if curr_var_hash != prev_var_hash:
                                recompile_reason = f"Detected change in referenced variable (vars): '{var_sel}'"
                                logger.info(recompile_reason)
                                break

                        # 2. Check Template Dependencies
                        if not recompile_reason:
                            for tpl_sel in template_deps:
                                tpl_path = cls._resolve_yaml_target(project_root, "templates", tpl_sel)
                                curr_tpl_hash = cls._get_target_hashes(tpl_path)
                                prev_tpl_hash = all_hashes.get("templates", {}).get(tpl_sel)
                                if curr_tpl_hash != prev_tpl_hash:
                                    recompile_reason = f"Detected change in referenced template: '{tpl_sel}'"
                                    logger.info(recompile_reason)
                                    break

                        # 3. Check Custom Function Dependencies
                        if not recompile_reason:
                            for func_name in func_deps:
                                func_path = cls._resolve_function_path(project_root, func_name)
                                curr_func_hash = cls._compute_md5(func_path)
                                prev_func_hash = all_hashes.get("functions", {}).get(func_name)
                                if curr_func_hash != prev_func_hash:
                                    recompile_reason = f"Detected change in Python custom function: '{func_name}' ({func_path})"
                                    logger.info(recompile_reason)
                                    break

                        # 4. Check Custom Check Dependencies
                        if not recompile_reason:
                            for check_name in custom_check_deps:
                                check_path = cls._resolve_custom_check_path(project_root, check_name)
                                curr_check_hash = cls._compute_md5(check_path)
                                prev_check_hash = all_hashes.get("custom_checks", {}).get(check_name)
                                if curr_check_hash != prev_check_hash:
                                    recompile_reason = f"Detected change in Python custom quality check: '{check_name}' ({check_path})"
                                    logger.info(recompile_reason)
                                    break

                        if not recompile_reason:
                            logger.info(f"[Cache HIT] Loaded valid compiled JSON cache for '{subfolder}/{selector}'")
                            return cached_data

                    except Exception as e:
                        recompile_reason = f"Failed to validate cached dependencies: {str(e)}"
                        logger.warning(f"Cache validation error for '{subfolder}/{selector}': {str(e)}. Triggering recompile.")

                # Cache MISS -> Parse and re-index
                logger.info(f"[Cache MISS] Compiling '{subfolder}/{selector}' -> Reason: {recompile_reason}")

                if os.path.isdir(raw_target_path):
                    logger.debug(f"Processing folder-based config at '{raw_target_path}'")
                    file_map: Dict[str, Dict[str, Any]] = {}
                    for fname in sorted(os.listdir(raw_target_path)):
                        if fname.endswith((".yaml", ".yml")):
                            key_name = fname.rsplit(".", 1)[0]
                            full_file_path = os.path.join(raw_target_path, fname)
                            with open(full_file_path, "r", encoding="utf-8") as f:
                                file_map[key_name] = yaml.safe_load(f) or {}

                    if subfolder in ("transformation_rules", "quality_gate"):
                        logger.debug("Extracting variables from folder configs...")
                        all_referenced_vars = set()
                        for k, single_cfg in file_map.items():
                            parsed_cfg, ref_vars = VariablesManager.extract_vars_and_parse(single_cfg, project_root)
                            file_map[k] = parsed_cfg
                            all_referenced_vars.update(ref_vars)

                    referenced_keys = {cfg.get("run_ref") for cfg in file_map.values() if cfg.get("run_ref")}
                    terminal_keys = set(file_map.keys()) - referenced_keys

                    if not terminal_keys:
                        err_msg = f"[CacheManager Error] Circular reference or missing root in folder '{selector}'"
                        logger.error(err_msg)
                        raise ValueError(err_msg)

                    entry_key = next(iter(terminal_keys))
                    entry_config = file_map[entry_key]

                    if subfolder == "transformation_rules":
                        logger.info("Executing TransformationParser for folder configuration...")
                        compiled_result = TransformationParser.parse(entry_config, file_map=file_map)
                    elif subfolder == "quality_gate":
                        logger.info("Executing QualityChecksParser for folder configuration...")
                        compiled_result = QualityChecksParser.parse_quality_checks(entry_config)
                    else:
                        raise ValueError(f"Folder resolution not supported for subfolder '{subfolder}'")

                    if subfolder in ("transformation_rules", "quality_gate"):
                        compiled_result["ContainVarsFrom"] = sorted(list(all_referenced_vars))

                else:
                    # Single-file mode
                    logger.debug(f"Processing single YAML config file at '{raw_target_path}'")
                    with open(raw_target_path, "r", encoding="utf-8") as f:
                        raw_config = yaml.safe_load(f) or {}

                    if subfolder in ("transformation_rules", "quality_gate"):
                        logger.debug(f"Resolving variables for single file '{selector}'...")
                        raw_config, referenced_vars_set = VariablesManager.extract_vars_and_parse(raw_config, project_root)
                        raw_config["ContainVarsFrom"] = sorted(list(referenced_vars_set))

                    if subfolder in ("vars", "templates"):
                        compiled_result = raw_config
                    elif subfolder == "transformation_rules":
                        logger.info("Executing TransformationParser for single file...")
                        compiled_result = TransformationParser.parse(raw_config)
                    elif subfolder == "quality_gate":
                        logger.info("Executing QualityChecksParser for single file...")
                        compiled_result = QualityChecksParser.parse_quality_checks(raw_config)
                    else:
                        raise ValueError(f"Unsupported subfolder: '{subfolder}'")

                # Atomic cache file write
                unique_id = uuid.uuid4().hex
                temp_json_path = f"{json_cache_path}.tmp.{os.getpid()}_{unique_id}"
                logger.debug(f"Writing compiled output atomically to '{json_cache_path}'...")
                with open(temp_json_path, "w", encoding="utf-8") as f:
                    json.dump(compiled_result, f, indent=4, ensure_ascii=False)
                os.replace(temp_json_path, json_cache_path)
                logger.info(f"Successfully cached compiled JSON to '{json_cache_path}'")

                # Update global hashes registry
                with hash_lock:
                    logger.debug("Updating global hash registry with latest dependency states...")
                    latest_hashes = cls._load_hashes(hash_file_path)
                    latest_hashes.setdefault(subfolder, {})[selector] = current_raw_hash

                    # Log tracked dependencies
                    vars_list = compiled_result.get("ContainVarsFrom", [])
                    if vars_list:
                        logger.debug(f"Tracking {len(vars_list)} variable dependency file(s): {vars_list}")
                    for var_sel in vars_list:
                        var_path = cls._resolve_yaml_target(project_root, "vars", var_sel)
                        latest_hashes["vars"][var_sel] = cls._get_target_hashes(var_path)

                    tpls_list = compiled_result.get("ContainTemplatesFrom", [])
                    if tpls_list:
                        logger.debug(f"Tracking {len(tpls_list)} template dependency file(s): {tpls_list}")
                    for tpl_sel in tpls_list:
                        tpl_path = cls._resolve_yaml_target(project_root, "templates", tpl_sel)
                        latest_hashes["templates"][tpl_sel] = cls._get_target_hashes(tpl_path)

                    funcs_list = compiled_result.get("ContainFunctionsFrom", [])
                    if funcs_list:
                        logger.debug(f"Tracking {len(funcs_list)} function dependency file(s): {funcs_list}")
                    for func_name in funcs_list:
                        func_path = cls._resolve_function_path(project_root, func_name)
                        latest_hashes["functions"][func_name] = cls._compute_md5(func_path)

                    checks_list = compiled_result.get("ContainCustomChecksFrom", [])
                    if checks_list:
                        logger.debug(f"Tracking {len(checks_list)} custom quality check dependency file(s): {checks_list}")
                    for check_name in checks_list:
                        check_path = cls._resolve_custom_check_path(project_root, check_name)
                        latest_hashes["custom_checks"][check_name] = cls._compute_md5(check_path)

                    cls._save_hashes_atomic(hash_file_path, latest_hashes)

                return compiled_result

        except Timeout:
            err_msg = f"[CacheManager Error] Lock timeout acquiring file locks for '{subfolder}/{selector}'."
            logger.error(err_msg)
            raise TimeoutError(err_msg)

    @staticmethod
    def _resolve_yaml_target(project_root: str, subfolder: str, selector: str) -> str:
        clean_selector = selector.rsplit(".", 1)[0] if selector.endswith((".yaml", ".yml")) else selector

        if subfolder == "vars":
            target_base = os.path.join(project_root, "vars")
        elif subfolder == "templates":
            target_base = os.path.join(project_root, "templates")  # Root-level ONLY
        else:
            target_base = os.path.join(project_root, "yaml_configs", subfolder)

        if not os.path.exists(target_base):
            err_msg = f"[CacheManager Error] Directory does not exist: '{target_base}'"
            logger.error(err_msg)
            raise FileNotFoundError(err_msg)

        folder_path = os.path.join(target_base, clean_selector)
        if os.path.isdir(folder_path):
            logger.debug(f"Resolved selector '{selector}' to folder path: '{folder_path}'")
            return folder_path

        for ext in (".yaml", ".yml"):
            file_path = os.path.join(target_base, f"{clean_selector}{ext}")
            if os.path.isfile(file_path):
                logger.debug(f"Resolved selector '{selector}' to direct file path: '{file_path}'")
                return file_path

        target_filenames = {f"{clean_selector}.yaml", f"{clean_selector}.yml"}
        for root, dirs, files in os.walk(target_base):
            if clean_selector in dirs:
                resolved = os.path.join(root, clean_selector)
                logger.debug(f"Resolved selector '{selector}' via recursive search to folder: '{resolved}'")
                return resolved
            for file in files:
                if file in target_filenames:
                    resolved = os.path.join(root, file)
                    logger.debug(f"Resolved selector '{selector}' via recursive search to file: '{resolved}'")
                    return resolved

        err_msg = f"[CacheManager Error] Source file or directory not found for '{clean_selector}' inside '{target_base}'."
        logger.error(err_msg)
        raise FileNotFoundError(err_msg)