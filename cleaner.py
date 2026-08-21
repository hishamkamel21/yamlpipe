
import hashlib
import json
import logging
import os
import shutil
import threading
import uuid
from typing import Any, Dict, Optional
from filelock import FileLock, Timeout
from yamlpipe.utility.helper import Helper
from yamlpipe.core.cache_manager import ReentrantFileLock

logger = logging.getLogger("CacheManager")


def clean(cls, selector: Optional[str] = None, project_root: Optional[str] = None) -> None:
        """
        Cleans up parsed JSON files, locks, or hash index based on selector.
        Dynamically finds project_root if not passed.
        """
        if project_root is None:
            project_root = Helper.find_project_root()

        parsed_dir = os.path.join(project_root, "parsed")
        
        if not os.path.exists(parsed_dir):
            logger.info("[CacheManager Clean] 'parsed' directory does not exist. Nothing to clean.")
            return

        # Map alias
        if selector == "quality_rules":
            selector = "quality_gate"

        locks_dir = os.path.join(parsed_dir, ".locks")
        os.makedirs(locks_dir, exist_ok=True)
        global_hash_lock = ReentrantFileLock(
            os.path.join(locks_dir, ".hash_registry.lock"), timeout=30
        )

        with global_hash_lock:
            # 1. Clean All
            if selector is None or selector.lower() == "all":
                for item in os.listdir(parsed_dir):
                    item_path = os.path.join(parsed_dir, item)
                    try:
                        if os.path.isfile(item_path) or os.path.islink(item_path):
                            os.unlink(item_path)
                        elif os.path.isdir(item_path):
                            shutil.rmtree(item_path)
                    except Exception as e:
                        logger.error(f"[CacheManager Clean Error] Failed to delete '{item_path}': {e}")
                logger.info("[CacheManager Clean] Successfully purged all cached files and hash registry.")
                return

            hash_file_path = os.path.join(parsed_dir, "parsed_hash.yml")

            # 2. Clean 'hashes' registry only
            if selector == "hashes":
                if os.path.exists(hash_file_path):
                    os.remove(hash_file_path)
                    logger.info("[CacheManager Clean] Successfully removed 'parsed_hash.yml'.")
                return

            # 3. Clean entire Subfolder
            if selector in ("transformation_rules", "quality_gate", "vars"):
                target_subfolder = os.path.join(parsed_dir, selector)
                if os.path.exists(target_subfolder):
                    shutil.rmtree(target_subfolder)
                    logger.info(f"[CacheManager Clean] Removed all cached files in subfolder '{selector}'.")

                all_hashes = cls._load_hashes(hash_file_path)
                if selector in all_hashes:
                    all_hashes[selector] = {}
                    cls._save_hashes_atomic(hash_file_path, all_hashes)
                return

            # 4. Clean a single file selector
            clean_name = os.path.basename(selector).rsplit(".", 1)[0]
            file_found = False

            for subfolder in ("transformation_rules", "quality_gate", "vars"):
                candidate_path = os.path.join(parsed_dir, subfolder, f"{clean_name}.json")
                if os.path.exists(candidate_path):
                    os.remove(candidate_path)
                    file_found = True
                    logger.info(f"[CacheManager Clean] Removed cache file '{candidate_path}'.")

                    all_hashes = cls._load_hashes(hash_file_path)
                    if subfolder in all_hashes and selector in all_hashes[subfolder]:
                        del all_hashes[subfolder][selector]
                        cls._save_hashes_atomic(hash_file_path, all_hashes)
                    break

            if not file_found:
                logger.warning(f"[CacheManager Clean Warning] No cache file found for selector '{selector}'.")