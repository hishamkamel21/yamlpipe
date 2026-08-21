import logging
import os
import shutil
from typing import Optional

# استيراد القفل من CacheManager والـ Helper لـ Project Root
from yamlpipe.core.cache_manager import CacheManager, ReentrantFileLock
from yamlpipe.utils.helper import Helper

logger = logging.getLogger("Cleaner")


class Cleaner:

    @staticmethod
    def _load_hashes(hash_file_path: str):
        if not os.path.exists(hash_file_path):
            return {}
        try:
            import yaml
            with open(hash_file_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}

    @staticmethod
    def _save_hashes_atomic(hash_file_path: str, hash_data: dict) -> None:
        import uuid
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
            logger.error(f"[Cleaner Error] Failed updating hash index: {str(e)}")

    @classmethod
    def clean(cls, selector: Optional[str] = None, project_root: Optional[str] = None) -> None:
        """
        Cleans up parsed JSON files, locks, or hash index based on selector.
        """
        if project_root is None:
            project_root = Helper.find_project_root()

        parsed_dir = os.path.join(project_root, "parsed")

        if not os.path.exists(parsed_dir):
            logger.info("[Cleaner] 'parsed' directory does not exist. Nothing to clean.")
            return

        # Map aliases
        if selector == "quality_rules":
            selector = "quality_gate"

        locks_dir = os.path.join(parsed_dir, ".locks")
        os.makedirs(locks_dir, exist_ok=True)
        global_hash_lock = ReentrantFileLock(
            os.path.join(locks_dir, ".hash_registry.lock"), timeout=30
        )

        with global_hash_lock:
            # 1. Clean All (Everything inside parsed/)
            if selector is None or selector.lower() == "all":
                for item in os.listdir(parsed_dir):
                    item_path = os.path.join(parsed_dir, item)
                    try:
                        if os.path.isfile(item_path) or os.path.islink(item_path):
                            os.unlink(item_path)
                        elif os.path.isdir(item_path):
                            shutil.rmtree(item_path)
                    except Exception as e:
                        logger.error(f"[Cleaner Error] Failed to delete '{item_path}': {e}")
                logger.info("[Cleaner] Successfully purged all cached files and hash registry.")
                return

            hash_file_path = os.path.join(parsed_dir, "parsed_hash.yml")

            # 2. Clean 'hashes' registry file only
            if selector == "hashes":
                if os.path.exists(hash_file_path):
                    os.remove(hash_file_path)
                    logger.info("[Cleaner] Successfully removed 'parsed_hash.yml'.")
                return

            # 3. Clean full target subfolder
            if selector in ("transformation_rules", "quality_gate", "vars"):
                target_subfolder = os.path.join(parsed_dir, selector)
                if os.path.exists(target_subfolder):
                    shutil.rmtree(target_subfolder)
                    logger.info(f"[Cleaner] Removed all cached files in subfolder '{selector}'.")

                # Sync parsed_hash.yml
                all_hashes = cls._load_hashes(hash_file_path)
                if selector in all_hashes:
                    all_hashes[selector] = {}
                    cls._save_hashes_atomic(hash_file_path, all_hashes)
                return

            # 4. Clean a specific JSON cache file
            clean_name = os.path.basename(selector).rsplit(".", 1)[0]
            file_found = False

            for subfolder in ("transformation_rules", "quality_gate", "vars"):
                candidate_path = os.path.join(parsed_dir, subfolder, f"{clean_name}.json")
                if os.path.exists(candidate_path):
                    os.remove(candidate_path)
                    file_found = True
                    logger.info(f"[Cleaner] Removed cache file '{candidate_path}'.")

                    # Sync parsed_hash.yml
                    all_hashes = cls._load_hashes(hash_file_path)
                    if subfolder in all_hashes and selector in all_hashes[subfolder]:
                        del all_hashes[subfolder][selector]
                        cls._save_hashes_atomic(hash_file_path, all_hashes)
                    break

            if not file_found:
                logger.warning(f"[Cleaner Warning] No cache file found for selector '{selector}'.")


# Standalone function wrapper
def clean(selector: Optional[str] = None, project_root: Optional[str] = None) -> None:
    Cleaner.clean(selector=selector, project_root=project_root)