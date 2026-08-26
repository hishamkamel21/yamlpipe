import os
import yaml
import logging
from typing import Optional, Dict, Any
from yamlpipe.core.cache_manager import CacheManager
from yamlpipe.utility.helper import Helper
from yamlpipe.parsers.transformation_parser import TransformationParser

logger = logging.getLogger("Getter")


class Getter:

    @classmethod
    def get_quality_rules(cls, selector: str, project_dir: Optional[str] = None) -> Dict[str, Any]:
        """Loads compiled/cached Quality Gate configuration as parsed JSON dict."""
        project_root = Helper.find_project_root(explicit_project_dir=project_dir)
        clean_selector = selector.rsplit(".", 1)[0] if selector.endswith((".yaml", ".yml")) else selector

        return CacheManager.get_or_compile(
            project_root=project_root,
            subfolder="quality_gate",
            selector=clean_selector
        )

    @classmethod
    def get_transformation_rules(cls, selector: str, project_dir: Optional[str] = None) -> Dict[str, Any]:
        """Loads compiled Transformation Rules configuration. Supports single YAML or multi-file directories."""
        project_root = Helper.find_project_root(explicit_project_dir=project_dir)
        clean_selector = selector.rsplit(".", 1)[0] if selector.endswith((".yaml", ".yml")) else selector
        
        target_path = os.path.join(project_root, "yaml_configs", "transformation_rules", clean_selector)

        # Folder processing mode
        if os.path.isdir(target_path):
            file_map: Dict[str, Dict[str, Any]] = {}
            yaml_files = [f for f in os.listdir(target_path) if f.endswith((".yaml", ".yml"))]
            
            if not yaml_files:
                raise FileNotFoundError(f"[Getter Error] No YAML configurations found in directory: {target_path}")

            for fname in yaml_files:
                key_name = fname.rsplit(".", 1)[0]
                full_path = os.path.join(target_path, fname)
                with open(full_path, "r", encoding="utf-8") as stream:
                    file_map[key_name] = yaml.safe_load(stream) or {}

            # Identify leaf node (file not referenced in any 'run_ref')
            referenced_keys = {
                cfg.get("run_ref") for cfg in file_map.values() if cfg.get("run_ref")
            }
            terminal_keys = set(file_map.keys()) - referenced_keys

            if not terminal_keys:
                raise ValueError(f"[Getter Error] Circular references or missing terminal file in folder: {clean_selector}")

            # Pick entrypoint node
            terminal_key = next(iter(terminal_keys))
            entry_config = file_map[terminal_key]

            return TransformationParser.parse(
                raw_config=entry_config,
                file_map=file_map
            )

        # Standard file fallback mode via CacheManager
        return CacheManager.get_or_compile(
            project_root=project_root,
            subfolder="transformation_rules",
            selector=clean_selector
        )

    @classmethod
    def get_vars(cls, selector: str, project_dir: Optional[str] = None) -> Dict[str, Any]:
        """Loads compiled/cached variables configuration as parsed JSON dict."""
        project_root = Helper.find_project_root(explicit_project_dir=project_dir)
        clean_selector = selector.rsplit(".", 1)[0] if selector.endswith((".yaml", ".yml")) else selector

        return CacheManager.get_or_compile(
            project_root=project_root,
            subfolder="vars",
            selector=clean_selector
        )