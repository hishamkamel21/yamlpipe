import os
import logging
from typing import Optional, Dict, Any
from yamlpipe.core.cache_manager import CacheManager
from yamlpipe.utility.helper import Helper

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
        """Loads compiled/cached Transformation Rules configuration as parsed JSON dict."""
        project_root = Helper.find_project_root(explicit_project_dir=project_dir)
        clean_selector = selector.rsplit(".", 1)[0] if selector.endswith((".yaml", ".yml")) else selector

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