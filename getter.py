import os
import yaml
import logging
from typing import Optional, Dict, Any
from yamlpipe.core.cache_manager import CacheManager

logger = logging.getLogger("Getter")


class Getter:

    @staticmethod
    def _find_project_root(explicit_project_dir: Optional[str] = None) -> str:
        cwd = os.path.abspath(os.getcwd())

        # Step 1: Handle explicit project directory override
        if explicit_project_dir:
            resolved_path = (
                explicit_project_dir
                if os.path.isabs(explicit_project_dir)
                else os.path.abspath(os.path.join(cwd, explicit_project_dir))
            )
            
            if os.path.exists(os.path.join(resolved_path, "project.yml")):
                return resolved_path
            elif os.path.exists(resolved_path):
                logger.warning(f"Directory '{resolved_path}' specified, but 'project.yml' was not found.")
                return resolved_path
            else:
                raise FileNotFoundError(f"Specified project directory does not exist: {resolved_path}")

        # Step 2: Standard Upward Search
        current_dir = cwd
        while True:
            candidate_config = os.path.join(current_dir, "project.yml")
            if os.path.exists(candidate_config):
                try:
                    with open(candidate_config, "r", encoding="utf-8") as f:
                        config = yaml.safe_load(f)
                        return config.get("project", {}).get("project_dir", current_dir)
                except Exception as e:
                    logger.warning(f"Could not parse project.yml at {candidate_config}: {str(e)}")
                    return current_dir

            parent_dir = os.path.dirname(current_dir)
            if parent_dir == current_dir:
                break
            current_dir = parent_dir

        # Step 3: Dynamic Downward Search
        try:
            subdirs = [
                os.path.join(cwd, d) for d in os.listdir(cwd) 
                if os.path.isdir(os.path.join(cwd, d)) and not d.startswith((".", "_"))
            ]
            
            projects_found = [
                d for d in subdirs 
                if os.path.exists(os.path.join(d, "project.yml"))
            ]

            if len(projects_found) == 1:
                logger.info(f"Dynamically resolved project root to: '{projects_found[0]}'")
                return projects_found[0]
            
            elif len(projects_found) > 1:
                project_names = [os.path.basename(p) for p in projects_found]
                raise RuntimeError(
                    f"Multiple projects detected in subdirectories: {project_names}. "
                    f"Please specify 'project_dir' explicitly in your Getter call."
                )

        except Exception as e:
            if isinstance(e, RuntimeError):
                raise e
            logger.debug(f"Subdirectory search skipped: {e}")

        # Step 4: Fallback to CWD
        return cwd

    @classmethod
    def get_quality_rules(cls, selector: str, project_dir: Optional[str] = None) -> Dict[str, Any]:
        """Loads compiled/cached Quality Gate configuration as parsed JSON dict."""
        project_root = cls._find_project_root(explicit_project_dir=project_dir)
        clean_selector = selector.rsplit(".", 1)[0] if selector.endswith((".yaml", ".yml")) else selector
        
        return CacheManager.get_or_compile(
            project_root=project_root,
            subfolder="quality_gate",
            selector=clean_selector
        )

    @classmethod
    def get_transformation_rules(cls, selector: str, project_dir: Optional[str] = None) -> Dict[str, Any]:
        """Loads compiled/cached Transformation Rules configuration as parsed JSON dict."""
        project_root = cls._find_project_root(explicit_project_dir=project_dir)
        clean_selector = selector.rsplit(".", 1)[0] if selector.endswith((".yaml", ".yml")) else selector

        return CacheManager.get_or_compile(
            project_root=project_root,
            subfolder="transformation_rules",
            selector=clean_selector
        )