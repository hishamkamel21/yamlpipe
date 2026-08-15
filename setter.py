import os
import yaml
import logging

logger = logging.getLogger("ProjectSetter")


def set_project(project_dir: str = ".") -> str:
    """
    Creates project directory structure including raw YAML configs, 
    custom modules, variable definitions, and compilation cache directories.
    """
    try:
        target_dir = os.path.abspath(project_dir)
        root_dir = os.path.dirname(target_dir) if project_dir != "." else target_dir

        # Physical directories inside project_dir
        folders = [
            os.path.join(target_dir, "custom_checks"),
            os.path.join(target_dir, "functions"),
            os.path.join(target_dir, "pipeline"),
            os.path.join(target_dir, "vars"),
            os.path.join(target_dir, "yaml_configs", "transformation_rules"),
            os.path.join(target_dir, "yaml_configs", "quality_gate"),
            # Dynamic Compilation Cache Folders
            os.path.join(target_dir, "parsed", "transformation_rules"),
            os.path.join(target_dir, "parsed", "quality_gate"),
            os.path.join(target_dir, "parsed", "vars"),
        ]

        for folder in folders:
            os.makedirs(folder, exist_ok=True)
            logger.info(f"Created directory: {folder}")

        project_config = {
            "project": {
                "name": os.path.basename(target_dir),
                "project_dir": target_dir
            }
        }

        project_yml_path = os.path.join(root_dir, "project.yml")
        with open(project_yml_path, "w", encoding="utf-8") as f:
            yaml.dump(project_config, f, default_flow_style=False, sort_keys=False)

        logger.info(f"Generated root project config at: {project_yml_path}")
        return project_yml_path

    except Exception as e:
        logger.error(f"Failed to set project structure: {str(e)}")
        raise RuntimeError(f"Error while setting project structure: {str(e)}") from e