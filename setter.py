import os
import yaml
from yamlpipe.utility.logger import setup_logger, get_logger

logger = get_logger("ProjectSetter")


def set_project(project_dir: str = ".") -> str:
    """Creates project directory structure including raw YAML configs, custom modules,
    variable definitions, templates, compilation cache directories, project.yml,
    pipe.log, and .gitignore file inside the target project directory.
    """
    try:
        target_dir = os.path.abspath(project_dir)

        # 1. Initialize logging before any file/folder creation
        setup_logger(target_dir)
        logger.info(f"Initializing project setup at target directory: {target_dir}")

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

        # 2. Create project.yml inside target_dir
        project_config = {
            "project": {
                "name": os.path.basename(target_dir),
                "project_dir": target_dir,
            }
        }

        project_yml_path = os.path.join(target_dir, "project.yml")
        with open(project_yml_path, "w", encoding="utf-8") as f:
            yaml.dump(project_config, f, default_flow_style=False, sort_keys=False)

        logger.info(f"Generated root project config at: {project_yml_path}")

        # 3. Create .gitignore inside target_dir (ignore parsed cache and log files)
        gitignore_path = os.path.join(target_dir, ".gitignore")
        gitignore_content = "/parsed\npipe.log\n"

        with open(gitignore_path, "w", encoding="utf-8") as f:
            f.write(gitignore_content)

        logger.info(f"Generated .gitignore at: {gitignore_path}")

        return project_yml_path

    except Exception as e:
        logger.error(f"Failed to set project structure: {str(e)}")
        raise RuntimeError(f"Error while setting project structure: {str(e)}") from e