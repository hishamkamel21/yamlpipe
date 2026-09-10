import logging
import os
import sys
import yaml
from typing import Optional

_LOG_FILE_PATH: Optional[str] = None


def find_project_root(explicit_project_dir: Optional[str] = None) -> str:
    """
    Dynamically locates the active project directory root by searching for project.yml.
    """
    cwd = os.path.abspath(os.getcwd())

    # 1. Explicit directory argument
    if explicit_project_dir:
        resolved_path = (
            explicit_project_dir
            if os.path.isabs(explicit_project_dir)
            else os.path.abspath(os.path.join(cwd, explicit_project_dir))
        )
        if os.path.exists(os.path.join(resolved_path, "project.yml")) or os.path.exists(resolved_path):
            return resolved_path

    # 2. Search upward in directory hierarchy for project.yml
    current_dir = cwd
    while True:
        candidate_config = os.path.join(current_dir, "project.yml")
        if os.path.exists(candidate_config):
            try:
                with open(candidate_config, "r", encoding="utf-8") as f:
                    config = yaml.safe_load(f)
                    return config.get("project", {}).get("project_dir", current_dir)
            except Exception:
                return current_dir

        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:  # Filesystem root reached
            break
        current_dir = parent_dir

    # 3. Check direct subdirectories for project.yml
    try:
        subdirs = [
            os.path.join(cwd, d)
            for d in os.listdir(cwd)
            if os.path.isdir(os.path.join(cwd, d)) and not d.startswith((".", "_"))
        ]
        projects_found = [d for d in subdirs if os.path.exists(os.path.join(d, "project.yml"))]
        if len(projects_found) == 1:
            return projects_found[0]
    except Exception:
        pass

    # 4. Default fallback to CWD
    return cwd


def _setup_root_logger() -> str:
    """
    Configures the root logger automatically to write to console AND <project_root>/pipe.log.
    Creates pipe.log if it doesn't exist, or appends to it if found.
    """
    global _LOG_FILE_PATH

    project_root = find_project_root()
    log_file_path = os.path.join(project_root, "pipe.log")
    _LOG_FILE_PATH = log_file_path

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Prevent duplicate handlers
    if not root_logger.handlers:
        # 1. Console Output (stdout)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)
        root_logger.addHandler(console_handler)

        # 2. File Output (pipe.log)
        # mode="a" automatically creates file if missing or appends if present
        file_handler = logging.FileHandler(log_file_path, mode="a", encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)
        root_logger.addHandler(file_handler)

    return log_file_path


# Initialize root logger automatically when logger module is imported
_setup_root_logger()


def get_logger(name: str) -> logging.Logger:
    """
    Returns a child logger inheriting the automatic console + pipe.log output.
    """
    return logging.getLogger(name)