import logging
import os
import sys
from typing import Optional

# Global cache for initialized loggers to avoid redundant handler setup
_INITIALIZED_LOGGERS = set()
_LOG_FILE_PATH: Optional[str] = None


def _auto_find_project_root() -> str:
    """
    Dynamically locates project root using CWD or upward hierarchy to find project.yml.
    Defaults to current working directory (CWD) if project.yml is not found.
    """
    cwd = os.path.abspath(os.getcwd())
    current_dir = cwd

    while True:
        candidate_config = os.path.join(current_dir, "project.yml")
        if os.path.exists(candidate_config):
            return current_dir

        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:  # Reached filesystem root
            break
        current_dir = parent_dir

    return cwd


def _configure_logger(logger: logging.Logger) -> None:
    """
    Attaches StreamHandler (Console) and FileHandler (pipe.log) automatically.
    Appends logs if pipe.log exists, or creates it automatically if missing.
    """
    global _LOG_FILE_PATH

    # Detect project root dynamically
    project_root = _auto_find_project_root()
    log_file_path = os.path.join(project_root, "pipe.log")
    _LOG_FILE_PATH = log_file_path

    # Ensure directory exists
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Clear pre-existing handlers to prevent duplicate outputs
    if logger.hasHandlers():
        logger.handlers.clear()

    # 1. Console Output Handler (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

    # 2. File Output Handler (pipe.log in project root)
    # mode="a" ensures it creates pipe.log if missing or appends if found
    file_handler = logging.FileHandler(log_file_path, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    logger.addHandler(file_handler)

    logger.setLevel(logging.INFO)


def get_logger(name: str) -> logging.Logger:
    """
    Returns a configured Logger instance. Automatically attaches pipe.log FileHandler
    on first access. No manual setup or initialization required.
    """
    logger = logging.getLogger(name)

    if name not in _INITIALIZED_LOGGERS:
        _configure_logger(logger)
        _INITIALIZED_LOGGERS.add(name)

    return logger