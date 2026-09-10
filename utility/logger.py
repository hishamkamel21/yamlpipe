import logging
import os
import sys
from typing import Optional

# Global variable to track log file path
_LOG_FILE_PATH: Optional[str] = None


def setup_logger(project_dir: str = ".") -> None:
    """Configures the root/library logger to write to console and <project_dir>/pipe.log."""
    global _LOG_FILE_PATH

    target_dir = os.path.abspath(project_dir)
    os.makedirs(target_dir, exist_ok=True)

    log_file = os.path.join(target_dir, "pipe.log")
    _LOG_FILE_PATH = log_file

    # Define log message format
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Get root logger or module-specific parent logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Clear existing handlers to prevent duplicate log outputs
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # 1. Console Handler (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    root_logger.addHandler(console_handler)

    # 2. File Handler (pipe.log in project root)
    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    """Returns a named logger instance inheriting project log configurations."""
    return logging.getLogger(name)