import logging
import sys
from pathlib import Path

# Path to log file (saved in project root directory)
LOG_FILE_PATH = Path(__file__).resolve().parent.parent / "pipe.log"


def setup_logger(name: str = "yamlpipe") -> logging.Logger:
    """Configures and returns a thread-safe logger with console and file output."""
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if logger is imported multiple times
    if logger.hasHandlers():
        return logger

    logger.setLevel(logging.INFO)

    # Log Formatter
    log_format = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 1. File Handler (Writes to pipe.log)
    file_handler = logging.FileHandler(LOG_FILE_PATH, mode="a", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(log_format)

    # 2. Console Handler (Standard Output)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(log_format)

    # Attach handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def get_logger(module_name: str) -> logging.Logger:
    """Helper function to get a named logger for specific modules."""
    setup_logger()  # Ensure root/base logger is initialized
    return logging.getLogger(f"{module_name}") 