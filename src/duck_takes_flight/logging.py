"""
Logging configuration for Duck Takes Flight.
"""

import logging
import os
import sys
from typing import Optional


def configure_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
    component: str = "duck-takes-flight",
) -> logging.Logger:
    """
    Configure logging for the application.

    Args:
        level: The logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional path to a log file
        component: The component name to use in the logger

    Returns:
        A configured logger instance
    """
    # Convert string level to logging level
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level}")

    # Create logger
    logger = logging.getLogger(component)
    logger.setLevel(numeric_level)
    logger.handlers = []  # Clear any existing handlers

    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Create file handler if log_file is specified
    if log_file:
        # Ensure the directory exists
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


# Default logger
logger = configure_logging()
