"""Logging configuration using loguru."""

from __future__ import annotations

import sys

from loguru import logger


def setup_logging(level: str = "INFO", log_file: str | None = None) -> None:
    """
    Configure loguru logger for the application.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR).
        log_file: Optional file path for log output with rotation.
    """
    # Remove default handler
    logger.remove()

    # Console handler with colour
    fmt = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    )
    logger.add(sys.stderr, format=fmt, level=level, colorize=True)

    # File handler (optional)
    if log_file:
        logger.add(
            log_file,
            level=level,
            rotation="10 MB",
            retention="30 days",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
        )
