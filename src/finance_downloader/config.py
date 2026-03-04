"""Configuration loading and validation."""

from __future__ import annotations

import json
from pathlib import Path

from loguru import logger
from pydantic import ValidationError

from finance_downloader.core.models import AppConfig


def load_config(config_path: str | Path) -> AppConfig:
    """
    Load and validate a JSON configuration file.

    Raises:
        FileNotFoundError: If config file doesn't exist.
        ValueError: If config is invalid.
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path) as f:
        raw = json.load(f)

    try:
        config = AppConfig(**raw)
    except ValidationError as e:
        logger.error(f"Invalid configuration: {e}")
        raise ValueError(f"Invalid configuration in {path}: {e}") from e

    logger.info(f"Loaded config from {path}: {len(config.jobs)} jobs, {len(config.providers)} providers")
    return config
