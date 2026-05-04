"""Configuration loading and validation."""

from __future__ import annotations

import json
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
from pydantic import ValidationError

from finance_downloader.core.models import AppConfig


def _load_env(config_path: str | Path | None = None) -> None:
    """Load .env file from config file directory or CWD."""
    env_paths = [Path.cwd() / ".env"]
    if config_path:
        env_paths.insert(0, Path(config_path).parent / ".env")
    for env_path in env_paths:
        if env_path.exists():
            load_dotenv(env_path)
            logger.debug(f"Loaded environment from {env_path}")
            return
    load_dotenv()


def load_config(config_path: str | Path) -> AppConfig:
    """
    Load and validate a JSON configuration file.

    Raises:
        FileNotFoundError: If config file doesn't exist.
        ValueError: If config is invalid.
    """
    _load_env(config_path)
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

    logger.info(
        f"Loaded config from {path}: "
        f"{len(config.jobs)} jobs, {len(config.providers)} providers"
    )
    return config
