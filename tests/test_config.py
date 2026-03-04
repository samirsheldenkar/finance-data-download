"""Tests for config loading and validation."""

from __future__ import annotations

import json

import pytest

from finance_downloader.config import load_config
from finance_downloader.core.models import AppConfig, StorageFormat


def test_load_valid_config(tmp_path):
    """Test loading a valid config file."""
    config_data = {
        "output_dir": "./test_data",
        "storage_format": "parquet",
        "providers": {"yahoo": {}},
        "jobs": [
            {
                "name": "test",
                "provider": "yahoo",
                "data_type": "eod_prices",
                "symbols": ["AAPL"],
            }
        ],
    }
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(config_data))

    config = load_config(config_file)
    assert isinstance(config, AppConfig)
    assert config.output_dir == "./test_data"
    assert config.storage_format == StorageFormat.PARQUET
    assert len(config.jobs) == 1
    assert config.jobs[0].symbols == ["AAPL"]


def test_load_config_file_not_found():
    """Test error on missing config file."""
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/config.json")


def test_load_invalid_config(tmp_path):
    """Test error on invalid config (bad data type)."""
    config_data = {
        "jobs": [
            {
                "name": "bad",
                "provider": "yahoo",
                "data_type": "invalid_type",
                "symbols": ["AAPL"],
            }
        ]
    }
    config_file = tmp_path / "bad_config.json"
    config_file.write_text(json.dumps(config_data))

    with pytest.raises(ValueError, match="Invalid configuration"):
        load_config(config_file)


def test_load_config_with_defaults(tmp_path):
    """Test that defaults are applied for missing optional fields."""
    config_data = {
        "jobs": [
            {
                "name": "minimal",
                "provider": "yahoo",
                "data_type": "eod_prices",
                "symbols": ["MSFT"],
            }
        ]
    }
    config_file = tmp_path / "minimal.json"
    config_file.write_text(json.dumps(config_data))

    config = load_config(config_file)
    assert config.output_dir == "./data"
    assert config.storage_format == StorageFormat.PARQUET
    assert config.log_level == "INFO"
