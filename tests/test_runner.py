"""Tests for DownloadRunner."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest

from finance_downloader.core.models import (
    AppConfig,
    DataType,
    DownloadJob,
    ProviderConfig,
    StorageFormat,
)
from finance_downloader.core.base_provider import BaseProvider
from finance_downloader.core.registry import ProviderRegistry
from finance_downloader.runner import DownloadRunner


class MockProvider(BaseProvider):
    """Mock provider for testing."""

    name = "mock"
    supported_data_types = [DataType.EOD_PRICES]

    def __init__(self, config=None):
        super().__init__(config)
        self.download_calls = []

    def requires_api_key(self):
        return False

    def is_available(self):
        return True

    def download(self, job, start_override=None):
        self.download_calls.append((job, start_override))
        dates = pd.date_range("2024-01-02", periods=5, freq="B")
        return pd.DataFrame(
            {
                "open": [150.0] * 5,
                "high": [155.0] * 5,
                "low": [149.0] * 5,
                "close": [154.0] * 5,
                "volume": [1000000] * 5,
            },
            index=pd.DatetimeIndex(dates, name="date"),
        )

    def get_last_available_date(self, symbol, data_type):
        return None


@pytest.fixture
def mock_registry():
    reg = ProviderRegistry()
    reg.register(MockProvider)
    return reg


@pytest.fixture
def runner_config(tmp_path):
    return AppConfig(
        output_dir=str(tmp_path / "data"),
        storage_format=StorageFormat.PARQUET,
        providers={"mock": ProviderConfig()},
        jobs=[
            DownloadJob(
                name="test_job",
                provider="mock",
                data_type=DataType.EOD_PRICES,
                symbols=["AAPL", "MSFT"],
                start_date=date(2024, 1, 1),
                output_subdir="equities",
            )
        ],
    )


def test_run_all_jobs(runner_config, mock_registry):
    """Test running all jobs returns results."""
    runner = DownloadRunner(runner_config, mock_registry)
    results = runner.run_all_jobs()

    assert len(results) == 1
    result = results[0]
    assert result.job_name == "test_job"
    assert result.successful == 2
    assert result.failed == 0
    assert result.all_succeeded


def test_run_job_unknown_provider(runner_config, mock_registry):
    """Test that unknown provider name produces failures."""
    job = DownloadJob(
        name="bad_job",
        provider="nonexistent",
        data_type=DataType.EOD_PRICES,
        symbols=["AAPL"],
    )
    runner = DownloadRunner(runner_config, mock_registry)
    result = runner.run_job(job)

    assert result.failed == 1
    assert not result.all_succeeded


def test_run_job_unsupported_data_type(runner_config, mock_registry):
    """Test that unsupported data type is caught."""
    job = DownloadJob(
        name="wrong_type",
        provider="mock",
        data_type=DataType.ECONOMIC,
        symbols=["GDP"],
    )
    runner = DownloadRunner(runner_config, mock_registry)
    result = runner.run_job(job)

    assert result.failed == 1


def test_incremental_update(runner_config, mock_registry):
    """Test that second run performs incremental update."""
    runner = DownloadRunner(runner_config, mock_registry)

    # First run
    results1 = runner.run_all_jobs()
    assert results1[0].successful == 2

    # Second run should detect existing data
    results2 = runner.run_all_jobs()
    assert results2[0].successful == 2


def test_adhoc_download(runner_config, mock_registry):
    """Test ad-hoc download."""
    runner = DownloadRunner(runner_config, mock_registry)
    result = runner.run_adhoc(
        provider_name="mock",
        data_type="eod_prices",
        symbols=["TSLA"],
        start_date=date(2024, 1, 1),
    )

    assert result.successful == 1
    assert result.job_name == "adhoc"
