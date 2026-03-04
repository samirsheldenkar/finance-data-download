"""Shared test fixtures."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pytest

from finance_downloader.core.models import (
    AppConfig,
    DataType,
    DownloadJob,
    ProviderConfig,
    StorageFormat,
)
from finance_downloader.core.storage import DataStorage


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    """Temporary directory for test data output."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    return data_dir


@pytest.fixture
def storage(tmp_data_dir: Path) -> DataStorage:
    """DataStorage instance writing to temp directory."""
    return DataStorage(tmp_data_dir, StorageFormat.PARQUET)


@pytest.fixture
def csv_storage(tmp_data_dir: Path) -> DataStorage:
    """DataStorage instance writing CSV to temp directory."""
    return DataStorage(tmp_data_dir, StorageFormat.CSV)


@pytest.fixture
def sample_eod_df() -> pd.DataFrame:
    """Sample EOD price DataFrame."""
    dates = pd.date_range("2024-01-02", periods=5, freq="B")
    return pd.DataFrame(
        {
            "open": [150.0, 151.0, 152.0, 153.0, 154.0],
            "high": [155.0, 156.0, 157.0, 158.0, 159.0],
            "low": [149.0, 150.0, 151.0, 152.0, 153.0],
            "close": [154.0, 155.0, 156.0, 157.0, 158.0],
            "volume": [1000000, 1100000, 1200000, 1300000, 1400000],
        },
        index=pd.DatetimeIndex(dates, name="date"),
    )


@pytest.fixture
def sample_economic_df() -> pd.DataFrame:
    """Sample economic time series DataFrame."""
    dates = pd.date_range("2024-01-01", periods=4, freq="MS")
    return pd.DataFrame(
        {"value": [3.7, 3.8, 3.6, 3.5]},
        index=pd.DatetimeIndex(dates, name="date"),
    )


@pytest.fixture
def sample_job() -> DownloadJob:
    """Sample download job."""
    return DownloadJob(
        name="test_job",
        provider="yahoo",
        data_type=DataType.EOD_PRICES,
        symbols=["AAPL", "MSFT"],
        start_date=date(2024, 1, 1),
        output_subdir="test",
    )


@pytest.fixture
def sample_config(tmp_data_dir: Path) -> AppConfig:
    """Sample app config pointing to temp directory."""
    return AppConfig(
        output_dir=str(tmp_data_dir),
        storage_format=StorageFormat.PARQUET,
        providers={"yahoo": ProviderConfig()},
        jobs=[
            DownloadJob(
                name="test",
                provider="yahoo",
                data_type=DataType.EOD_PRICES,
                symbols=["AAPL"],
                start_date=date(2024, 1, 1),
                output_subdir="equities",
            )
        ],
    )
