"""Tests for EODHD provider."""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pytest

from finance_downloader.core.models import DataType, DownloadJob, ProviderConfig
from finance_downloader.providers.eodhd import EodhdProvider


@pytest.fixture
def eodhd():
    return EodhdProvider(config=ProviderConfig(api_key_env="EODHD_API_KEY"))


def test_provider_metadata(eodhd):
    assert eodhd.name == "eodhd"
    assert DataType.EOD_PRICES in eodhd.supported_data_types
    assert DataType.DIVIDENDS in eodhd.supported_data_types
    assert DataType.SPLITS in eodhd.supported_data_types
    assert DataType.FUNDAMENTALS in eodhd.supported_data_types
    assert not eodhd.supports(DataType.ECONOMIC)


def test_requires_api_key(eodhd):
    assert eodhd.requires_api_key()


def test_symbol_formatting(eodhd):
    """Test that symbols without exchange suffix get .US appended."""
    assert eodhd._symbol("AAPL") == "AAPL.US"
    assert eodhd._symbol("VOD.LSE") == "VOD.LSE"
    assert eodhd._symbol("BMW.XETRA") == "BMW.XETRA"


@patch.object(EodhdProvider, "_get")
def test_download_eod(mock_get, eodhd):
    """Test EOD prices download with mocked API."""
    mock_get.return_value = [
        {
            "date": "2024-01-02",
            "open": 150.0,
            "high": 155.0,
            "low": 149.0,
            "close": 154.0,
            "volume": 1000000,
            "adjusted_close": 153.5,
        },
        {
            "date": "2024-01-03",
            "open": 154.0,
            "high": 158.0,
            "low": 153.0,
            "close": 157.0,
            "volume": 1100000,
            "adjusted_close": 156.5,
        },
    ]

    job = DownloadJob(
        name="test",
        provider="eodhd",
        data_type=DataType.EOD_PRICES,
        symbols=["AAPL"],
        start_date=date(2024, 1, 1),
    )

    df = eodhd.download(job)
    assert not df.empty
    assert "open" in df.columns
    assert "close" in df.columns
    assert df.index.name == "date"


@patch.object(EodhdProvider, "_get")
def test_download_eod_empty(mock_get, eodhd):
    """Test EOD download when API returns empty."""
    mock_get.return_value = []

    job = DownloadJob(
        name="test",
        provider="eodhd",
        data_type=DataType.EOD_PRICES,
        symbols=["INVALID"],
        start_date=date(2024, 1, 1),
    )

    df = eodhd.download(job)
    assert df.empty


@patch.object(EodhdProvider, "_get")
def test_download_dividends(mock_get, eodhd):
    """Test dividend download with mocked API."""
    mock_get.return_value = [
        {"date": "2024-02-09", "value": 0.24},
        {"date": "2024-05-10", "value": 0.25},
    ]

    job = DownloadJob(
        name="test",
        provider="eodhd",
        data_type=DataType.DIVIDENDS,
        symbols=["AAPL"],
        start_date=date(2024, 1, 1),
    )

    df = eodhd.download(job)
    assert not df.empty
    assert "amount" in df.columns


@patch.object(EodhdProvider, "_get")
def test_get_last_available_date(mock_get, eodhd):
    """Test getting last available date from EODHD."""
    mock_get.return_value = [{"date": "2024-12-31", "open": 150, "close": 152}]

    result = eodhd.get_last_available_date("AAPL", DataType.EOD_PRICES)
    assert result == date(2024, 12, 31)
