"""Tests for Finnhub provider."""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pytest

from finance_downloader.core.models import DataType, DownloadJob, ProviderConfig
from finance_downloader.providers.finnhub import FinnhubProvider


@pytest.fixture
def finnhub():
    return FinnhubProvider(config=ProviderConfig(api_key_env="FINNHUB_API_KEY"))


def test_provider_metadata(finnhub):
    assert finnhub.name == "finnhub"
    assert DataType.EOD_PRICES in finnhub.supported_data_types
    assert DataType.FUNDAMENTALS in finnhub.supported_data_types
    assert not finnhub.supports(DataType.ECONOMIC)


def test_requires_api_key(finnhub):
    assert finnhub.requires_api_key()


@patch.object(FinnhubProvider, "_get")
def test_download_candles(mock_get, finnhub):
    """Test EOD candle download with mocked Finnhub API."""
    mock_get.return_value = {
        "s": "ok",
        "t": [1704067200, 1704153600],  # 2024-01-01, 2024-01-02
        "o": [150.0, 151.0],
        "h": [155.0, 156.0],
        "l": [149.0, 150.0],
        "c": [154.0, 155.0],
        "v": [1000000, 1100000],
    }

    job = DownloadJob(
        name="test",
        provider="finnhub",
        data_type=DataType.EOD_PRICES,
        symbols=["AAPL"],
        start_date=date(2024, 1, 1),
    )

    df = finnhub.download(job)
    assert not df.empty
    assert "open" in df.columns
    assert "close" in df.columns
    assert df.index.name == "date"


@patch.object(FinnhubProvider, "_get")
def test_download_candles_no_data(mock_get, finnhub):
    """Test download when Finnhub returns no data."""
    mock_get.return_value = {"s": "no_data"}

    job = DownloadJob(
        name="test",
        provider="finnhub",
        data_type=DataType.EOD_PRICES,
        symbols=["INVALID_SYMBOL"],
    )

    df = finnhub.download(job)
    assert df.empty


@patch.object(FinnhubProvider, "_get")
def test_download_financials(mock_get, finnhub):
    """Test financials download with mocked Finnhub API."""
    mock_get.return_value = {
        "data": [
            {
                "filedDate": "2024-11-01",
                "period": "Q3",
                "year": 2024,
                "quarter": 3,
                "form": "10-Q",
                "report": {
                    "bs": [
                        {"concept": "Assets", "value": 350000000000},
                        {"concept": "Liabilities", "value": 200000000000},
                    ]
                },
            }
        ]
    }

    job = DownloadJob(
        name="test",
        provider="finnhub",
        data_type=DataType.FUNDAMENTALS,
        symbols=["AAPL"],
    )

    df = finnhub.download(job)
    assert not df.empty


def test_unsupported_data_type(finnhub):
    """Test that unsupported data type raises ValueError."""
    job = DownloadJob(
        name="test",
        provider="finnhub",
        data_type=DataType.ECONOMIC,
        symbols=["GDP"],
    )

    with pytest.raises(ValueError, match="Unsupported data type"):
        finnhub.download(job)
