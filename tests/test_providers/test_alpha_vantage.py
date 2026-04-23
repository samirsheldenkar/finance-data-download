"""Tests for Alpha Vantage provider."""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pytest

from finance_downloader.core.models import DataType, DownloadJob, ProviderConfig
from finance_downloader.providers.alpha_vantage import AlphaVantageProvider


@pytest.fixture
def alpha_vantage():
    return AlphaVantageProvider(config=ProviderConfig(api_key_env="ALPHAVANTAGE_API_KEY"))


def test_provider_metadata(alpha_vantage):
    assert alpha_vantage.name == "alpha_vantage"
    assert DataType.EOD_PRICES in alpha_vantage.supported_data_types
    assert DataType.ECONOMIC in alpha_vantage.supported_data_types
    assert not alpha_vantage.supports(DataType.INTRADAY)


def test_requires_api_key(alpha_vantage):
    assert alpha_vantage.requires_api_key()


@patch.object(AlphaVantageProvider, "_get")
def test_download_eod(mock_get, alpha_vantage):
    """Test EOD download with mocked API response."""
    mock_get.return_value = {
        "Time Series (Daily)": {
            "2024-01-02": {
                "1. open": "150.0",
                "2. high": "155.0",
                "3. low": "149.0",
                "4. close": "154.0",
                "5. adjusted close": "153.5",
                "6. volume": "1000000",
            },
            "2024-01-03": {
                "1. open": "154.0",
                "2. high": "158.0",
                "3. low": "153.0",
                "4. close": "157.0",
                "5. adjusted close": "156.5",
                "6. volume": "1100000",
            },
        }
    }

    job = DownloadJob(
        name="test",
        provider="alpha_vantage",
        data_type=DataType.EOD_PRICES,
        symbols=["AAPL"],
        start_date=date(2024, 1, 1),
    )

    df = alpha_vantage.download(job)
    assert not df.empty
    assert "open" in df.columns
    assert "close" in df.columns
    assert df.index.name == "date"


@patch.object(AlphaVantageProvider, "_get")
def test_download_eod_empty(mock_get, alpha_vantage):
    """Test download when API returns no data."""
    mock_get.return_value = {"Time Series (Daily)": {}}

    job = DownloadJob(
        name="test",
        provider="alpha_vantage",
        data_type=DataType.EOD_PRICES,
        symbols=["INVALID"],
    )

    df = alpha_vantage.download(job)
    assert df.empty


@patch.object(AlphaVantageProvider, "_get")
def test_download_economic(mock_get, alpha_vantage):
    """Test economic indicator download."""
    mock_get.return_value = {
        "name": "Real Gross Domestic Product",
        "data": [
            {"date": "2024-01-01", "value": "22.5"},
            {"date": "2024-04-01", "value": "23.0"},
        ],
    }

    job = DownloadJob(
        name="test",
        provider="alpha_vantage",
        data_type=DataType.ECONOMIC,
        symbols=["GDP"],
        start_date=date(2024, 1, 1),
    )

    df = alpha_vantage.download(job)
    assert not df.empty
    assert "value" in df.columns


def test_get_last_available_date_returns_none(alpha_vantage):
    """Alpha Vantage returns None (relies on metadata sidecar)."""
    assert alpha_vantage.get_last_available_date("AAPL", DataType.EOD_PRICES) is None
