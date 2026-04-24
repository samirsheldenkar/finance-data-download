"""Tests for FRED provider."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from finance_downloader.core.models import DataType, DownloadJob, ProviderConfig
from finance_downloader.providers.fred import FredProvider


@pytest.fixture
def fred():
    return FredProvider(config=ProviderConfig(api_key_env="FRED_API_KEY"))


def test_provider_metadata(fred):
    assert fred.name == "fred"
    assert DataType.ECONOMIC in fred.supported_data_types
    assert not fred.supports(DataType.EOD_PRICES)


def test_requires_api_key(fred):
    assert fred.requires_api_key()


def test_is_available_with_key(monkeypatch):
    monkeypatch.setenv("FRED_API_KEY", "test_key")
    provider = FredProvider()
    assert provider.is_available()


def test_not_available_without_key(monkeypatch):
    """Provider should not be available when API key env var is not set."""
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    provider = FredProvider(config=ProviderConfig(api_key_env="FRED_API_KEY"))
    assert not provider.is_available()


@patch("finance_downloader.providers.fred.FredProvider._get_client")
def test_download_economic(mock_get_client, fred):
    """Test economic data download with mocked fredapi client."""
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client

    series = pd.Series(
        [3.7, 3.8, 3.6, 3.5],
        index=pd.DatetimeIndex(
            pd.date_range("2024-01-01", periods=4, freq="MS"), name="date"
        ),
        name="GDP",
    )
    mock_client.get_series.return_value = series

    job = DownloadJob(
        name="test",
        provider="fred",
        data_type=DataType.ECONOMIC,
        symbols=["GDP"],
        start_date=date(2024, 1, 1),
    )

    df = fred.download(job)
    assert not df.empty
    assert "value" in df.columns
    assert len(df) == 4


@patch("finance_downloader.providers.fred.FredProvider._get_client")
def test_download_empty(mock_get_client, fred):
    """Test download when FRED returns empty series."""
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    mock_client.get_series.return_value = pd.Series(dtype=float)

    job = DownloadJob(
        name="test",
        provider="fred",
        data_type=DataType.ECONOMIC,
        symbols=["INVALID_SERIES"],
        start_date=date(2024, 1, 1),
    )

    df = fred.download(job)
    assert df.empty


@patch("finance_downloader.providers.fred.FredProvider._get_client")
def test_get_last_available_date(mock_get_client, fred):
    """Test getting last available date from FRED."""
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client

    mock_info = MagicMock()
    mock_info.observation_end = "2024-12-31"
    mock_client.get_series_info.return_value = mock_info

    result = fred.get_last_available_date("GDP", DataType.ECONOMIC)
    assert result == date(2024, 12, 31)
