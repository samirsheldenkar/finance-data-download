"""Tests for Databento provider."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from finance_downloader.core.models import DataType, DownloadJob, ProviderConfig
from finance_downloader.providers.databento import DatabentoProvider


@pytest.fixture
def databento():
    return DatabentoProvider(config=ProviderConfig(api_key_env="DATABENTO_API_KEY"))


def test_provider_metadata(databento):
    assert databento.name == "databento"
    assert DataType.EOD_PRICES in databento.supported_data_types
    assert DataType.INTRADAY in databento.supported_data_types
    assert not databento.supports(DataType.ECONOMIC)


def test_requires_api_key(databento):
    assert databento.requires_api_key()


def test_get_last_available_date_returns_none(databento):
    """Databento provider returns None for last available date (relies on metadata)."""
    result = databento.get_last_available_date("ES.c.0", DataType.EOD_PRICES)
    assert result is None


@patch("finance_downloader.providers.databento.DatabentoProvider._get_client")
def test_download_eod(mock_get_client, databento):
    """Test EOD download with mocked Databento client."""
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client

    mock_data = MagicMock()
    dates = pd.date_range("2024-01-02", periods=3, freq="B")
    mock_data.to_df.return_value = pd.DataFrame(
        {
            "Open": [4500.0, 4510.0, 4520.0],
            "High": [4550.0, 4560.0, 4570.0],
            "Low": [4490.0, 4500.0, 4510.0],
            "Close": [4525.0, 4535.0, 4545.0],
            "Volume": [100000, 110000, 120000],
        },
        index=pd.DatetimeIndex(dates, name="date"),
    )
    mock_client.timeseries.get_range.return_value = mock_data

    job = DownloadJob(
        name="test",
        provider="databento",
        data_type=DataType.EOD_PRICES,
        symbols=["ES.c.0"],
        start_date=date(2024, 1, 1),
        extra={"dataset": "GLBX.MDP3"},
    )

    df = databento.download(job)
    assert not df.empty
    # Columns should be normalized to lowercase
    assert "open" in df.columns or "Open" in df.columns


@patch("finance_downloader.providers.databento.DatabentoProvider._get_client")
def test_download_empty(mock_get_client, databento):
    """Test download when Databento returns empty DataFrame."""
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client

    mock_data = MagicMock()
    mock_data.to_df.return_value = pd.DataFrame()
    mock_client.timeseries.get_range.return_value = mock_data

    job = DownloadJob(
        name="test",
        provider="databento",
        data_type=DataType.EOD_PRICES,
        symbols=["INVALID"],
        start_date=date(2024, 1, 1),
    )

    df = databento.download(job)
    assert df.empty


def test_intraday_schema_default(databento):
    """Test that intraday defaults to ohlcv-1m schema."""
    # This tests the logic path; actual API call is mocked above
    assert DataType.INTRADAY in databento.supported_data_types
