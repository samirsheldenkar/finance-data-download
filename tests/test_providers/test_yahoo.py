"""Tests for Yahoo Finance provider."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from finance_downloader.core.models import DataType, DownloadJob
from finance_downloader.providers.yahoo import YahooFinanceProvider


@pytest.fixture
def yahoo():
    return YahooFinanceProvider()


def test_provider_metadata(yahoo):
    """Test provider name and supported types."""
    assert yahoo.name == "yahoo"
    assert DataType.EOD_PRICES in yahoo.supported_data_types
    assert DataType.DIVIDENDS in yahoo.supported_data_types
    assert DataType.SPLITS in yahoo.supported_data_types
    assert DataType.FUNDAMENTALS in yahoo.supported_data_types


def test_no_api_key_required(yahoo):
    """Yahoo Finance should not require an API key."""
    assert not yahoo.requires_api_key()
    assert yahoo.is_available()


def test_supports_data_types(yahoo):
    """Test supports() method."""
    assert yahoo.supports(DataType.EOD_PRICES)
    assert yahoo.supports(DataType.DIVIDENDS)
    assert not yahoo.supports(DataType.ECONOMIC)


@patch("finance_downloader.providers.yahoo.yf.Ticker")
def test_download_eod(mock_ticker_cls, yahoo):
    """Test EOD download with mocked yfinance."""
    mock_ticker = MagicMock()
    mock_ticker_cls.return_value = mock_ticker

    dates = pd.date_range("2024-01-02", periods=3, freq="B")
    mock_ticker.history.return_value = pd.DataFrame(
        {
            "Open": [150.0, 151.0, 152.0],
            "High": [155.0, 156.0, 157.0],
            "Low": [149.0, 150.0, 151.0],
            "Close": [154.0, 155.0, 156.0],
            "Volume": [1000000, 1100000, 1200000],
        },
        index=dates,
    )

    job = DownloadJob(
        name="test",
        provider="yahoo",
        data_type=DataType.EOD_PRICES,
        symbols=["AAPL"],
        start_date=date(2024, 1, 1),
    )

    df = yahoo.download(job)
    assert len(df) == 3
    assert "open" in df.columns
    assert "close" in df.columns
    assert df.index.name == "date"


@patch("finance_downloader.providers.yahoo.yf.Ticker")
def test_download_eod_empty(mock_ticker_cls, yahoo):
    """Test EOD download when no data is returned."""
    mock_ticker = MagicMock()
    mock_ticker_cls.return_value = mock_ticker
    mock_ticker.history.return_value = pd.DataFrame()

    job = DownloadJob(
        name="test",
        provider="yahoo",
        data_type=DataType.EOD_PRICES,
        symbols=["INVALID"],
    )

    df = yahoo.download(job)
    assert df.empty


@patch("finance_downloader.providers.yahoo.yf.Ticker")
def test_download_dividends(mock_ticker_cls, yahoo):
    """Test dividend download with mocked yfinance."""
    mock_ticker = MagicMock()
    mock_ticker_cls.return_value = mock_ticker

    dates = pd.DatetimeIndex([pd.Timestamp("2024-02-09"), pd.Timestamp("2024-05-10")])
    mock_ticker.dividends = pd.Series([0.24, 0.25], index=dates, name="Dividends")

    job = DownloadJob(
        name="test",
        provider="yahoo",
        data_type=DataType.DIVIDENDS,
        symbols=["AAPL"],
        start_date=date(2024, 1, 1),
    )

    df = yahoo.download(job)
    assert len(df) == 2
    assert "amount" in df.columns
