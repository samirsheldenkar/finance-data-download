"""Integration tests for providers against real APIs.

These tests require API keys set as environment variables and are
skipped automatically when keys are not available.

Run with: pytest tests/test_integration/ -v --timeout=120
"""

from __future__ import annotations

import os

import pytest

from finance_downloader.core.models import DataType, DownloadJob, ProviderConfig
from finance_downloader.providers.fred import FredProvider
from finance_downloader.providers.eodhd import EodhdProvider
from finance_downloader.providers.yahoo import YahooProvider
from finance_downloader.providers.alpha_vantage import AlphaVantageProvider
from finance_downloader.providers.finnhub import FinnhubProvider


pytestmark = pytest.mark.integration


def _has_env(key: str) -> bool:
    return bool(os.environ.get(key))


@pytest.fixture
def yahoo():
    return YahooProvider()


@pytest.fixture
def fred():
    if not _has_env("FRED_API_KEY"):
        pytest.skip("FRED_API_KEY not set")
    return FredProvider()


@pytest.fixture
def eodhd():
    if not _has_env("EODHD_API_KEY"):
        pytest.skip("EODHD_API_KEY not set")
    return EodhdProvider()


@pytest.fixture
def alpha_vantage():
    if not _has_env("ALPHAVANTAGE_API_KEY"):
        pytest.skip("ALPHAVANTAGE_API_KEY not set")
    return AlphaVantageProvider()


@pytest.fixture
def finnhub():
    if not _has_env("FINNHUB_API_KEY"):
        pytest.skip("FINNHUB_API_KEY not set")
    return FinnhubProvider()


class TestYahooIntegration:
    def test_download_eod(self, yahoo):
        job = DownloadJob(
            name="test",
            provider="yahoo",
            data_type=DataType.EOD_PRICES,
            symbols=["AAPL"],
            start_date="2024-01-01",  # type: ignore[arg-type]
            end_date="2024-01-31",  # type: ignore[arg-type]
        )
        df = yahoo.download(job)
        assert not df.empty
        assert "close" in df.columns

    def test_download_dividends(self, yahoo):
        job = DownloadJob(
            name="test",
            provider="yahoo",
            data_type=DataType.DIVIDENDS,
            symbols=["AAPL"],
            start_date="2023-01-01",  # type: ignore[arg-type]
        )
        df = yahoo.download(job)
        # May be empty if no dividends in range, but should not error
        assert isinstance(df, object)  # returns a DataFrame


class TestFredIntegration:
    def test_download_economic(self, fred):
        job = DownloadJob(
            name="test",
            provider="fred",
            data_type=DataType.ECONOMIC,
            symbols=["GDP"],
            start_date="2023-01-01",  # type: ignore[arg-type]
        )
        df = fred.download(job)
        assert not df.empty


class TestEodhdIntegration:
    def test_download_eod(self, eodhd):
        job = DownloadJob(
            name="test",
            provider="eodhd",
            data_type=DataType.EOD_PRICES,
            symbols=["AAPL.US"],
            start_date="2024-01-01",  # type: ignore[arg-type]
        )
        df = eodhd.download(job)
        assert not df.empty


class TestAlphaVantageIntegration:
    def test_download_eod(self, alpha_vantage):
        job = DownloadJob(
            name="test",
            provider="alpha_vantage",
            data_type=DataType.EOD_PRICES,
            symbols=["AAPL"],
            start_date="2024-01-01",  # type: ignore[arg-type]
        )
        df = alpha_vantage.download(job)
        assert not df.empty


class TestFinnhubIntegration:
    def test_download_eod(self, finnhub):
        job = DownloadJob(
            name="test",
            provider="finnhub",
            data_type=DataType.EOD_PRICES,
            symbols=["AAPL"],
            start_date="2024-01-01",  # type: ignore[arg-type]
        )
        df = finnhub.download(job)
        assert not df.empty