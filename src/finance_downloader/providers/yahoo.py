"""Yahoo Finance provider using yfinance library."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import yfinance as yf
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from finance_downloader.core.base_provider import BaseProvider
from finance_downloader.core.models import DataType, DownloadJob, ProviderConfig


class YahooFinanceProvider(BaseProvider):
    """
    Yahoo Finance data provider (completely free, no API key required).

    Supports: eod_prices, dividends, splits, fundamentals
    Uses the yfinance library.
    """

    name = "yahoo"
    supported_data_types = [
        DataType.EOD_PRICES,
        DataType.DIVIDENDS,
        DataType.SPLITS,
        DataType.FUNDAMENTALS,
    ]

    def __init__(self, config: ProviderConfig | None = None) -> None:
        super().__init__(config)

    def requires_api_key(self) -> bool:
        return False

    def is_available(self) -> bool:
        return True

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    def download(self, job: DownloadJob, start_override: date | None = None) -> pd.DataFrame:
        symbol = job.symbols[0]
        data_type = job.data_type

        if data_type == DataType.EOD_PRICES:
            return self._download_eod(symbol, job, start_override)
        elif data_type == DataType.DIVIDENDS:
            return self._download_dividends(symbol, job, start_override)
        elif data_type == DataType.SPLITS:
            return self._download_splits(symbol, job, start_override)
        elif data_type == DataType.FUNDAMENTALS:
            return self._download_fundamentals(symbol)
        else:
            raise ValueError(f"Unsupported data type: {data_type}")

    def _download_eod(
        self, symbol: str, job: DownloadJob, start_override: date | None
    ) -> pd.DataFrame:
        """Download end-of-day OHLCV data."""
        start = start_override or job.start_date
        end = job.end_date or (date.today() + timedelta(days=1))

        ticker = yf.Ticker(symbol)
        df = ticker.history(
            start=str(start) if start else None,
            end=str(end),
            auto_adjust=False,
        )

        if df.empty:
            return df

        # Normalize column names
        df = df.rename(columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
            "Adj Close": "adj_close",
        })

        # Keep only standard columns
        cols = ["open", "high", "low", "close", "volume", "adj_close"]
        available = [c for c in cols if c in df.columns]
        df = df[available]

        df.index.name = "date"
        return df

    def _download_dividends(
        self, symbol: str, job: DownloadJob, start_override: date | None
    ) -> pd.DataFrame:
        """Download dividend history."""
        ticker = yf.Ticker(symbol)
        divs = ticker.dividends

        if divs.empty:
            return pd.DataFrame()

        df = divs.to_frame(name="amount")
        df.index.name = "date"

        # Filter by date range
        start = start_override or job.start_date
        if start:
            df = df[df.index >= pd.Timestamp(start)]

        return df

    def _download_splits(
        self, symbol: str, job: DownloadJob, start_override: date | None
    ) -> pd.DataFrame:
        """Download stock split history."""
        ticker = yf.Ticker(symbol)
        splits = ticker.splits

        if splits.empty:
            return pd.DataFrame()

        df = splits.to_frame(name="ratio")
        df.index.name = "date"

        start = start_override or job.start_date
        if start:
            df = df[df.index >= pd.Timestamp(start)]

        return df

    def _download_fundamentals(self, symbol: str) -> pd.DataFrame:
        """Download financial statements (income, balance sheet, cash flow)."""
        ticker = yf.Ticker(symbol)

        frames = {}
        for name, attr in [
            ("income_stmt", "quarterly_income_stmt"),
            ("balance_sheet", "quarterly_balance_sheet"),
            ("cashflow", "quarterly_cashflow"),
        ]:
            data = getattr(ticker, attr, None)
            if data is not None and not data.empty:
                # yfinance returns dates as columns, items as rows — transpose
                transposed = data.T
                transposed.columns = [f"{name}_{c}" for c in transposed.columns]
                frames[name] = transposed

        if not frames:
            return pd.DataFrame()

        # Merge all statements on their date index
        combined = pd.concat(frames.values(), axis=1)
        combined.index.name = "date"
        combined = combined.sort_index()
        return combined

    def get_last_available_date(self, symbol: str, data_type: DataType) -> date | None:
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="5d")
            if not hist.empty:
                return hist.index[-1].date()
        except Exception as e:
            logger.debug(f"Could not get last date for {symbol}: {e}")
        return None
