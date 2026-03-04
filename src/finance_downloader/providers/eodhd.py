"""EODHD (End of Day Historical Data) provider."""

from __future__ import annotations

from datetime import date

import pandas as pd
import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from finance_downloader.core.base_provider import BaseProvider
from finance_downloader.core.models import DataType, DownloadJob, ProviderConfig
from finance_downloader.utils.rate_limiter import RateLimiter

BASE_URL = "https://eodhd.com/api"


class EodhdProvider(BaseProvider):
    """
    EODHD data provider for global equities, ETFs, and more.

    Free tier: 20 API calls/day, 1 year of history.
    Symbols use EODHD format: TICKER.EXCHANGE (e.g. AAPL.US, VOD.LSE).
    If no exchange suffix is given, '.US' is assumed.
    """

    name = "eodhd"
    supported_data_types = [
        DataType.EOD_PRICES,
        DataType.DIVIDENDS,
        DataType.SPLITS,
        DataType.FUNDAMENTALS,
    ]

    def __init__(self, config: ProviderConfig | None = None) -> None:
        if config is None:
            config = ProviderConfig(api_key_env="EODHD_API_KEY")
        super().__init__(config)
        self._limiter = RateLimiter(
            calls_per_minute=config.rate_limit_per_minute or 20
        )

    def _symbol(self, symbol: str) -> str:
        """Ensure symbol has an exchange suffix."""
        return symbol if "." in symbol else f"{symbol}.US"

    def _get(self, endpoint: str, params: dict | None = None) -> dict | list:
        """Make an authenticated GET request to EODHD API."""
        self._limiter.acquire()
        params = params or {}
        params["api_token"] = self.api_key
        params["fmt"] = "json"

        url = f"{BASE_URL}/{endpoint}"
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

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
            raise ValueError(f"Unsupported data type for EODHD: {data_type}")

    def _download_eod(
        self, symbol: str, job: DownloadJob, start_override: date | None
    ) -> pd.DataFrame:
        eodhd_sym = self._symbol(symbol)
        params: dict = {}
        start = start_override or job.start_date
        if start:
            params["from"] = str(start)
        if job.end_date:
            params["to"] = str(job.end_date)

        data = self._get(f"eod/{eodhd_sym}", params)

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
        df = df.rename(columns={
            "adjusted_close": "adj_close",
        })

        cols = ["open", "high", "low", "close", "volume", "adj_close"]
        available = [c for c in cols if c in df.columns]
        return df[available]

    def _download_dividends(
        self, symbol: str, job: DownloadJob, start_override: date | None
    ) -> pd.DataFrame:
        eodhd_sym = self._symbol(symbol)
        params: dict = {}
        start = start_override or job.start_date
        if start:
            params["from"] = str(start)
        if job.end_date:
            params["to"] = str(job.end_date)

        data = self._get(f"div/{eodhd_sym}", params)

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")

        if "value" in df.columns:
            df = df.rename(columns={"value": "amount"})

        return df[["amount"]] if "amount" in df.columns else df

    def _download_splits(
        self, symbol: str, job: DownloadJob, start_override: date | None
    ) -> pd.DataFrame:
        eodhd_sym = self._symbol(symbol)
        params: dict = {}
        start = start_override or job.start_date
        if start:
            params["from"] = str(start)
        if job.end_date:
            params["to"] = str(job.end_date)

        data = self._get(f"splits/{eodhd_sym}", params)

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")

        if "split" in df.columns:
            df = df.rename(columns={"split": "ratio"})

        return df[["ratio"]] if "ratio" in df.columns else df

    def _download_fundamentals(self, symbol: str) -> pd.DataFrame:
        eodhd_sym = self._symbol(symbol)
        data = self._get(f"fundamentals/{eodhd_sym}")

        if not data or not isinstance(data, dict):
            return pd.DataFrame()

        # Extract key financial data
        rows = []
        financials = data.get("Financials", {})
        for stmt_type in ["Income_Statement", "Balance_Sheet", "Cash_Flow"]:
            quarterly = financials.get(stmt_type, {}).get("quarterly", {})
            for period_key, values in quarterly.items():
                if isinstance(values, dict):
                    row = {"period": period_key, "statement_type": stmt_type}
                    row.update(values)
                    rows.append(row)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.dropna(subset=["date"])
            df = df.set_index("date")
            df = df.sort_index()
        elif "period" in df.columns:
            df = df.set_index("period")

        return df

    def get_last_available_date(self, symbol: str, data_type: DataType) -> date | None:
        try:
            eodhd_sym = self._symbol(symbol)
            data = self._get(f"eod/{eodhd_sym}", {"order": "d", "limit": "1"})
            if data and isinstance(data, list) and len(data) > 0:
                return pd.Timestamp(data[0]["date"]).date()
        except Exception as e:
            logger.debug(f"Could not get last date for {symbol}: {e}")
        return None
