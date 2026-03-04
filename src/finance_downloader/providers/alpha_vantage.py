"""Alpha Vantage provider for stocks, forex, crypto, and economic data."""

from __future__ import annotations

from datetime import date

import pandas as pd
import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from finance_downloader.core.base_provider import BaseProvider
from finance_downloader.core.models import DataType, DownloadJob, ProviderConfig
from finance_downloader.utils.rate_limiter import RateLimiter

BASE_URL = "https://www.alphavantage.co/query"


class AlphaVantageProvider(BaseProvider):
    """
    Alpha Vantage data provider.

    Free tier: 25 API calls/day. Supports stocks, forex, crypto, economic indicators.
    Get a free key at https://www.alphavantage.co/support/#api-key
    """

    name = "alpha_vantage"
    supported_data_types = [DataType.EOD_PRICES, DataType.ECONOMIC]

    def __init__(self, config: ProviderConfig | None = None) -> None:
        if config is None:
            config = ProviderConfig(api_key_env="ALPHAVANTAGE_API_KEY")
        super().__init__(config)
        # Free tier: 25 calls/day ≈ conservative 5/min
        self._limiter = RateLimiter(
            calls_per_minute=config.rate_limit_per_minute or 5
        )

    def _get(self, params: dict) -> dict:
        self._limiter.acquire()
        params["apikey"] = self.api_key
        resp = requests.get(BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        # Alpha Vantage returns error messages as JSON
        if "Error Message" in data:
            raise ValueError(f"Alpha Vantage error: {data['Error Message']}")
        if "Note" in data:
            logger.warning(f"Alpha Vantage rate limit note: {data['Note']}")

        return data

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, max=30))
    def download(self, job: DownloadJob, start_override: date | None = None) -> pd.DataFrame:
        symbol = job.symbols[0]

        if job.data_type == DataType.EOD_PRICES:
            return self._download_eod(symbol, job, start_override)
        elif job.data_type == DataType.ECONOMIC:
            return self._download_economic(symbol, job, start_override)
        else:
            raise ValueError(f"Unsupported data type for Alpha Vantage: {job.data_type}")

    def _download_eod(
        self, symbol: str, job: DownloadJob, start_override: date | None
    ) -> pd.DataFrame:
        """Download daily stock prices (full history or compact)."""
        start = start_override or job.start_date

        # Use compact (100 days) for incremental, full for initial
        outputsize = "compact" if start_override else "full"

        data = self._get({
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": symbol,
            "outputsize": outputsize,
        })

        time_series = data.get("Time Series (Daily)", {})
        if not time_series:
            return pd.DataFrame()

        df = pd.DataFrame.from_dict(time_series, orient="index")
        df.index = pd.to_datetime(df.index)
        df.index.name = "date"
        df = df.sort_index()

        # Normalize column names (Alpha Vantage uses "1. open", etc.)
        rename = {}
        for col in df.columns:
            lower = col.lower()
            if "open" in lower:
                rename[col] = "open"
            elif "high" in lower:
                rename[col] = "high"
            elif "low" in lower:
                rename[col] = "low"
            elif "close" in lower and "adjusted" not in lower:
                rename[col] = "close"
            elif "adjusted" in lower:
                rename[col] = "adj_close"
            elif "volume" in lower:
                rename[col] = "volume"
        df = df.rename(columns=rename)

        # Convert to numeric
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        cols = ["open", "high", "low", "close", "volume", "adj_close"]
        available = [c for c in cols if c in df.columns]
        df = df[available]

        # Filter by start date
        if start:
            df = df[df.index >= pd.Timestamp(start)]

        return df

    def _download_economic(
        self, symbol: str, job: DownloadJob, start_override: date | None
    ) -> pd.DataFrame:
        """
        Download economic indicators.

        Supported symbols (Alpha Vantage function names):
        REAL_GDP, REAL_GDP_PER_CAPITA, TREASURY_YIELD, FEDERAL_FUNDS_RATE,
        CPI, INFLATION, RETAIL_SALES, DURABLES, UNEMPLOYMENT, NONFARM_PAYROLL
        """
        # Map common names to AV function names
        func_map = {
            "REAL_GDP": "REAL_GDP",
            "GDP": "REAL_GDP",
            "CPI": "CPI",
            "INFLATION": "INFLATION",
            "UNEMPLOYMENT": "UNEMPLOYMENT",
            "TREASURY_YIELD": "TREASURY_YIELD",
            "FEDERAL_FUNDS_RATE": "FEDERAL_FUNDS_RATE",
            "RETAIL_SALES": "RETAIL_SALES",
            "NONFARM_PAYROLL": "NONFARM_PAYROLL",
        }

        function = func_map.get(symbol.upper(), symbol.upper())
        params: dict = {"function": function}

        # Some endpoints accept interval
        if function in ("REAL_GDP", "TREASURY_YIELD", "CPI"):
            params["interval"] = job.extra.get("interval", "monthly")

        # Treasury yield accepts maturity
        if function == "TREASURY_YIELD":
            params["maturity"] = job.extra.get("maturity", "10year")

        data = self._get(params)

        # Response key varies by function
        data_key = None
        for key in data:
            if key != "name" and key != "interval" and key != "unit" and isinstance(data[key], list):
                data_key = key
                break

        if not data_key:
            return pd.DataFrame()

        records = data[data_key]
        df = pd.DataFrame(records)

        if df.empty:
            return df

        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
        df = df.sort_index()
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df[["value"]].dropna()

        start = start_override or job.start_date
        if start:
            df = df[df.index >= pd.Timestamp(start)]

        return df

    def get_last_available_date(self, symbol: str, data_type: DataType) -> date | None:
        return None  # Rely on metadata sidecar
