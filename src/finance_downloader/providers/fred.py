"""FRED (Federal Reserve Economic Data) provider."""

from __future__ import annotations

from datetime import date

import pandas as pd
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from finance_downloader.core.base_provider import BaseProvider
from finance_downloader.core.models import DataType, DownloadJob, ProviderConfig
from finance_downloader.utils.rate_limiter import RateLimiter


class FredProvider(BaseProvider):
    """
    FRED data provider for economic/macro time series.

    Requires a free API key from https://fred.stlouisfed.org/docs/api/api_key.html
    Symbols are FRED series IDs (e.g. GDP, UNRATE, CPIAUCSL, DFF, T10Y2Y).
    """

    name = "fred"
    supported_data_types = [DataType.ECONOMIC]

    def __init__(self, config: ProviderConfig | None = None) -> None:
        if config is None:
            config = ProviderConfig(api_key_env="FRED_API_KEY")
        super().__init__(config)
        self._client = None
        self._limiter = RateLimiter(calls_per_minute=120)

    def _get_client(self):
        if self._client is None:
            from fredapi import Fred

            if not self.api_key:
                raise RuntimeError("FRED API key not set. Export FRED_API_KEY.")
            self._client = Fred(api_key=self.api_key)
        return self._client

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    def download(self, job: DownloadJob, start_override: date | None = None) -> pd.DataFrame:
        symbol = job.symbols[0]  # FRED series ID
        start = start_override or job.start_date
        end = job.end_date

        self._limiter.acquire()
        client = self._get_client()

        series = client.get_series(
            symbol,
            observation_start=str(start) if start else None,
            observation_end=str(end) if end else None,
        )

        if series is None or series.empty:
            return pd.DataFrame()

        df = series.to_frame(name="value")
        df.index.name = "date"
        df = df.dropna()
        return df

    def get_last_available_date(self, symbol: str, data_type: DataType) -> date | None:
        try:
            self._limiter.acquire()
            client = self._get_client()
            info = client.get_series_info(symbol)
            if hasattr(info, "observation_end"):
                return pd.Timestamp(info.observation_end).date()
        except Exception as e:
            logger.debug(f"Could not get last date for FRED series {symbol}: {e}")
        return None
