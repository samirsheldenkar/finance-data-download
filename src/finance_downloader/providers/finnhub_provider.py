"""Finnhub provider for stock prices and company fundamentals."""

from __future__ import annotations

import time as _time
from datetime import date, datetime

import pandas as pd
import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from finance_downloader.core.base_provider import BaseProvider
from finance_downloader.core.models import DataType, DownloadJob, ProviderConfig
from finance_downloader.utils.rate_limiter import RateLimiter

BASE_URL = "https://finnhub.io/api/v1"


class FinnhubProvider(BaseProvider):
    """
    Finnhub data provider.

    Free tier: 60 API calls/minute.
    Supports stock candles, company financials, and economic data.
    Get a free key at https://finnhub.io/
    """

    name = "finnhub"
    supported_data_types = [DataType.EOD_PRICES, DataType.FUNDAMENTALS]

    def __init__(self, config: ProviderConfig | None = None) -> None:
        if config is None:
            config = ProviderConfig(api_key_env="FINNHUB_API_KEY")
        super().__init__(config)
        self._limiter = RateLimiter(
            calls_per_minute=config.rate_limit_per_minute or 60
        )

    def _get(self, endpoint: str, params: dict | None = None) -> dict:
        self._limiter.acquire()
        params = params or {}
        params["token"] = self.api_key

        url = f"{BASE_URL}/{endpoint}"
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    def download(self, job: DownloadJob, start_override: date | None = None) -> pd.DataFrame:
        symbol = job.symbols[0]

        if job.data_type == DataType.EOD_PRICES:
            return self._download_candles(symbol, job, start_override)
        elif job.data_type == DataType.FUNDAMENTALS:
            return self._download_financials(symbol)
        else:
            raise ValueError(f"Unsupported data type for Finnhub: {job.data_type}")

    def _download_candles(
        self, symbol: str, job: DownloadJob, start_override: date | None
    ) -> pd.DataFrame:
        """Download daily candle data via /stock/candle endpoint."""
        start = start_override or job.start_date or date(2020, 1, 1)
        end = job.end_date or date.today()

        # Convert to UNIX timestamps
        start_ts = int(datetime.combine(start, datetime.min.time()).timestamp())
        end_ts = int(datetime.combine(end, datetime.min.time()).timestamp())

        data = self._get("stock/candle", {
            "symbol": symbol,
            "resolution": "D",
            "from": start_ts,
            "to": end_ts,
        })

        if data.get("s") == "no_data" or "t" not in data:
            return pd.DataFrame()

        df = pd.DataFrame({
            "date": pd.to_datetime(data["t"], unit="s"),
            "open": data["o"],
            "high": data["h"],
            "low": data["l"],
            "close": data["c"],
            "volume": data["v"],
        })

        df = df.set_index("date")
        return df

    def _download_financials(self, symbol: str) -> pd.DataFrame:
        """Download company financial statements via /stock/financials-reported."""
        data = self._get("stock/financials-reported", {
            "symbol": symbol,
            "freq": "quarterly",
        })

        reports = data.get("data", [])
        if not reports:
            return pd.DataFrame()

        rows = []
        for report in reports:
            row = {
                "date": report.get("filedDate") or report.get("acceptedDate", ""),
                "period": report.get("period", ""),
                "year": report.get("year", ""),
                "quarter": report.get("quarter", ""),
                "form": report.get("form", ""),
            }
            # Flatten report fields
            for section in report.get("report", {}).values():
                if isinstance(section, list):
                    for item in section:
                        if isinstance(item, dict) and "concept" in item:
                            row[item["concept"]] = item.get("value")
            rows.append(row)

        df = pd.DataFrame(rows)
        if "date" in df.columns and not df.empty:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.dropna(subset=["date"])
            df = df.set_index("date")
            df = df.sort_index()

        return df

    def get_last_available_date(self, symbol: str, data_type: DataType) -> date | None:
        try:
            now_ts = int(_time.time())
            week_ago = now_ts - 7 * 86400
            data = self._get("stock/candle", {
                "symbol": symbol,
                "resolution": "D",
                "from": week_ago,
                "to": now_ts,
            })
            if data.get("s") != "no_data" and "t" in data:
                last_ts = max(data["t"])
                return datetime.fromtimestamp(last_ts).date()
        except Exception as e:
            logger.debug(f"Could not get last date for {symbol}: {e}")
        return None
