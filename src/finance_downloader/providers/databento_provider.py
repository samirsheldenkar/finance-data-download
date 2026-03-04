"""Databento provider for futures and equities market data."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from finance_downloader.core.base_provider import BaseProvider
from finance_downloader.core.models import DataType, DownloadJob, ProviderConfig


class DatabentoProvider(BaseProvider):
    """
    Databento provider for high-quality market data.

    Paid service with $125 free credits. Supports futures, equities.
    Uses the `databento` Python client for `timeseries.get_range()`.

    Extra job config:
        dataset: str  — Databento dataset ID (e.g. "GLBX.MDP3" for CME Globex)
        schema: str   — Data schema (default: "ohlcv-1d")
        stype_in: str — Symbol type (default: "raw_symbol")
    """

    name = "databento"
    supported_data_types = [DataType.EOD_PRICES, DataType.INTRADAY]

    def __init__(self, config: ProviderConfig | None = None) -> None:
        if config is None:
            config = ProviderConfig(api_key_env="DATABENTO_API_KEY")
        super().__init__(config)
        self._client = None

    def _get_client(self):
        if self._client is None:
            import databento as db

            if self.api_key:
                self._client = db.Historical(self.api_key)
            else:
                self._client = db.Historical()  # uses DATABENTO_API_KEY env var
        return self._client

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, max=30))
    def download(self, job: DownloadJob, start_override: date | None = None) -> pd.DataFrame:
        symbol = job.symbols[0]
        start = start_override or job.start_date
        end = job.end_date or date.today()

        # Extract Databento-specific options from job.extra
        dataset = job.extra.get("dataset", "GLBX.MDP3")
        stype_in = job.extra.get("stype_in", "raw_symbol")

        if job.data_type == DataType.INTRADAY:
            schema = job.extra.get("schema", "ohlcv-1m")
        else:
            schema = job.extra.get("schema", "ohlcv-1d")

        client = self._get_client()

        logger.debug(
            f"Databento request: dataset={dataset}, symbol={symbol}, "
            f"schema={schema}, {start} to {end}"
        )

        data = client.timeseries.get_range(
            dataset=dataset,
            symbols=symbol,
            schema=schema,
            stype_in=stype_in,
            start=str(start) if start else None,
            end=str(end + timedelta(days=1)),  # Databento end is exclusive
        )

        df = data.to_df()

        if df.empty:
            return df

        # Normalize columns for OHLCV schemas
        rename_map = {}
        for col in df.columns:
            lower = col.lower()
            if lower in ("open", "high", "low", "close", "volume"):
                rename_map[col] = lower

        if rename_map:
            df = df.rename(columns=rename_map)

        df.index.name = "date"

        # Keep standard OHLCV columns plus any extras
        standard = ["open", "high", "low", "close", "volume"]
        available = [c for c in standard if c in df.columns]
        extra_cols = [c for c in df.columns if c not in standard]
        return df[available + extra_cols]

    def get_last_available_date(self, symbol: str, data_type: DataType) -> date | None:
        # Databento doesn't have a simple "last date" query;
        # return None and rely on metadata sidecar.
        return None
