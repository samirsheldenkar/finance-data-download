"""Storage module for writing/reading data files with metadata tracking."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from loguru import logger

from finance_downloader.core.models import DownloadMetadata, StorageFormat


class DataStorage:
    """
    Handles reading and writing data files with companion metadata sidecars.

    Directory layout:
        {base_dir}/{subdir}/{symbol}_{data_type}.{parquet|csv}
        {base_dir}/{subdir}/{symbol}_{data_type}.meta.json
    """

    def __init__(self, base_dir: str | Path, storage_format: StorageFormat = StorageFormat.PARQUET):
        self.base_dir = Path(base_dir)
        self.storage_format = storage_format
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _get_dir(self, subdir: str) -> Path:
        path = self.base_dir / subdir if subdir else self.base_dir
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _data_filename(self, symbol: str, data_type: str) -> str:
        safe_symbol = symbol.replace("/", "_").replace("\\", "_").replace(".", "_")
        ext = "parquet" if self.storage_format == StorageFormat.PARQUET else "csv"
        return f"{safe_symbol}_{data_type}.{ext}"

    def _meta_filename(self, symbol: str, data_type: str) -> str:
        safe_symbol = symbol.replace("/", "_").replace("\\", "_").replace(".", "_")
        return f"{safe_symbol}_{data_type}.meta.json"

    def get_data_path(self, symbol: str, data_type: str, subdir: str = "") -> Path:
        return self._get_dir(subdir) / self._data_filename(symbol, data_type)

    def get_meta_path(self, symbol: str, data_type: str, subdir: str = "") -> Path:
        return self._get_dir(subdir) / self._meta_filename(symbol, data_type)

    # ── Write ──────────────────────────────────────────────────────────

    def write(
        self,
        df: pd.DataFrame,
        symbol: str,
        data_type: str,
        provider: str,
        subdir: str = "",
    ) -> Path:
        """Write a DataFrame and its metadata sidecar. Returns the data file path."""
        if df.empty:
            logger.warning(f"Empty DataFrame for {symbol}/{data_type}, skipping write")
            return self.get_data_path(symbol, data_type, subdir)

        data_path = self.get_data_path(symbol, data_type, subdir)

        if self.storage_format == StorageFormat.PARQUET:
            df.to_parquet(data_path, compression="snappy", index=True, engine="pyarrow")
        else:
            df.to_csv(data_path, index=True)

        # Determine the last data date
        last_data_date = None
        if isinstance(df.index, pd.DatetimeIndex) and len(df) > 0:
            last_data_date = df.index.max().date()
        elif "date" in df.columns and len(df) > 0:
            last_data_date = pd.to_datetime(df["date"]).max().date()

        meta = DownloadMetadata(
            provider=provider,
            symbol=symbol,
            data_type=data_type,
            last_downloaded=datetime.now(),
            last_data_date=last_data_date,
            row_count=len(df),
        )
        meta.compute_file_hash(data_path)
        self._write_meta(meta, symbol, data_type, subdir)

        logger.debug(f"Wrote {len(df)} rows to {data_path}")
        return data_path

    def append(
        self,
        new_df: pd.DataFrame,
        symbol: str,
        data_type: str,
        provider: str,
        subdir: str = "",
    ) -> Path:
        """Append new data to an existing file, deduplicating by index."""
        existing = self.read(symbol, data_type, subdir)

        if existing.empty:
            return self.write(new_df, symbol, data_type, provider, subdir)

        combined = pd.concat([existing, new_df])
        if isinstance(combined.index, pd.DatetimeIndex):
            combined = combined[~combined.index.duplicated(keep="last")]
            combined = combined.sort_index()
        else:
            combined = combined.drop_duplicates(keep="last")

        return self.write(combined, symbol, data_type, provider, subdir)

    # ── Read ───────────────────────────────────────────────────────────

    def read(self, symbol: str, data_type: str, subdir: str = "") -> pd.DataFrame:
        """Read a data file. Returns empty DataFrame if not found."""
        data_path = self.get_data_path(symbol, data_type, subdir)

        if not data_path.exists():
            return pd.DataFrame()

        try:
            if self.storage_format == StorageFormat.PARQUET:
                return pd.read_parquet(data_path, engine="pyarrow")
            else:
                return pd.read_csv(data_path, index_col=0, parse_dates=True)
        except Exception as e:
            logger.error(f"Error reading {data_path}: {e}")
            return pd.DataFrame()

    def read_meta(self, symbol: str, data_type: str, subdir: str = "") -> DownloadMetadata | None:
        """Read metadata sidecar for a data file."""
        meta_path = self.get_meta_path(symbol, data_type, subdir)
        if not meta_path.exists():
            return None
        try:
            with open(meta_path) as f:
                data = json.load(f)
            return DownloadMetadata(**data)
        except Exception as e:
            logger.error(f"Error reading metadata {meta_path}: {e}")
            return None

    def _write_meta(
        self, meta: DownloadMetadata, symbol: str, data_type: str, subdir: str
    ) -> None:
        meta_path = self.get_meta_path(symbol, data_type, subdir)
        with open(meta_path, "w") as f:
            json.dump(meta.model_dump(mode="json"), f, indent=2, default=str)

    # ── Query ──────────────────────────────────────────────────────────

    def exists(self, symbol: str, data_type: str, subdir: str = "") -> bool:
        return self.get_data_path(symbol, data_type, subdir).exists()

    def get_last_data_date(
        self, symbol: str, data_type: str, subdir: str = ""
    ) -> date | None:
        """Get the last data date from metadata, without reading the full file."""
        meta = self.read_meta(symbol, data_type, subdir)
        if meta and meta.last_data_date:
            return meta.last_data_date
        return None

    def list_downloads(self, subdir: str = "") -> list[dict]:
        """List all downloaded files and their metadata in a subdirectory."""
        target_dir = self._get_dir(subdir) if subdir else self.base_dir
        results = []

        for meta_file in sorted(target_dir.rglob("*.meta.json")):
            try:
                with open(meta_file) as f:
                    data = json.load(f)
                meta = DownloadMetadata(**data)
                results.append(
                    {
                        "symbol": meta.symbol,
                        "provider": meta.provider,
                        "data_type": meta.data_type,
                        "last_downloaded": str(meta.last_downloaded),
                        "last_data_date": str(meta.last_data_date) if meta.last_data_date else None,
                        "row_count": meta.row_count,
                        "path": str(meta_file.parent / meta_file.name.replace(".meta.json", "")),
                    }
                )
            except Exception as e:
                logger.warning(f"Error reading {meta_file}: {e}")

        return results
