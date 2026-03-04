"""Data models for finance downloader."""

from __future__ import annotations

import hashlib
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class DataType(str, Enum):
    """Supported data types across providers."""

    EOD_PRICES = "eod_prices"
    INTRADAY = "intraday"
    DIVIDENDS = "dividends"
    SPLITS = "splits"
    FUNDAMENTALS = "fundamentals"
    ECONOMIC = "economic"
    FILINGS = "filings"


class StorageFormat(str, Enum):
    """Supported output storage formats."""

    PARQUET = "parquet"
    CSV = "csv"


class ProviderConfig(BaseModel):
    """Configuration for a single provider."""

    api_key_env: str | None = None
    rate_limit_per_minute: int | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class DownloadJob(BaseModel):
    """A single download job specification."""

    name: str
    provider: str
    data_type: DataType
    symbols: list[str]
    start_date: date | None = None
    end_date: date | None = None
    output_subdir: str = ""
    extra: dict[str, Any] = Field(default_factory=dict)


class AppConfig(BaseModel):
    """Top-level application configuration."""

    output_dir: str = "./data"
    storage_format: StorageFormat = StorageFormat.PARQUET
    log_level: str = "INFO"
    log_file: str | None = None
    providers: dict[str, ProviderConfig] = Field(default_factory=dict)
    jobs: list[DownloadJob] = Field(default_factory=list)


class DownloadMetadata(BaseModel):
    """Metadata sidecar for a downloaded data file."""

    provider: str
    symbol: str
    data_type: str
    last_downloaded: datetime
    last_data_date: date | None = None
    row_count: int = 0
    file_hash: str = ""

    def compute_file_hash(self, filepath: Path) -> str:
        """Compute SHA-256 hash of a file."""
        sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        self.file_hash = f"sha256:{sha256.hexdigest()}"
        return self.file_hash


class DownloadResult(BaseModel):
    """Result of a single symbol download."""

    symbol: str
    success: bool
    rows_downloaded: int = 0
    rows_total: int = 0
    error: str | None = None
    data_date_range: tuple[date, date] | None = None

    model_config = {"arbitrary_types_allowed": True}


class JobResult(BaseModel):
    """Aggregate result of a download job."""

    job_name: str
    provider: str
    data_type: str
    total_symbols: int
    successful: int = 0
    failed: int = 0
    skipped: int = 0
    results: list[DownloadResult] = Field(default_factory=list)
    elapsed_seconds: float = 0.0

    @property
    def all_succeeded(self) -> bool:
        return self.failed == 0
