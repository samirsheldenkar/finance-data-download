"""Tests for DataStorage module."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from finance_downloader.core.storage import DataStorage


def test_write_and_read_parquet(storage, sample_eod_df):
    """Test writing and reading back a Parquet file."""
    storage.write(sample_eod_df, "AAPL", "eod_prices", "yahoo")

    result = storage.read("AAPL", "eod_prices")
    assert not result.empty
    assert len(result) == 5
    assert list(result.columns) == ["open", "high", "low", "close", "volume"]


def test_write_and_read_csv(csv_storage, sample_eod_df):
    """Test writing and reading back a CSV file."""
    csv_storage.write(sample_eod_df, "AAPL", "eod_prices", "yahoo")

    result = csv_storage.read("AAPL", "eod_prices")
    assert not result.empty
    assert len(result) == 5


def test_metadata_sidecar(storage, sample_eod_df):
    """Test that metadata sidecar is created correctly."""
    storage.write(sample_eod_df, "AAPL", "eod_prices", "yahoo")

    meta = storage.read_meta("AAPL", "eod_prices")
    assert meta is not None
    assert meta.provider == "yahoo"
    assert meta.symbol == "AAPL"
    assert meta.data_type == "eod_prices"
    assert meta.row_count == 5
    assert meta.last_data_date == date(2024, 1, 8)  # Last business day in range
    assert meta.file_hash.startswith("sha256:")


def test_get_last_data_date(storage, sample_eod_df):
    """Test retrieving last data date from metadata."""
    storage.write(sample_eod_df, "AAPL", "eod_prices", "yahoo")

    last_date = storage.get_last_data_date("AAPL", "eod_prices")
    assert last_date == date(2024, 1, 8)


def test_get_last_data_date_missing(storage):
    """Test last data date returns None for missing data."""
    assert storage.get_last_data_date("AAPL", "eod_prices") is None


def test_exists(storage, sample_eod_df):
    """Test existence check."""
    assert not storage.exists("AAPL", "eod_prices")
    storage.write(sample_eod_df, "AAPL", "eod_prices", "yahoo")
    assert storage.exists("AAPL", "eod_prices")


def test_append_deduplicates(storage, sample_eod_df):
    """Test that appending deduplicates by date index."""
    storage.write(sample_eod_df, "AAPL", "eod_prices", "yahoo")

    # Create overlapping new data
    new_dates = pd.date_range("2024-01-08", periods=3, freq="B")
    new_df = pd.DataFrame(
        {
            "open": [155.0, 156.0, 157.0],
            "high": [160.0, 161.0, 162.0],
            "low": [154.0, 155.0, 156.0],
            "close": [159.0, 160.0, 161.0],
            "volume": [1500000, 1600000, 1700000],
        },
        index=pd.DatetimeIndex(new_dates, name="date"),
    )

    storage.append(new_df, "AAPL", "eod_prices", "yahoo")
    result = storage.read("AAPL", "eod_prices")

    # Should have 7 unique dates (5 original + 2 new, 1 overlap)
    assert len(result) == 7
    # The overlapping date should have the new data
    assert result.loc["2024-01-08", "close"] == 159.0


def test_write_empty_df(storage):
    """Test that writing an empty DataFrame is a no-op."""
    empty_df = pd.DataFrame()
    storage.write(empty_df, "AAPL", "eod_prices", "yahoo")

    assert not storage.exists("AAPL", "eod_prices")


def test_list_downloads(storage, sample_eod_df, sample_economic_df):
    """Test listing all downloads."""
    storage.write(sample_eod_df, "AAPL", "eod_prices", "yahoo")
    storage.write(sample_economic_df, "GDP", "economic", "fred", subdir="macro")

    downloads = storage.list_downloads()
    assert len(downloads) == 2

    symbols = {d["symbol"] for d in downloads}
    assert symbols == {"AAPL", "GDP"}


def test_subdirectory_storage(storage, sample_eod_df):
    """Test writing to a subdirectory."""
    storage.write(sample_eod_df, "AAPL", "eod_prices", "yahoo", subdir="equities/eod")

    result = storage.read("AAPL", "eod_prices", subdir="equities/eod")
    assert not result.empty
    assert len(result) == 5

    # Should not be found in root
    root_result = storage.read("AAPL", "eod_prices")
    assert root_result.empty
