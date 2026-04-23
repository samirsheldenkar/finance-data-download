"""Tests for SEC EDGAR provider."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from finance_downloader.core.models import DataType, DownloadJob
from finance_downloader.providers.sec_edgar import SecEdgarProvider


@pytest.fixture
def sec():
    return SecEdgarProvider()


def test_provider_metadata(sec):
    assert sec.name == "sec_edgar"
    assert DataType.FILINGS in sec.supported_data_types
    assert not sec.supports(DataType.EOD_PRICES)
    assert not sec.supports(DataType.ECONOMIC)


def test_no_api_key_required(sec):
    assert not sec.requires_api_key()


def test_always_available(sec):
    assert sec.is_available()


@patch.object(SecEdgarProvider, "_load_ticker_map")
def test_cik_lookup(mock_load_map, sec):
    """Test that ticker is correctly mapped to CIK."""
    mock_load_map.return_value = {"AAPL": "0000320193", "MSFT": "0000789019"}

    cik = sec._get_cik("AAPL")
    assert cik == "0000320193"


@patch.object(SecEdgarProvider, "_load_ticker_map")
def test_cik_lookup_not_found(mock_load_map, sec):
    """Test that unknown ticker raises ValueError."""
    mock_load_map.return_value = {"AAPL": "0000320193"}

    with pytest.raises(ValueError, match="Could not find CIK"):
        sec._get_cik("UNKNOWN_TICKER")


@patch.object(SecEdgarProvider, "_get_cik")
@patch.object(SecEdgarProvider, "_load_ticker_map")
def test_download_financials(mock_load_map, mock_get_cik, sec):
    """Test financial data download with mocked SEC API."""
    mock_get_cik.return_value = "0000320193"
    mock_load_map.return_value = {"AAPL": "0000320193"}

    # Mock the SEC API response
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "units": {
                        "USD": [
                            {
                                "val": 394328000000,
                                "end": "2024-09-30",
                                "filed": "2024-11-01",
                                "form": "10-K",
                                "fy": 2024,
                                "fp": "FY",
                                "accn": "0000320193-24-000001",
                            },
                            {
                                "val": 383285000000,
                                "end": "2023-09-30",
                                "filed": "2023-11-03",
                                "form": "10-K",
                                "fy": 2023,
                                "fp": "FY",
                                "accn": "0000320193-23-000001",
                            },
                        ]
                    }
                }
            }
        }
    }
    mock_response.raise_for_status = MagicMock()

    with patch.object(sec._session, "get", return_value=mock_response):
        job = DownloadJob(
            name="test",
            provider="sec_edgar",
            data_type=DataType.FILINGS,
            symbols=["AAPL"],
            start_date=date(2023, 1, 1),
        )

        # Need to also mock the ticker map load
        sec._ticker_to_cik = {"AAPL": "0000320193"}
        df = sec.download(job)
        assert not df.empty
        assert "concept" in df.columns
        assert "value" in df.columns


def test_unsupported_data_type(sec):
    """Test that unsupported data type raises ValueError."""
    job = DownloadJob(
        name="test",
        provider="sec_edgar",
        data_type=DataType.EOD_PRICES,
        symbols=["AAPL"],
    )

    with pytest.raises(ValueError, match="Unsupported data type"):
        sec.download(job)
