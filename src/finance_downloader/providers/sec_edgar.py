"""SEC EDGAR provider for free company financial filings."""

from __future__ import annotations

import os
from datetime import date

import pandas as pd
import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from finance_downloader.core.base_provider import BaseProvider
from finance_downloader.core.models import DataType, DownloadJob, ProviderConfig
from finance_downloader.utils.rate_limiter import RateLimiter

EDGAR_BASE = "https://data.sec.gov"
COMPANY_TICKERS_URL = f"{EDGAR_BASE}/files/company_tickers.json"
COMPANY_FACTS_URL = f"{EDGAR_BASE}/api/xbrl/companyfacts/CIK{{cik}}.json"


class SecEdgarProvider(BaseProvider):
    """
    SEC EDGAR provider for free company financial data.

    No API key needed, but SEC requires a User-Agent header identifying the requester.
    Set SEC_EDGAR_USER_AGENT env var (e.g. "YourName your@email.com").

    Provides quarterly/annual financial statements via the XBRL API.
    Symbols are stock tickers (e.g. AAPL, MSFT). Internally mapped to CIK numbers.
    """

    name = "sec_edgar"
    supported_data_types = [DataType.FILINGS]

    def __init__(self, config: ProviderConfig | None = None) -> None:
        super().__init__(config or ProviderConfig())
        user_agent = os.environ.get("SEC_EDGAR_USER_AGENT", "FinanceDownloader bot@example.com")
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": user_agent})
        # SEC rate limit: 10 requests/second
        self._limiter = RateLimiter(calls_per_minute=500)
        self._ticker_to_cik: dict[str, str] | None = None

    def requires_api_key(self) -> bool:
        return False

    def is_available(self) -> bool:
        return True

    def _load_ticker_map(self) -> dict[str, str]:
        """Load the ticker → CIK mapping from SEC."""
        if self._ticker_to_cik is not None:
            return self._ticker_to_cik

        self._limiter.acquire()
        resp = self._session.get(COMPANY_TICKERS_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        mapping = {}
        for entry in data.values():
            ticker = entry.get("ticker", "").upper()
            cik = str(entry.get("cik_str", "")).zfill(10)
            if ticker:
                mapping[ticker] = cik

        self._ticker_to_cik = mapping
        logger.debug(f"Loaded {len(mapping)} ticker→CIK mappings from SEC")
        return mapping

    def _get_cik(self, symbol: str) -> str:
        mapping = self._load_ticker_map()
        cik = mapping.get(symbol.upper())
        if not cik:
            raise ValueError(f"Could not find CIK for ticker '{symbol}' in SEC database")
        return cik

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    def download(self, job: DownloadJob, start_override: date | None = None) -> pd.DataFrame:
        symbol = job.symbols[0]

        if job.data_type == DataType.FILINGS:
            return self._download_financials(symbol, job, start_override)
        else:
            raise ValueError(f"Unsupported data type for SEC EDGAR: {job.data_type}")

    def _download_financials(
        self, symbol: str, job: DownloadJob, start_override: date | None
    ) -> pd.DataFrame:
        """Download company facts (financial data) from XBRL API."""
        cik = self._get_cik(symbol)

        self._limiter.acquire()
        url = COMPANY_FACTS_URL.format(cik=cik)
        resp = self._session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        facts = data.get("facts", {})

        # Extract US-GAAP facts (most common for US companies)
        us_gaap = facts.get("us-gaap", {})
        if not us_gaap:
            logger.warning(f"No US-GAAP data found for {symbol} (CIK: {cik})")
            return pd.DataFrame()

        # Key financial concepts to extract
        key_concepts = [
            "Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax",
            "NetIncomeLoss", "GrossProfit", "OperatingIncomeLoss",
            "Assets", "Liabilities", "StockholdersEquity",
            "CashAndCashEquivalentsAtCarryingValue",
            "LongTermDebt", "EarningsPerShareBasic", "EarningsPerShareDiluted",
            "CommonStockSharesOutstanding",
            "NetCashProvidedByUsedInOperatingActivities",
            "NetCashProvidedByUsedInFinancingActivities",
            "NetCashProvidedByUsedInInvestingActivities",
        ]

        # If user specified concepts in extras, use those instead
        if "concepts" in job.extra:
            key_concepts = job.extra["concepts"]

        rows = []
        for concept in key_concepts:
            if concept not in us_gaap:
                continue

            units = us_gaap[concept].get("units", {})
            # Usually "USD" for monetary values, "shares" for share counts
            for unit_type, entries in units.items():
                for entry in entries:
                    rows.append({
                        "concept": concept,
                        "value": entry.get("val"),
                        "unit": unit_type,
                        "end_date": entry.get("end"),
                        "filed_date": entry.get("filed"),
                        "form": entry.get("form", ""),
                        "fiscal_year": entry.get("fy"),
                        "fiscal_period": entry.get("fp"),
                        "accession": entry.get("accn", ""),
                    })

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        # Filter to 10-K and 10-Q filings only
        df = df[df["form"].isin(["10-K", "10-Q"])]

        if "end_date" in df.columns and not df.empty:
            df["date"] = pd.to_datetime(df["end_date"], errors="coerce")
            df = df.dropna(subset=["date"])
            df = df.set_index("date")
            df = df.sort_index()

        # Filter by date
        start = start_override or job.start_date
        if start and not df.empty:
            df = df[df.index >= pd.Timestamp(start)]

        return df

    def get_last_available_date(self, symbol: str, data_type: DataType) -> date | None:
        return None  # SEC data is not real-time; rely on metadata
