"""Job runner that orchestrates downloads with incremental update support."""

from __future__ import annotations

import time
from datetime import date, timedelta

from loguru import logger

from finance_downloader.core.models import (
    AppConfig,
    DataType,
    DownloadJob,
    DownloadResult,
    JobResult,
)
from finance_downloader.core.registry import ProviderRegistry
from finance_downloader.core.storage import DataStorage


class DownloadRunner:
    """
    Orchestrates download jobs, handling incremental updates and error isolation.

    For each job/symbol:
      1. Check existing metadata for the last downloaded date.
      2. If data exists, set start_date = last_data_date + 1 day (incremental).
      3. Download new data from the provider.
      4. Append to existing data (or write fresh).
      5. Update metadata sidecar.
    """

    def __init__(self, config: AppConfig, registry: ProviderRegistry) -> None:
        self.config = config
        self.registry = registry
        self.storage = DataStorage(config.output_dir, config.storage_format)

    def run_all_jobs(self) -> list[JobResult]:
        """Execute all jobs defined in the config. Returns list of results."""
        results = []
        for job in self.config.jobs:
            result = self.run_job(job)
            results.append(result)
            status = "OK" if result.all_succeeded else "PARTIAL FAILURE"
            logger.info(
                f"Job '{job.name}': {status} — "
                f"{result.successful}/{result.total_symbols} symbols "
                f"in {result.elapsed_seconds:.1f}s"
            )
        return results

    def run_job(self, job: DownloadJob) -> JobResult:
        """Execute a single download job across all its symbols."""
        start_time = time.time()

        result = JobResult(
            job_name=job.name,
            provider=job.provider,
            data_type=job.data_type.value,
            total_symbols=len(job.symbols),
        )

        # Get provider
        try:
            provider = self.registry.get_provider(job.provider)
        except ValueError as e:
            logger.error(f"Job '{job.name}': {e}")
            result.failed = len(job.symbols)
            result.results = [
                DownloadResult(symbol=s, success=False, error=str(e))
                for s in job.symbols
            ]
            result.elapsed_seconds = time.time() - start_time
            return result

        # Check provider availability
        if not provider.is_available():
            msg = f"Provider '{job.provider}' is not available (missing API key?)"
            logger.error(f"Job '{job.name}': {msg}")
            result.failed = len(job.symbols)
            result.results = [
                DownloadResult(symbol=s, success=False, error=msg)
                for s in job.symbols
            ]
            result.elapsed_seconds = time.time() - start_time
            return result

        # Check data type support
        if not provider.supports(job.data_type):
            msg = (
                f"Provider '{job.provider}' does not support data type '{job.data_type.value}'"
            )
            logger.error(f"Job '{job.name}': {msg}")
            result.failed = len(job.symbols)
            result.results = [
                DownloadResult(symbol=s, success=False, error=msg)
                for s in job.symbols
            ]
            result.elapsed_seconds = time.time() - start_time
            return result

        # Process each symbol
        for symbol in job.symbols:
            dl_result = self._download_symbol(job, symbol, provider)
            result.results.append(dl_result)
            if dl_result.success:
                result.successful += 1
            else:
                result.failed += 1

        result.elapsed_seconds = time.time() - start_time
        return result

    def _download_symbol(self, job: DownloadJob, symbol: str, provider) -> DownloadResult:
        """Download data for a single symbol, with incremental support."""
        try:
            # Check for existing data (incremental update)
            start_override: date | None = None
            last_date = self.storage.get_last_data_date(
                symbol, job.data_type.value, job.output_subdir
            )

            if last_date:
                incremental_start = last_date + timedelta(days=1)
                today = date.today()
                if incremental_start > today:
                    logger.info(f"  {symbol}: already up-to-date (last: {last_date})")
                    return DownloadResult(
                        symbol=symbol,
                        success=True,
                        rows_downloaded=0,
                        rows_total=0,
                    )
                start_override = incremental_start
                logger.info(f"  {symbol}: incremental update from {incremental_start}")
            else:
                logger.info(f"  {symbol}: full download")

            # Create a per-symbol job (single symbol)
            symbol_job = job.model_copy(update={"symbols": [symbol]})

            # Download
            df = provider.download(symbol_job, start_override=start_override)

            if df.empty:
                logger.info(f"  {symbol}: no new data returned")
                return DownloadResult(symbol=symbol, success=True, rows_downloaded=0)

            # Store data
            rows_new = len(df)
            if last_date:
                self.storage.append(
                    df, symbol, job.data_type.value, job.provider, job.output_subdir
                )
            else:
                self.storage.write(
                    df, symbol, job.data_type.value, job.provider, job.output_subdir
                )

            # Read back total row count
            meta = self.storage.read_meta(symbol, job.data_type.value, job.output_subdir)
            total_rows = meta.row_count if meta else rows_new

            logger.info(f"  {symbol}: downloaded {rows_new} new rows (total: {total_rows})")
            return DownloadResult(
                symbol=symbol,
                success=True,
                rows_downloaded=rows_new,
                rows_total=total_rows,
            )

        except Exception as e:
            logger.error(f"  {symbol}: download failed — {e}")
            return DownloadResult(symbol=symbol, success=False, error=str(e))

    def run_adhoc(
        self,
        provider_name: str,
        data_type: str,
        symbols: list[str],
        start_date: date | None = None,
        end_date: date | None = None,
        output_subdir: str = "",
    ) -> JobResult:
        """Run an ad-hoc download (not from config file)."""
        job = DownloadJob(
            name="adhoc",
            provider=provider_name,
            data_type=DataType(data_type),
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            output_subdir=output_subdir,
        )
        return self.run_job(job)
