"""Command-line interface for finance-download."""

from __future__ import annotations

import argparse
import sys
from datetime import date

from finance_downloader import __version__
from finance_downloader.config import load_config
from finance_downloader.core.models import AppConfig, ProviderConfig, StorageFormat
from finance_downloader.core.registry import ProviderRegistry, registry
from finance_downloader.core.storage import DataStorage
from finance_downloader.runner import DownloadRunner
from finance_downloader.utils.logging import setup_logging


def _init_registry(app_config: AppConfig | None = None) -> ProviderRegistry:
    """Discover providers and optionally initialize with config."""
    registry.discover_providers()
    if app_config:
        registry.initialize_providers(app_config.providers)
    return registry


# ── Subcommands ────────────────────────────────────────────────────────


def cmd_run(args: argparse.Namespace) -> int:
    """Run jobs from a config file."""
    config = load_config(args.config)
    setup_logging(config.log_level, config.log_file)
    reg = _init_registry(config)
    runner = DownloadRunner(config, reg)

    results = runner.run_all_jobs()

    all_ok = all(r.all_succeeded for r in results)
    total_ok = sum(r.successful for r in results)
    total_fail = sum(r.failed for r in results)

    if all_ok:
        print(f"All jobs completed successfully ({total_ok} symbols downloaded)")
        return 0
    else:
        print(f"Completed with errors: {total_ok} succeeded, {total_fail} failed")
        return 1


def cmd_fetch(args: argparse.Namespace) -> int:
    """Ad-hoc fetch for a single provider/data-type."""
    setup_logging(args.log_level)

    config = AppConfig(
        output_dir=args.output_dir,
        storage_format=StorageFormat(args.format),
        providers={args.provider: ProviderConfig()},
    )
    reg = _init_registry(config)
    runner = DownloadRunner(config, reg)

    start = date.fromisoformat(args.start_date) if args.start_date else None
    end = date.fromisoformat(args.end_date) if args.end_date else None

    result = runner.run_adhoc(
        provider_name=args.provider,
        data_type=args.data_type,
        symbols=args.symbols,
        start_date=start,
        end_date=end,
        output_subdir=args.subdir,
    )

    if result.all_succeeded:
        print(f"Downloaded {result.successful} symbols successfully")
        return 0
    else:
        print(f"{result.successful} succeeded, {result.failed} failed")
        return 1


def cmd_providers(args: argparse.Namespace) -> int:
    """List available providers and their capabilities."""
    setup_logging("WARNING")
    _init_registry()

    providers = registry.list_providers()
    if not providers:
        print("No providers found.")
        return 0

    print(f"{'Provider':<20} {'API Key?':<10} {'Data Types'}")
    print("-" * 70)
    for p in providers:
        key_req = "Yes" if p["requires_api_key"] else "No"
        types = ", ".join(p["data_types"])
        print(f"{p['name']:<20} {key_req:<10} {types}")

    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """Show status of existing downloads."""
    setup_logging("WARNING")
    storage = DataStorage(args.output_dir)

    downloads = storage.list_downloads()
    if not downloads:
        print(f"No downloads found in {args.output_dir}")
        return 0

    print(f"{'Symbol':<15} {'Provider':<12} {'Type':<15} {'Rows':>8} {'Last Date':<12}")
    print("-" * 65)
    for d in downloads:
        print(
            f"{d['symbol']:<15} {d['provider']:<12} {d['data_type']:<15} "
            f"{d['row_count']:>8} {d['last_data_date'] or 'N/A':<12}"
        )

    return 0


# ── Main parser ────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="finance-download",
        description="Comprehensive financial data downloader",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command", help="Available commands")

    # run
    p_run = sub.add_parser("run", help="Run download jobs from a config file")
    p_run.add_argument("--config", "-c", required=True, help="Path to JSON config file")

    # fetch
    p_fetch = sub.add_parser("fetch", help="Ad-hoc download for a single provider")
    p_fetch.add_argument("--provider", "-p", required=True, help="Provider name")
    p_fetch.add_argument("--symbols", "-s", nargs="+", required=True, help="Symbols to download")
    p_fetch.add_argument("--data-type", "-t", required=True, help="Data type (e.g. eod_prices)")
    p_fetch.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    p_fetch.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    p_fetch.add_argument("--output-dir", "-o", default="./data", help="Output directory")
    p_fetch.add_argument("--subdir", default="", help="Output subdirectory")
    p_fetch.add_argument("--format", default="parquet", choices=["parquet", "csv"])
    p_fetch.add_argument("--log-level", default="INFO")

    # providers
    sub.add_parser("providers", help="List available providers and data types")

    # status
    p_status = sub.add_parser("status", help="Show download status")
    p_status.add_argument("--output-dir", "-o", default="./data", help="Output directory")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    dispatch = {
        "run": cmd_run,
        "fetch": cmd_fetch,
        "providers": cmd_providers,
        "status": cmd_status,
    }

    handler = dispatch.get(args.command)
    if handler is None:
        parser.print_help()
        sys.exit(0)

    exit_code = handler(args)
    sys.exit(exit_code)
