# Finance Data Download

A modular, configuration-driven Python tool for downloading financial market data from multiple providers. Designed for easy scheduling (cron) with incremental updates that avoid re-downloading existing data.

## Features

- **7 data providers** with a plugin architecture for easy extension
- **JSON configuration** for defining download jobs
- **Incremental downloads** — metadata sidecars track last-downloaded dates
- **CLI with 4 subcommands** — `run`, `fetch`, `providers`, `status`
- **Parquet and CSV** output formats
- **Rate limiting** per provider to respect API quotas
- **Retry with exponential backoff** for transient errors
- **Structured logging** via loguru with optional file rotation

## Supported Providers

| Provider | API Key | Data Types |
|---|---|---|
| **Yahoo Finance** | No | EOD prices, dividends, splits, fundamentals |
| **FRED** | Free | Economic indicators (800k+ series) |
| **EODHD** | Free tier | EOD prices, dividends, splits, fundamentals |
| **Databento** | Paid ($125 free) | EOD/intraday OHLCV for futures & equities |
| **Alpha Vantage** | Free tier | EOD prices, economic indicators |
| **Finnhub** | Free tier | EOD prices, fundamentals |
| **SEC EDGAR** | No | Company financial filings (10-K, 10-Q) |

## Installation

```bash
pip install -e .

# With dev dependencies
pip install -e ".[dev]"
```

## Quick Start

### 1. Set up API keys

```bash
cp .env.example .env
# Edit .env with your API keys
source .env  # or use direnv / dotenv
```

### 2. Run from a config file

```bash
finance-download run --config configs/example_config.json
```

### 3. Ad-hoc download

```bash
# Download Apple and Microsoft daily prices via Yahoo Finance
finance-download fetch --provider yahoo --symbols AAPL MSFT --data-type eod_prices --start-date 2020-01-01

# Download GDP data from FRED
finance-download fetch --provider fred --symbols GDP UNRATE --data-type economic
```

### 4. List providers

```bash
finance-download providers
```

### 5. Check download status

```bash
finance-download status --output-dir ./data
```

## Configuration

Create a JSON config file (see `configs/example_config.json`):

```json
{
  "output_dir": "./data",
  "storage_format": "parquet",
  "log_level": "INFO",
  "providers": {
    "yahoo": {},
    "fred": { "api_key_env": "FRED_API_KEY" }
  },
  "jobs": [
    {
      "name": "us_equities",
      "provider": "yahoo",
      "data_type": "eod_prices",
      "symbols": ["AAPL", "MSFT"],
      "start_date": "2020-01-01",
      "output_subdir": "equities/eod"
    }
  ]
}
```

## Scheduling with Cron

```bash
# Run weekdays at 6 PM (after US market close)
0 18 * * 1-5 /path/to/venv/bin/finance-download run --config /path/to/config.json >> /var/log/finance-download.log 2>&1
```

Re-runs only download data newer than the last successful download (incremental).

## Adding a New Provider

1. Create a new file in `src/finance_downloader/providers/`
2. Subclass `BaseProvider`:

```python
from finance_downloader.core.base_provider import BaseProvider
from finance_downloader.core.models import DataType, DownloadJob, ProviderConfig

class MyProvider(BaseProvider):
    name = "my_provider"
    supported_data_types = [DataType.EOD_PRICES]

    def download(self, job, start_override=None):
        # Your download logic here
        return df

    def get_last_available_date(self, symbol, data_type):
        return None
```

3. The provider is automatically discovered — no registration code needed.

## Project Structure

```
src/finance_downloader/
├── cli.py               # CLI entry point
├── config.py            # Config loading
├── runner.py            # Job orchestration
├── core/
│   ├── base_provider.py # Abstract base class
│   ├── registry.py      # Auto-discovery registry
│   ├── models.py        # Pydantic data models
│   └── storage.py       # Parquet/CSV + metadata
├── providers/
│   ├── yahoo.py         # Yahoo Finance
│   ├── fred.py          # FRED
│   ├── eodhd.py         # EODHD
│   ├── databento_provider.py
│   ├── alpha_vantage.py
│   ├── finnhub_provider.py
│   └── sec_edgar.py     # SEC EDGAR
└── utils/
    ├── logging.py       # loguru config
    └── rate_limiter.py  # Token-bucket rate limiter
```

## Testing

```bash
pytest
pytest --cov=finance_downloader
```
