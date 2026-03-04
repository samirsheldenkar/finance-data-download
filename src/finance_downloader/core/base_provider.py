"""Abstract base class for all data providers."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from datetime import date

import pandas as pd
from loguru import logger

from finance_downloader.core.models import DataType, DownloadJob, ProviderConfig


class BaseProvider(ABC):
    """
    Abstract base class for financial data providers.

    To add a new provider:
      1. Create a new file in providers/
      2. Subclass BaseProvider
      3. Set `name` and `supported_data_types`
      4. Implement `download()` and `get_last_available_date()`
      5. The provider will be auto-discovered by the registry
    """

    name: str = ""
    supported_data_types: list[DataType] = []

    def __init__(self, config: ProviderConfig | None = None) -> None:
        self.config = config or ProviderConfig()
        self._api_key: str | None = None
        if self.config.api_key_env:
            self._api_key = os.environ.get(self.config.api_key_env)

    @property
    def api_key(self) -> str | None:
        return self._api_key

    def requires_api_key(self) -> bool:
        """Whether this provider requires an API key to function."""
        return self.config.api_key_env is not None

    def is_available(self) -> bool:
        """Check if this provider is properly configured and available."""
        if self.requires_api_key() and not self._api_key:
            logger.warning(
                f"Provider '{self.name}' requires API key via "
                f"${self.config.api_key_env} but it is not set"
            )
            return False
        return True

    @abstractmethod
    def download(self, job: DownloadJob, start_override: date | None = None) -> pd.DataFrame:
        """
        Download data for the given job.

        Args:
            job: The download job specification (contains symbols, data_type, dates, etc.)
            start_override: If set, override job.start_date (used for incremental updates).

        Returns:
            A pandas DataFrame with the downloaded data.
            The DataFrame should have a DatetimeIndex named 'date' for time series data.
        """

    @abstractmethod
    def get_last_available_date(self, symbol: str, data_type: DataType) -> date | None:
        """
        Query the provider for the latest available date for a symbol/data_type.

        Returns None if unknown or unsupported.
        """

    def validate_config(self) -> bool:
        """Validate that the provider configuration is sufficient."""
        if self.requires_api_key() and not self._api_key:
            return False
        return True

    def supports(self, data_type: DataType) -> bool:
        """Check if this provider supports a given data type."""
        return data_type in self.supported_data_types

    def __repr__(self) -> str:
        types = ", ".join(dt.value for dt in self.supported_data_types)
        return f"<{self.__class__.__name__} name='{self.name}' types=[{types}]>"
