"""Provider registry with auto-discovery."""

from __future__ import annotations

import importlib
import pkgutil
from typing import TYPE_CHECKING

from loguru import logger

from finance_downloader.core.base_provider import BaseProvider
from finance_downloader.core.models import DataType, ProviderConfig

if TYPE_CHECKING:
    pass


class ProviderRegistry:
    """
    Registry that discovers and manages provider instances.

    Providers are auto-discovered by importing all modules in the providers package.
    Each module should define one or more BaseProvider subclasses.
    """

    def __init__(self) -> None:
        self._providers: dict[str, type[BaseProvider]] = {}
        self._instances: dict[str, BaseProvider] = {}

    def register(self, provider_cls: type[BaseProvider]) -> None:
        """Register a provider class."""
        if not provider_cls.name:
            raise ValueError(f"Provider class {provider_cls.__name__} has no 'name' attribute")
        logger.debug(f"Registered provider: {provider_cls.name}")
        self._providers[provider_cls.name] = provider_cls

    def discover_providers(self) -> None:
        """Auto-discover providers by importing all modules in the providers package."""
        import finance_downloader.providers as providers_pkg

        for _importer, modname, _ispkg in pkgutil.iter_modules(providers_pkg.__path__):
            try:
                module = importlib.import_module(f"finance_downloader.providers.{modname}")
                # Find all BaseProvider subclasses in the module
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if (
                        isinstance(attr, type)
                        and issubclass(attr, BaseProvider)
                        and attr is not BaseProvider
                        and attr.name  # skip abstract classes without a name
                    ):
                        self.register(attr)
            except Exception as e:
                logger.warning(f"Failed to load provider module '{modname}': {e}")

    def get_provider(
        self, name: str, config: ProviderConfig | None = None
    ) -> BaseProvider:
        """Get or create a provider instance by name."""
        if name in self._instances:
            return self._instances[name]

        if name not in self._providers:
            available = ", ".join(sorted(self._providers.keys()))
            raise ValueError(
                f"Unknown provider '{name}'. Available providers: {available}"
            )

        instance = self._providers[name](config=config)
        self._instances[name] = instance
        return instance

    def initialize_providers(
        self, provider_configs: dict[str, ProviderConfig]
    ) -> None:
        """Initialize all configured providers."""
        for name, config in provider_configs.items():
            if name in self._providers:
                self.get_provider(name, config)
            else:
                logger.warning(f"Provider '{name}' in config but not registered (skipping)")

    def list_providers(self) -> list[dict]:
        """List all registered providers and their capabilities."""
        result = []
        for name, cls in sorted(self._providers.items()):
            result.append(
                {
                    "name": name,
                    "class": cls.__name__,
                    "data_types": [dt.value for dt in cls.supported_data_types],
                    "requires_api_key": cls(config=ProviderConfig()).requires_api_key(),
                }
            )
        return result

    def get_providers_for_data_type(self, data_type: DataType) -> list[str]:
        """Get provider names that support a given data type."""
        return [
            name
            for name, cls in self._providers.items()
            if data_type in cls.supported_data_types
        ]


# Global registry instance
registry = ProviderRegistry()
