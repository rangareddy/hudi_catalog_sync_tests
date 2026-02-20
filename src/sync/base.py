"""
Abstract base class for all Hudi catalog sync tool implementations.
Subclasses provide catalog-specific configuration, CLI arguments, and post-sync validation.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from ..util.config_loader import get_sync_config


class AbstractSyncTool(ABC):
    """
    Abstract base for sync tool implementations (Hive, BigQuery, Glue, DataHub).
    Subclasses must implement:
    - get_sync_tool_class_name(): Java class for --sync-tool-classes
    - get_streamer_hoodie_conf(): hoodie-conf key/value for HoodieStreamer
    - get_standalone_tool_args(): CLI args for standalone SyncTool spark-submit
    - validate_config(): return list of validation error messages (empty if valid)
    """

    # Subclasses must set this to the config key (e.g. "hive", "bigquery").
    SYNC_TYPE: str = "base"

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        config_path: Optional[str] = None,
        **overrides: Any,
    ) -> None:
        self._merged_config = get_sync_config(
            self.SYNC_TYPE, config=config, config_path=config_path
        )
        for key, value in overrides.items():
            if value is not None:
                self._merged_config[key] = value

    @property
    def config(self) -> Dict[str, Any]:
        """Resolved configuration (global + sync-type merged, with overrides)."""
        return self._merged_config

    @property
    @abstractmethod
    def sync_tool_class_name(self) -> str:
        """Fully qualified Java class name for HoodieStreamer --sync-tool-classes."""
        ...

    @abstractmethod
    def get_streamer_hoodie_conf(self) -> Dict[str, str]:
        """Hoodie-conf key/value pairs for HoodieStreamer (with sync enabled)."""
        ...

    def get_datasource_hoodie_options(self) -> Dict[str, str]:
        """Options for Spark DataFrameWriter.format('hudi') with meta sync enabled."""
        options: Dict[str, str] = {
            "hoodie.datasource.meta.sync.enable": "true",
            "hoodie.meta.sync.client.tool.class": self.sync_tool_class_name,
        }
        options.update(self.get_streamer_hoodie_conf())
        return options

    @abstractmethod
    def get_standalone_tool_args(self) -> List[str]:
        """CLI argument list for running the sync tool standalone (e.g. spark-submit ... SyncTool ...)."""
        ...

    def validate_config(self) -> List[str]:
        """
        Validate required configuration. Returns a list of error messages; empty if valid.
        Subclasses may override to add catalog-specific checks.
        """
        errors: List[str] = []
        if not self._merged_config.get("base_path"):
            errors.append(f"{self.SYNC_TYPE}: base_path is required")
        table = self._merged_config.get("table_name") or self._merged_config.get("table")
        if not table:
            errors.append(f"{self.SYNC_TYPE}: table_name or table is required")
        return errors

    def validate_environment(self) -> list:
        """
        Run environment-specific validation after sync: database/dataset exists, table exists, table path exists.
        Subclasses override and return a list of ValidationResult (from ..util.validation).
        """
        return []

    def is_enabled(self) -> bool:
        """Whether this sync type is enabled in config."""
        return bool(self._merged_config.get("enabled", True))
