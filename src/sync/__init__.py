"""
Hudi catalog sync tool implementations.
Abstract base and concrete types: Hive, BigQuery, Glue, DataHub.
"""

from typing import Optional

from .base import AbstractSyncTool
from .hive import HiveSyncTool
from .bigquery import BigQuerySyncTool
from .glue import GlueSyncTool
from .datahub import DataHubSyncTool

# Registry: sync_type string -> concrete class (for factory)
SYNC_TYPE_REGISTRY = {
    "hive": HiveSyncTool,
    "bigquery": BigQuerySyncTool,
    "glue": GlueSyncTool,
    "datahub": DataHubSyncTool,
}

VALID_SYNC_TYPES = tuple(SYNC_TYPE_REGISTRY.keys())


def get_sync_tool(
    sync_type: str,
    config_path: Optional[str] = None,
    config: Optional[dict] = None,
    **overrides,
) -> AbstractSyncTool:
    """
    Factory: return an AbstractSyncTool implementation for the given sync type.
    """
    sync_type = sync_type.lower().strip()
    if sync_type not in SYNC_TYPE_REGISTRY:
        raise ValueError(
            f"Unknown sync_type: {sync_type}. Valid types: {list(SYNC_TYPE_REGISTRY)}"
        )
    cls = SYNC_TYPE_REGISTRY[sync_type]
    return cls(config_path=config_path, config=config, **overrides)


__all__ = [
    "AbstractSyncTool",
    "HiveSyncTool",
    "BigQuerySyncTool",
    "GlueSyncTool",
    "DataHubSyncTool",
    "SYNC_TYPE_REGISTRY",
    "VALID_SYNC_TYPES",
    "get_sync_tool",
]
