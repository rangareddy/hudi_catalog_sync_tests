"""
Load and expose configuration from config.yaml.
Provides typed access to global and per-sync-type (hive, bigquery, glue, datahub) config.
"""

import os
from pathlib import Path
from typing import Any, Optional

import yaml

from .logging_config import get_logger

logger = get_logger(__name__)
# Project root config.yaml (util is src/util, so parent.parent.parent = project root)
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config.yaml"


def load_config(config_path: Optional[os.PathLike] = None) -> dict[str, Any]:
    """Load config.yaml and return the full configuration dict."""
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    if not path.is_file():
        logger.error("Config file not found: %s", path)
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}
    logger.debug("Loaded config from %s", path)
    return config


def _deep_merge(base: dict, override: dict) -> dict:
    """Merge override into base (override wins). Nested dicts merged recursively."""
    result = dict(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def get_global_config(config: Optional[dict] = None, config_path: Optional[os.PathLike] = None) -> dict[str, Any]:
    """Return global configuration, with defaults for missing keys."""
    if config is None:
        config = load_config(config_path)
    return config.get("global", {})


def get_sync_config(
    sync_type: str,
    config: Optional[dict] = None,
    config_path: Optional[os.PathLike] = None,
) -> dict[str, Any]:
    """
    Return configuration for a given sync type (hive, bigquery, glue, datahub).
    Merges global defaults with sync-specific values. Empty strings for table_name/table/base_path
    are filled from global.table_name and global.base_path.
    """
    if config is None:
        config = load_config(config_path)
    global_cfg = get_global_config(config=config)
    sync_key = sync_type.lower().strip()
    if sync_key not in config:
        raise KeyError(f"Unknown sync type: '{sync_type}'. Expected one of: hive, bigquery, glue, datahub")
    sync_cfg = _deep_merge(global_cfg, config[sync_key])

    # Resolve empty table/base_path from global
    default_table = global_cfg.get("table_name", "")
    if not (sync_cfg.get("table_name") or sync_cfg.get("table")):
        sync_cfg["table_name"] = default_table
        sync_cfg["table"] = default_table
    else:
        sync_cfg["table_name"] = sync_cfg.get("table_name") or sync_cfg.get("table") or default_table
        sync_cfg["table"] = sync_cfg.get("table") or sync_cfg.get("table_name") or default_table
    if not sync_cfg.get("base_path"):
        sync_cfg["base_path"] = global_cfg.get("base_path", "")

    return sync_cfg
