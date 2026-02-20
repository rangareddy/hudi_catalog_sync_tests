"""
Build command-line arguments for HoodieStreamer and standalone sync tools.
Uses the sync package (AbstractSyncTool implementations) and config.
"""

from typing import List, Optional

from .config_loader import load_config
from .logging_config import get_logger
from ..sync import get_sync_tool

logger = get_logger(__name__)


def hoodie_conf_to_cli_args(hoodie_conf: dict[str, str]) -> List[str]:
    """Convert a dict of hoodie-conf key/value to a list of --hoodie-conf k=v for spark-submit."""
    result: List[str] = []
    for key, value in hoodie_conf.items():
        if value is None or value == "":
            continue
        result.append("--hoodie-conf")
        result.append(f"{key}={value}")
    return result


def build_streamer_sync_args(
    sync_type: str,
    config_path: Optional[str] = None,
    base_path: Optional[str] = None,
    table_name: Optional[str] = None,
    config: Optional[dict] = None,
) -> List[str]:
    """
    Build arguments for HoodieStreamer with sync enabled:
    --enable-sync, --sync-tool-classes <class>, and all --hoodie-conf for that sync type.
    """
    tool = get_sync_tool(
        sync_type,
        config_path=config_path,
        config=config,
        base_path=base_path,
        table_name=table_name,
    )
    validation_errors = tool.validate_config()
    if validation_errors:
        raise ValueError("Invalid config: " + "; ".join(validation_errors))
    args: List[str] = [
        "--enable-sync",
        "--sync-tool-classes",
        tool.sync_tool_class_name,
    ]
    args.extend(hoodie_conf_to_cli_args(tool.get_streamer_hoodie_conf()))
    logger.debug("Built streamer sync args for sync_type=%s (%d --hoodie-conf)", sync_type, len(tool.get_streamer_hoodie_conf()))
    return args


def build_standalone_sync_args(
    sync_type: str,
    config_path: Optional[str] = None,
    base_path: Optional[str] = None,
    table_name: Optional[str] = None,
    config: Optional[dict] = None,
) -> List[str]:
    """Build CLI arguments for the standalone sync tool (e.g. spark-submit ... SyncTool ...)."""
    tool = get_sync_tool(
        sync_type,
        config_path=config_path,
        config=config,
        base_path=base_path,
        table_name=table_name,
    )
    validation_errors = tool.validate_config()
    if validation_errors:
        raise ValueError("Invalid config: " + "; ".join(validation_errors))
    args = tool.get_standalone_tool_args()
    logger.debug("Built standalone sync args for sync_type=%s (%d args)", sync_type, len(args))
    return args
