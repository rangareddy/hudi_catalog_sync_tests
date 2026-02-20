"""
Build base HoodieStreamer command-line arguments from config.
Used by inline and separate modes; sync-specific args come from sync_runner.
"""

from typing import List, Optional

from .config_loader import get_global_config, load_config
from .logging_config import get_logger

logger = get_logger(__name__)


def build_base_streamer_args(
    config: Optional[dict] = None,
    config_path: Optional[str] = None,
    base_path: Optional[str] = None,
    table_name: Optional[str] = None,
    data_path: Optional[str] = None,
) -> List[str]:
    """
    Build the list of arguments for HoodieStreamer (without sync).
    Uses global config; base_path/table_name/data_path can override.
    """
    if config is None:
        config = load_config(config_path)
    global_cfg = get_global_config(config=config)
    resolved_data_path = data_path or global_cfg.get("data_path") or "${DATA_PATH}"
    resolved_base_path = base_path or global_cfg.get("base_path") or ""
    resolved_table_name = table_name or global_cfg.get("table_name") or "stocks_sync_test"

    args: List[str] = [
        "--target-base-path", resolved_base_path,
        "--target-table", resolved_table_name,
        "--table-type", global_cfg.get("table_type", "COPY_ON_WRITE"),
        "--base-file-format", global_cfg.get("base_file_format", "PARQUET"),
        "--props", f"{resolved_data_path.rstrip('/')}/hoodie.properties",
        "--source-class", "org.apache.hudi.utilities.sources.JsonDFSSource",
        "--source-ordering-field", global_cfg.get("precombine_field", "ts"),
        "--payload-class", "org.apache.hudi.common.model.DefaultHoodieRecordPayload",
        "--schemaprovider-class", "org.apache.hudi.utilities.schema.FilebasedSchemaProvider",
        "--hoodie-conf", f"hoodie.streamer.schemaprovider.source.schema.file={resolved_data_path.rstrip('/')}/schema.avsc",
        "--hoodie-conf", f"hoodie.streamer.schemaprovider.target.schema.file={resolved_data_path.rstrip('/')}/schema.avsc",
        "--hoodie-conf", f"hoodie.streamer.source.dfs.root={resolved_data_path.rstrip('/')}/input/",
        "--hoodie-conf", f"hoodie.datasource.write.recordkey.field={global_cfg.get('record_key_field', 'symbol')}",
        "--hoodie-conf", f"hoodie.datasource.write.partitionpath.field={global_cfg.get('partition_path_field', 'date')}",
        "--hoodie-conf", f"hoodie.datasource.write.precombine.field={global_cfg.get('precombine_field', 'ts')}",
        "--hoodie-conf", f"hoodie.datasource.write.keygenerator.type={global_cfg.get('keygenerator_type', 'SIMPLE')}",
        "--hoodie-conf", f"hoodie.datasource.write.hive_style_partitioning={str(global_cfg.get('hive_style_partitioning', True)).lower()}",
        "--hoodie-conf", f"hoodie.metadata.enable={str(global_cfg.get('metadata_enable', True)).lower()}",
        "--op", "UPSERT",
    ]
    logger.debug(
        "Built base streamer args: table=%s base_path=%s",
        resolved_table_name,
        resolved_base_path or "(empty)",
    )
    return args
