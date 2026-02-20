"""
Hive Metastore sync implementation (HiveSyncTool).
Validates: table path exists (local, HDFS, or cloud); Hive DB/table checks optional (require beeline/jdbc).
"""

from typing import Dict, List

from ..util.validation import ValidationResult, validate_table_path
from .base import AbstractSyncTool


class HiveSyncTool(AbstractSyncTool):
    """Sync Hudi tables to Hive Metastore (HMS or JDBC)."""

    SYNC_TYPE = "hive"

    @property
    def sync_tool_class_name(self) -> str:
        return self._merged_config.get(
            "sync_tool_class", "org.apache.hudi.hive.HiveSyncTool"
        )

    def get_streamer_hoodie_conf(self) -> Dict[str, str]:
        cfg = self._merged_config
        database = cfg.get("database", "default")
        table = cfg.get("table_name") or cfg.get("table", "")
        partition_fields = cfg.get("partition_fields", "date")
        partition_extractor = cfg.get(
            "partition_value_extractor",
            "org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor",
        )
        mode = cfg.get("mode", "hms")
        conf: Dict[str, str] = {
            "hoodie.datasource.hive_sync.database": database,
            "hoodie.datasource.hive_sync.table": table,
            "hoodie.datasource.hive_sync.partition_fields": partition_fields,
            "hoodie.datasource.hive_sync.partition_extractor_class": partition_extractor,
            "hoodie.datasource.hive_sync.mode": mode,
        }
        if mode == "hms":
            conf["hoodie.datasource.hive_sync.metastore.uris"] = cfg.get(
                "metastore_uris", "thrift://localhost:9083"
            )
        else:
            conf["hoodie.datasource.hive_sync.jdbcurl"] = cfg.get(
                "jdbc_url", "jdbc:hive2://localhost:10000"
            )
        schema_length_thresh = cfg.get("schema_string_length_thresh")
        if schema_length_thresh is not None:
            conf["hoodie.datasource.hive_sync.schema_string_length_thresh"] = str(
                schema_length_thresh
            )
        return conf

    def get_standalone_tool_args(self) -> List[str]:
        cfg = self._merged_config
        mode = cfg.get("mode", "jdbc")
        args: List[str] = [
            "--database", cfg.get("database", "default"),
            "--table", cfg.get("table_name") or cfg.get("table", ""),
            "--base-path", cfg.get("base_path", ""),
            "--partitioned-by", cfg.get("partition_fields", "date"),
            "--partition-value-extractor",
            cfg.get(
                "partition_value_extractor",
                "org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor",
            ),
            "--sync-mode", mode,
        ]
        if mode == "jdbc":
            args.extend(["--jdbc-url", cfg.get("jdbc_url", "jdbc:hive2://localhost:10000")])
        return args

    def validate_environment(self) -> List[ValidationResult]:
        """Validate table path exists (local, GCS, or S3). Hive metastore checks require beeline/jdbc."""
        base_path = (self._merged_config.get("base_path") or "").strip()
        if not base_path or base_path.startswith("${"):
            return []
        return [validate_table_path(base_path)]
