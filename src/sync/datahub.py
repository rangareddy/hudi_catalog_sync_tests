"""
DataHub metadata sync implementation (DataHubSyncTool).
Validates: dataset entity exists in DataHub, table path exists.
"""

from typing import Dict, List

from ..util.validation import (
    ValidationResult,
    validate_datahub_dataset,
    validate_table_path,
)
from .base import AbstractSyncTool


class DataHubSyncTool(AbstractSyncTool):
    """Sync Hudi table metadata to DataHub (schema, properties, last sync time)."""

    SYNC_TYPE = "datahub"

    @property
    def sync_tool_class_name(self) -> str:
        return self._merged_config.get(
            "sync_tool_class", "org.apache.hudi.sync.datahub.DataHubSyncTool"
        )

    def get_streamer_hoodie_conf(self) -> Dict[str, str]:
        cfg = self._merged_config
        conf: Dict[str, str] = {
            "hoodie.meta.sync.datahub.emitter.server": cfg.get(
                "emitter_server", "http://localhost:8080"
            ),
            "hoodie.datasource.hive_sync.database": cfg.get("database", "datahub_db"),
            "hoodie.datasource.hive_sync.table": cfg.get("table_name") or cfg.get("table", ""),
        }
        schema_length_thresh = cfg.get("schema_string_length_thresh")
        if schema_length_thresh is not None:
            conf["hoodie.datasource.hive_sync.schema_string_length_thresh"] = str(
                schema_length_thresh
            )
        if cfg.get("table_properties"):
            conf["hoodie.meta.sync.datahub.table.properties"] = cfg["table_properties"]
        if cfg.get("emit_log_metrics") is not None:
            conf["hoodie.meta.sync.datahub.emit.log.metrics"] = str(
                cfg["emit_log_metrics"]
            ).lower()
        return conf

    def get_standalone_tool_args(self) -> List[str]:
        cfg = self._merged_config
        return [
            "--emitter-server", cfg.get("emitter_server", "http://localhost:8080"),
            "--database", cfg.get("database", "datahub_db"),
            "--table", cfg.get("table_name") or cfg.get("table", ""),
            "--base-path", cfg.get("base_path", ""),
        ]

    def validate_config(self) -> List[str]:
        errors = super().validate_config()
        if not self._merged_config.get("emitter_server"):
            errors.append("datahub: emitter_server is required")
        return errors

    def validate_environment(self) -> List[ValidationResult]:
        """Validate DataHub dataset entity and table path."""
        cfg = self._merged_config
        emitter = (cfg.get("emitter_server") or "").strip()
        database = cfg.get("database", "datahub_db")
        table_name = cfg.get("table_name") or cfg.get("table", "")
        base_path = (cfg.get("base_path") or "").strip()

        results: List[ValidationResult] = []
        if emitter and table_name:
            results.append(validate_datahub_dataset(emitter, database, table_name))
        if base_path and not base_path.startswith("${"):
            results.append(validate_table_path(base_path))
        return results
