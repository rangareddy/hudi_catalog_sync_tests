"""
AWS Glue Data Catalog sync implementation (AwsGlueCatalogSyncTool).
Validates: Glue database exists, table exists, S3 table path exists.
"""

from typing import Dict, List

from ..util.validation import (
    ValidationResult,
    validate_glue_database,
    validate_glue_table,
    validate_table_path,
)
from .base import AbstractSyncTool


class GlueSyncTool(AbstractSyncTool):
    """Sync Hudi tables to AWS Glue Data Catalog."""

    SYNC_TYPE = "glue"

    @property
    def sync_tool_class_name(self) -> str:
        return self._merged_config.get(
            "sync_tool_class", "org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool"
        )

    def get_streamer_hoodie_conf(self) -> Dict[str, str]:
        cfg = self._merged_config
        database = cfg.get("database") or cfg.get("hive_sync_database", "hudi_db")
        table = cfg.get("table_name") or cfg.get("table") or cfg.get("hive_sync_table", "")
        return {
            "hoodie.datasource.meta_sync.condition.sync": str(
                cfg.get("meta_sync_condition_sync", True)
            ).lower(),
            "hoodie.datasource.hive_sync.mode": cfg.get("sync_mode", "jdbc"),
            "hoodie.datasource.hive_sync.jdbcurl": cfg.get(
                "jdbc_url", "jdbc:hive2://localhost:10000"
            ),
            "hoodie.datasource.hive_sync.database": database,
            "hoodie.datasource.hive_sync.table": table,
            "hoodie.datasource.hive_sync.partition_fields": cfg.get("partition_fields", "date"),
            "hoodie.datasource.hive_sync.partition_extractor_class": cfg.get(
                "partition_value_extractor",
                "org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor",
            ),
        }

    def get_standalone_tool_args(self) -> List[str]:
        cfg = self._merged_config
        return [
            "--database", cfg.get("database", "hudi_db"),
            "--table", cfg.get("table_name") or cfg.get("table", ""),
            "--base-path", cfg.get("base_path", ""),
            "--partitioned-by", cfg.get("partition_fields", "date"),
            "--partition-value-extractor",
            cfg.get(
                "partition_value_extractor",
                "org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor",
            ),
            "--sync-mode", cfg.get("sync_mode", "jdbc"),
            "--jdbc-url", cfg.get("jdbc_url", "jdbc:hive2://localhost:10000"),
        ]

    def validate_config(self) -> List[str]:
        errors = super().validate_config()
        if not self._merged_config.get("database") and not self._merged_config.get(
            "hive_sync_database"
        ):
            errors.append("glue: database or hive_sync_database is required")
        return errors

    def validate_environment(self) -> List[ValidationResult]:
        """Validate Glue database, table, and S3 table path (AWS environment)."""
        cfg = self._merged_config
        database = cfg.get("database") or cfg.get("hive_sync_database", "hudi_db")
        table_name = cfg.get("table_name") or cfg.get("table", "")
        base_path = (cfg.get("base_path") or "").strip()

        results: List[ValidationResult] = []
        results.append(validate_glue_database(database))
        results.append(validate_glue_table(database, table_name))
        if base_path and not base_path.startswith("${"):
            results.append(validate_table_path(base_path))
        return results
