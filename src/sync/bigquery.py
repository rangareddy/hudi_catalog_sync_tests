"""
Google BigQuery sync implementation (BigQuerySyncTool).
Validates: BigQuery dataset exists, table exists, GCS table path exists.
"""

from typing import Dict, List

from ..util.validation import (
    ValidationResult,
    validate_bigquery_dataset,
    validate_bigquery_table,
    validate_table_path,
)
from .base import AbstractSyncTool


class BigQuerySyncTool(AbstractSyncTool):
    """Sync Hudi tables to Google BigQuery external tables."""

    SYNC_TYPE = "bigquery"

    @property
    def sync_tool_class_name(self) -> str:
        return self._merged_config.get(
            "sync_tool_class", "org.apache.hudi.gcp.bigquery.BigQuerySyncTool"
        )

    def get_streamer_hoodie_conf(self) -> Dict[str, str]:
        cfg = self._merged_config
        base_path = (cfg.get("base_path") or "").rstrip("/")
        table = cfg.get("table_name") or cfg.get("table", "")
        source_uri = cfg.get("source_uri") or (f"{base_path}/date=*" if base_path else "")
        source_uri_prefix = cfg.get("source_uri_prefix") or (f"{base_path}/" if base_path else "")
        return {
            "hoodie.gcp.bigquery.sync.project_id": cfg.get("project_id", ""),
            "hoodie.gcp.bigquery.sync.dataset_name": cfg.get("dataset_name", ""),
            "hoodie.gcp.bigquery.sync.dataset_location": cfg.get("dataset_location", "us-central1"),
            "hoodie.gcp.bigquery.sync.table_name": table,
            "hoodie.gcp.bigquery.sync.base_path": cfg.get("base_path", ""),
            "hoodie.gcp.bigquery.sync.partition_fields": cfg.get("partition_fields", "date"),
            "hoodie.gcp.bigquery.sync.source_uri": source_uri,
            "hoodie.gcp.bigquery.sync.source_uri_prefix": source_uri_prefix,
            "hoodie.gcp.bigquery.sync.use_file_listing_from_metadata": str(
                cfg.get("use_file_listing_from_metadata", True)
            ).lower(),
            "hoodie.gcp.bigquery.sync.assume_date_partitioning": str(
                cfg.get("assume_date_partitioning", False)
            ).lower(),
            "hoodie.gcp.bigquery.sync.use_bq_manifest_file": str(
                cfg.get("use_bq_manifest_file", True)
            ).lower(),
        }

    def get_standalone_tool_args(self) -> List[str]:
        cfg = self._merged_config
        base_path = (cfg.get("base_path") or "").rstrip("/")
        source_uri = cfg.get("source_uri") or (f"{base_path}/date=*" if base_path else "")
        source_uri_prefix = cfg.get("source_uri_prefix") or (f"{base_path}/" if base_path else "")
        return [
            "--project-id", cfg.get("project_id", ""),
            "--dataset-name", cfg.get("dataset_name", ""),
            "--dataset-location", cfg.get("dataset_location", "us-central1"),
            "--table", cfg.get("table_name") or cfg.get("table", ""),
            "--base-path", cfg.get("base_path", ""),
            "--partitioned-by", cfg.get("partition_fields", "date"),
            "--partition-value-extractor",
            cfg.get(
                "partition_value_extractor",
                "org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor",
            ),
            "--source-uri", source_uri,
            "--source-uri-prefix", source_uri_prefix,
            "--use-file-listing-from-metadata",
        ]

    def validate_config(self) -> List[str]:
        errors = super().validate_config()
        if not self._merged_config.get("project_id"):
            errors.append("bigquery: project_id is required")
        if not self._merged_config.get("dataset_name"):
            errors.append("bigquery: dataset_name is required")
        return errors

    def validate_environment(self) -> List[ValidationResult]:
        """Validate BigQuery dataset, table, and GCS table path (GCP environment)."""
        cfg = self._merged_config
        project_id = cfg.get("project_id", "")
        dataset_name = cfg.get("dataset_name", "")
        table_name = cfg.get("table_name") or cfg.get("table", "")
        base_path = (cfg.get("base_path") or "").strip()

        results: List[ValidationResult] = []
        results.append(validate_bigquery_dataset(project_id, dataset_name))
        results.append(validate_bigquery_table(project_id, dataset_name, table_name))
        if base_path and not base_path.startswith("${"):
            results.append(validate_table_path(base_path))
        return results

    @property
    def spark_packages(self) -> str:
        """Spark --packages string for BigQuery client dependencies."""
        return self._merged_config.get(
            "packages",
            "com.google.cloud:google-cloud-bigquery:2.44.0,com.google.api-client:google-api-client:1.32.1,com.google.http-client:google-http-client-jackson2:1.39.2",
        )
