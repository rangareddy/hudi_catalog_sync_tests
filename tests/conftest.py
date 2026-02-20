"""
Pytest fixtures for Hudi catalog sync tests.
"""

import pytest


@pytest.fixture
def sample_global_config():
    """Minimal global config for tests."""
    return {
        "spark_version": "3.5",
        "scala_version": "2.12",
        "hudi_version": "0.16.0-SNAPSHOT",
        "table_type": "COPY_ON_WRITE",
        "base_file_format": "PARQUET",
        "table_name": "stocks_sync_test",
        "base_path": "gs://bucket/path",
        "data_path": "gs://bucket/data",
        "partition_fields": "date",
        "record_key_field": "symbol",
        "precombine_field": "ts",
        "partition_path_field": "date",
        "keygenerator_type": "SIMPLE",
        "hive_style_partitioning": True,
        "metadata_enable": True,
    }


@pytest.fixture
def sample_full_config(sample_global_config):
    """Full config with all sync types (for get_sync_config)."""
    return {
        "global": sample_global_config,
        "hive": {
            "enabled": True,
            "sync_tool_class": "org.apache.hudi.hive.HiveSyncTool",
            "mode": "hms",
            "metastore_uris": "thrift://localhost:9083",
            "database": "default",
            "partition_fields": "date",
        },
        "bigquery": {
            "enabled": True,
            "project_id": "my-project",
            "dataset_name": "my_dataset",
            "dataset_location": "us-central1",
            "partition_fields": "date",
        },
        "glue": {
            "enabled": True,
            "database": "hudi_db",
            "partition_fields": "date",
            "sync_mode": "jdbc",
            "jdbc_url": "jdbc:hive2://localhost:10000",
        },
        "datahub": {
            "enabled": True,
            "emitter_server": "http://localhost:8080",
            "database": "datahub_db",
        },
    }
