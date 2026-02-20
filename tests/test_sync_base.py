"""Tests for AbstractSyncTool and concrete implementations (via HiveSyncTool)."""

import pytest

from src.sync import (
    AbstractSyncTool,
    HiveSyncTool,
    BigQuerySyncTool,
    GlueSyncTool,
    DataHubSyncTool,
    get_sync_tool,
    SYNC_TYPE_REGISTRY,
    VALID_SYNC_TYPES,
)
from src.util.config_loader import get_sync_config


@pytest.fixture
def hive_tool(sample_full_config):
    return HiveSyncTool(config=sample_full_config)


class TestSyncRegistry:
    def test_registry_has_all_four_types(self):
        assert set(SYNC_TYPE_REGISTRY) == {"hive", "bigquery", "glue", "datahub"}

    def test_valid_sync_types_tuple(self):
        assert "hive" in VALID_SYNC_TYPES
        assert "bigquery" in VALID_SYNC_TYPES


class TestGetSyncTool:
    def test_get_sync_tool_returns_correct_type(self, sample_full_config):
        tool = get_sync_tool("hive", config=sample_full_config)
        assert isinstance(tool, HiveSyncTool)
        tool = get_sync_tool("bigquery", config=sample_full_config)
        assert isinstance(tool, BigQuerySyncTool)
        tool = get_sync_tool("glue", config=sample_full_config)
        assert isinstance(tool, GlueSyncTool)
        tool = get_sync_tool("datahub", config=sample_full_config)
        assert isinstance(tool, DataHubSyncTool)

    def test_get_sync_tool_unknown_raises(self, sample_full_config):
        with pytest.raises(ValueError, match="Unknown sync_type"):
            get_sync_tool("unknown", config=sample_full_config)

    def test_get_sync_tool_overrides_applied(self, sample_full_config):
        tool = get_sync_tool(
            "hive",
            config=sample_full_config,
            base_path="s3://custom/path",
            table_name="custom_table",
        )
        assert tool.config["base_path"] == "s3://custom/path"
        assert tool.config["table_name"] == "custom_table"


class TestHiveSyncTool:
    def test_sync_tool_class_name(self, hive_tool):
        assert "HiveSyncTool" in hive_tool.sync_tool_class_name

    def test_get_streamer_hoodie_conf(self, hive_tool):
        conf = hive_tool.get_streamer_hoodie_conf()
        assert "hoodie.datasource.hive_sync.database" in conf
        assert "hoodie.datasource.hive_sync.table" in conf

    def test_get_standalone_tool_args(self, hive_tool):
        args = hive_tool.get_standalone_tool_args()
        assert "--database" in args
        assert "--base-path" in args

    def test_get_datasource_hoodie_options(self, hive_tool):
        opts = hive_tool.get_datasource_hoodie_options()
        assert opts["hoodie.datasource.meta.sync.enable"] == "true"
        assert "HiveSyncTool" in opts["hoodie.meta.sync.client.tool.class"]

    def test_validate_config_success(self, hive_tool):
        errors = hive_tool.validate_config()
        assert errors == []

    def test_validate_config_missing_base_path(self, sample_full_config):
        sample_full_config["global"]["base_path"] = ""
        tool = HiveSyncTool(config=sample_full_config)
        errors = tool.validate_config()
        assert any("base_path" in e for e in errors)


class TestBigQuerySyncTool:
    def test_validate_config_requires_project_and_dataset(self, sample_full_config):
        sample_full_config["bigquery"]["project_id"] = ""
        tool = BigQuerySyncTool(config=sample_full_config)
        errors = tool.validate_config()
        assert any("project_id" in e for e in errors)
        sample_full_config["bigquery"]["project_id"] = "p"
        sample_full_config["bigquery"]["dataset_name"] = ""
        tool = BigQuerySyncTool(config=sample_full_config)
        errors = tool.validate_config()
        assert any("dataset_name" in e for e in errors)

    def test_spark_packages_property(self, sample_full_config):
        tool = BigQuerySyncTool(config=sample_full_config)
        assert "google-cloud-bigquery" in tool.spark_packages


class TestGlueSyncTool:
    def test_validate_config_requires_database(self, sample_full_config):
        sample_full_config["glue"]["database"] = ""
        sample_full_config["glue"]["hive_sync_database"] = ""
        tool = GlueSyncTool(config=sample_full_config)
        errors = tool.validate_config()
        assert any("database" in e for e in errors)


class TestDataHubSyncTool:
    def test_validate_config_requires_emitter_server(self, sample_full_config):
        sample_full_config["datahub"]["emitter_server"] = ""
        tool = DataHubSyncTool(config=sample_full_config)
        errors = tool.validate_config()
        assert any("emitter_server" in e for e in errors)


class TestValidateEnvironment:
    """validate_environment() returns a list of ValidationResult per sync type."""

    def test_bigquery_returns_dataset_table_path_checks(self, sample_full_config):
        tool = BigQuerySyncTool(config=sample_full_config)
        results = tool.validate_environment()
        assert len(results) >= 2  # dataset, table; path if base_path set
        from src.util.validation import ValidationResult
        for r in results:
            assert isinstance(r, ValidationResult)
            assert r.check_name in ("bigquery_dataset", "bigquery_table", "gcs_path", "table_path")

    def test_glue_returns_database_table_path_checks(self, sample_full_config):
        tool = GlueSyncTool(config=sample_full_config)
        results = tool.validate_environment()
        assert len(results) >= 2
        from src.util.validation import ValidationResult
        for r in results:
            assert isinstance(r, ValidationResult)

    def test_hive_returns_path_check_when_base_path_set(self, sample_full_config):
        tool = HiveSyncTool(config=sample_full_config)
        results = tool.validate_environment()
        assert len(results) == 1
        assert results[0].check_name in ("local_path", "table_path", "gcs_path", "s3_path")

    def test_datahub_returns_dataset_and_path_checks(self, sample_full_config):
        tool = DataHubSyncTool(config=sample_full_config)
        results = tool.validate_environment()
        assert len(results) >= 1
        from src.util.validation import ValidationResult
        for r in results:
            assert isinstance(r, ValidationResult)
