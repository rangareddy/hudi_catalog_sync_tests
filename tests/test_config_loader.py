"""Tests for config_loader."""

import pytest

from src.util.config_loader import (
    get_global_config,
    get_sync_config,
    load_config,
    DEFAULT_CONFIG_PATH,
)


class TestLoadConfig:
    def test_load_config_default_path(self):
        config = load_config()
        assert isinstance(config, dict)
        assert "global" in config

    def test_load_config_explicit_path(self):
        config = load_config(str(DEFAULT_CONFIG_PATH))
        assert "global" in config

    def test_load_config_file_not_found(self):
        with pytest.raises(FileNotFoundError, match="Config file not found"):
            load_config("/nonexistent/config.yaml")


class TestGetGlobalConfig:
    def test_get_global_config_from_dict(self, sample_full_config):
        g = get_global_config(config=sample_full_config)
        assert g["table_name"] == "stocks_sync_test"
        assert g["base_path"] == "gs://bucket/path"

    def test_get_global_config_empty_without_global_key(self):
        g = get_global_config(config={})
        assert g == {}


class TestGetSyncConfig:
    def test_get_sync_config_merges_global(self, sample_full_config):
        cfg = get_sync_config("hive", config=sample_full_config)
        assert cfg["table_name"] == "stocks_sync_test"
        assert cfg["base_path"] == "gs://bucket/path"
        assert cfg["database"] == "default"

    def test_get_sync_config_unknown_type_raises(self, sample_full_config):
        with pytest.raises(KeyError, match="Unknown sync type"):
            get_sync_config("unknown", config=sample_full_config)

    def test_get_sync_config_bigquery_has_project_and_dataset(self, sample_full_config):
        cfg = get_sync_config("bigquery", config=sample_full_config)
        assert cfg["project_id"] == "my-project"
        assert cfg["dataset_name"] == "my_dataset"
