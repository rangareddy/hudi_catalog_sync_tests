"""Tests for sync_runner (streamer sync args and standalone args)."""

import pytest

from src.util.sync_runner import (
    build_streamer_sync_args,
    build_standalone_sync_args,
    hoodie_conf_to_cli_args,
)


class TestHoodieConfToCliArgs:
    def test_converts_dict_to_cli_pairs(self):
        out = hoodie_conf_to_cli_args({"a": "1", "b": "2"})
        assert out == ["--hoodie-conf", "a=1", "--hoodie-conf", "b=2"]

    def test_skips_empty_values(self):
        out = hoodie_conf_to_cli_args({"a": "1", "b": ""})
        assert "b=" not in " ".join(out)


class TestBuildStreamerSyncArgs:
    def test_includes_enable_sync_and_tool_class(self, sample_full_config):
        args = build_streamer_sync_args(
            "hive",
            config=sample_full_config,
            base_path="gs://b/p",
            table_name="t",
        )
        assert "--enable-sync" in args
        assert "--sync-tool-classes" in args
        assert any("HiveSyncTool" in str(x) for x in args)

    def test_invalid_config_raises(self, sample_full_config):
        sample_full_config["global"]["base_path"] = ""
        with pytest.raises(ValueError, match="Invalid config"):
            build_streamer_sync_args("hive", config=sample_full_config)


class TestBuildStandaloneSyncArgs:
    def test_returns_tool_args(self, sample_full_config):
        args = build_standalone_sync_args(
            "glue",
            config=sample_full_config,
            base_path="s3://b/p",
            table_name="t",
        )
        assert "--database" in args
        assert "--base-path" in args
        assert "--table" in args
