"""Tests for CommandBuilder."""

import pytest

from src.util.command_builder import (
    CommandBuilder,
    build_inline_command,
    build_ingestion_only_command,
    build_standalone_sync_command,
    build_datasource_snippet,
)


class TestCommandBuilder:
    def test_build_inline_command_returns_list(self, sample_full_config):
        builder = CommandBuilder(
            config=sample_full_config,
            base_path="gs://b/p",
            table_name="t",
        )
        cmd = builder.build_inline_command("hive")
        assert isinstance(cmd, list)
        assert cmd[0] == "spark-submit"
        assert "--enable-sync" in cmd
        assert any("HoodieStreamer" in str(x) for x in cmd)

    def test_build_ingestion_only_command_no_sync_args(self, sample_full_config):
        builder = CommandBuilder(
            config=sample_full_config,
            base_path="gs://b/p",
            table_name="t",
        )
        cmd = builder.build_ingestion_only_command("glue")
        assert "spark-submit" in cmd
        assert "--enable-sync" not in cmd

    def test_build_standalone_sync_command_has_tool_class(self, sample_full_config):
        builder = CommandBuilder(
            config=sample_full_config,
            base_path="gs://b/p",
            table_name="t",
        )
        cmd = builder.build_standalone_sync_command("glue")
        assert any("AwsGlueCatalogSyncTool" in str(x) for x in cmd)

    def test_build_datasource_snippet_returns_string(self, sample_full_config):
        builder = CommandBuilder(
            config=sample_full_config,
            base_path="gs://b/p",
            table_name="t",
        )
        snippet = builder.build_datasource_snippet("bigquery")
        assert "pyspark" in snippet or "format(\"hudi\")" in snippet
        assert "bigquery" in snippet.lower()

    def test_command_to_string_quotes_args(self):
        out = CommandBuilder.command_to_string(["spark-submit", "--master", "yarn"])
        assert "spark-submit" in out
        assert "yarn" in out

    def test_command_to_string_hoodie_conf_on_same_line(self):
        cmd = ["spark-submit", "--hoodie-conf", "hoodie.datasource.hive_sync.table=stocks_sync_test"]
        out = CommandBuilder.command_to_string(cmd)
        assert "--hoodie-conf" in out
        assert "hoodie.datasource.hive_sync.table=stocks_sync_test" in out
        # --hoodie-conf and its value must be on the same line (no newline between them)
        line_with_hoodie_conf = [l for l in out.split("\n") if "--hoodie-conf" in l][0]
        assert "hoodie.datasource.hive_sync.table=stocks_sync_test" in line_with_hoodie_conf


class TestConvenienceFunctions:
    def test_build_inline_command_convenience(self, sample_full_config):
        cmd = build_inline_command(
            "hive",
            config=sample_full_config,
            base_path="gs://b/p",
            table_name="t",
        )
        assert cmd[0] == "spark-submit"
