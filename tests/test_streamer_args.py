"""Tests for streamer_args (base HoodieStreamer args)."""

import pytest

from src.util.streamer_args import build_base_streamer_args


class TestBuildBaseStreamerArgs:
    def test_returns_list_with_target_base_path_and_table(self, sample_full_config):
        args = build_base_streamer_args(
            config=sample_full_config,
            base_path="gs://b/p",
            table_name="mytable",
        )
        assert "--target-base-path" in args
        assert "gs://b/p" in args
        assert "--target-table" in args
        assert "mytable" in args

    def test_includes_hoodie_conf_and_op(self, sample_full_config):
        args = build_base_streamer_args(config=sample_full_config)
        assert "--op" in args
        assert "UPSERT" in args
        assert any("recordkey" in a for a in args)
        assert any("precombine" in a for a in args)
