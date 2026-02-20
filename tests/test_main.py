"""Tests for __main__ CLI (help, mode dispatch)."""

import pytest

from src import SYNC_TYPE_REGISTRY
from src.__main__ import MODES


class TestCliOptions:
    def test_modes_include_inline_separate_datasource_validate(self):
        assert "inline" in MODES
        assert "separate" in MODES
        assert "datasource" in MODES
        assert "validate" in MODES
        assert len(MODES) == 4

    def test_sync_types_match_registry(self):
        assert set(SYNC_TYPE_REGISTRY) == {"hive", "bigquery", "glue", "datahub"}
