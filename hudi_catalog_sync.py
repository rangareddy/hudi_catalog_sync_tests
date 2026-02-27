#!/usr/bin/env python3
"""
Hudi Catalog Sync Tests - single-file implementation.

Run Hudi catalog sync tests by sync type and mode.

Usage:
  python hudi_catalog_sync.py --sync-type bigquery --mode inline
  python hudi_catalog_sync.py --sync-type glue --mode separate --config config.yaml
  python hudi_catalog_sync.py --sync-type bigquery --mode datasource --base-path gs://bucket/path --table-name my_table

Modes:
  inline    - HoodieStreamer with sync enabled (ingestion + sync in one job)
  separate  - HoodieStreamer without sync, then standalone SyncTool
  datasource - Spark DataFrame write with meta sync (snippet to run in spark-shell/pyspark)
  validate  - Run environment validation only (dataset/database exists, table exists, table path exists)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shlex
import subprocess
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

# -----------------------------------------------------------------------------
# Paths and constants
# -----------------------------------------------------------------------------
_SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = _SCRIPT_DIR / "config.yaml"
DEFAULT_TABLE_NAME = "stocks_sync_test"
MODES = ("inline", "separate", "datasource", "validate")
DEFAULT_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_configured_root = False


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def setup_logging(
    level: Optional[str] = None,
    format_string: Optional[str] = None,
    stream: Optional[object] = None,
    force: bool = False,
) -> None:
    global _configured_root
    if _configured_root and not force:
        return
    level = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    format_string = format_string or os.environ.get("LOG_FORMAT", DEFAULT_FORMAT)
    stream = stream or sys.stderr
    numeric_level = getattr(logging, level, logging.INFO)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO
    handler = logging.StreamHandler(stream)
    handler.setLevel(numeric_level)
    handler.setFormatter(logging.Formatter(format_string, datefmt=DEFAULT_DATE_FORMAT))
    root = logging.getLogger()
    root.setLevel(numeric_level)
    for h in root.handlers[:]:
        root.removeHandler(h)
    root.addHandler(handler)
    _configured_root = True


# -----------------------------------------------------------------------------
# Config loader
# -----------------------------------------------------------------------------
def _deep_merge(base: dict, override: dict) -> dict:
    result = dict(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def load_config(config_path: Optional[os.PathLike] = None) -> dict[str, Any]:
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    if not path.is_file():
        get_logger(__name__).error("Config file not found: %s", path)
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}
    get_logger(__name__).debug("Loaded config from %s", path)
    return config


def get_global_config(config: Optional[dict] = None, config_path: Optional[os.PathLike] = None) -> dict[str, Any]:
    if config is None:
        config = load_config(config_path)
    return config.get("global", {})


def get_sync_config(
    sync_type: str,
    config: Optional[dict] = None,
    config_path: Optional[os.PathLike] = None,
) -> dict[str, Any]:
    if config is None:
        config = load_config(config_path)
    global_cfg = get_global_config(config=config)
    sync_key = sync_type.lower().strip()
    if sync_key not in config:
        raise KeyError(f"Unknown sync type: '{sync_type}'. Expected one of: hive, bigquery, glue, datahub")
    sync_cfg = _deep_merge(global_cfg, config[sync_key])
    default_table = global_cfg.get("table_name", "")
    if not (sync_cfg.get("table_name") or sync_cfg.get("table")):
        sync_cfg["table_name"] = default_table
        sync_cfg["table"] = default_table
    else:
        sync_cfg["table_name"] = sync_cfg.get("table_name") or sync_cfg.get("table") or default_table
        sync_cfg["table"] = sync_cfg.get("table") or sync_cfg.get("table_name") or default_table
    if not sync_cfg.get("base_path"):
        sync_cfg["base_path"] = global_cfg.get("base_path", "")
    return sync_cfg


# -----------------------------------------------------------------------------
# Validation
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class ValidationResult:
    check_name: str
    success: bool
    message: str

    def __str__(self) -> str:
        status = "PASS" if self.success else "FAIL"
        return f"[{status}] {self.check_name}: {self.message}"


def _run_cmd(cmd: List[str], timeout_seconds: int = 30) -> tuple[bool, str]:
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_seconds)
        if result.returncode == 0:
            return True, ""
        return False, (result.stderr or result.stdout or f"exit code {result.returncode}").strip()
    except FileNotFoundError:
        return False, f"Command not found: {cmd[0]}"
    except subprocess.TimeoutExpired:
        return False, "Command timed out"


def validate_gcs_path(path: str) -> ValidationResult:
    if not path.startswith("gs://"):
        return ValidationResult("gcs_path", False, f"Not a GCS path: {path}")
    path = path.rstrip("/") + "/"
    ok, err = _run_cmd(["gcloud", "storage", "ls", path])
    if ok:
        return ValidationResult("gcs_path", True, f"Path exists: {path}")
    if "gcloud" in err and ("not found" in err.lower() or "No such object" in err):
        return ValidationResult("gcs_path", False, f"Path not found: {path}")
    return ValidationResult("gcs_path", False, err or f"Path not accessible: {path}")


def validate_bigquery_dataset(project_id: str, dataset_name: str) -> ValidationResult:
    if not project_id or not dataset_name:
        return ValidationResult("bigquery_dataset", False, "project_id and dataset_name required")
    ok, err = _run_cmd(["bq", "show", "--dataset", f"{project_id}:{dataset_name}"])
    if ok:
        return ValidationResult("bigquery_dataset", True, f"Dataset {project_id}:{dataset_name} exists")
    return ValidationResult("bigquery_dataset", False, err or "Dataset not found")


def validate_bigquery_table(project_id: str, dataset_name: str, table_name: str) -> ValidationResult:
    if not project_id or not dataset_name or not table_name:
        return ValidationResult("bigquery_table", False, "project_id, dataset_name, table_name required")
    ok, err = _run_cmd(["bq", "show", f"{project_id}:{dataset_name}.{table_name}"])
    if ok:
        return ValidationResult("bigquery_table", True, f"Table {dataset_name}.{table_name} exists")
    return ValidationResult("bigquery_table", False, err or "Table not found")


def validate_s3_path(path: str) -> ValidationResult:
    s3_path = path.replace("s3a://", "s3://", 1).rstrip("/") + "/"
    if not s3_path.startswith("s3://"):
        return ValidationResult("s3_path", False, f"Not an S3 path: {path}")
    ok, err = _run_cmd(["aws", "s3", "ls", s3_path])
    if ok:
        return ValidationResult("s3_path", True, f"Path exists: {s3_path}")
    return ValidationResult("s3_path", False, err or f"Path not accessible: {s3_path}")


def validate_glue_database(database_name: str) -> ValidationResult:
    if not database_name:
        return ValidationResult("glue_database", False, "database name required")
    ok, err = _run_cmd(["aws", "glue", "get-database", "--name", database_name])
    if ok:
        return ValidationResult("glue_database", True, f"Database {database_name} exists")
    return ValidationResult("glue_database", False, err or "Database not found")


def validate_glue_table(database_name: str, table_name: str) -> ValidationResult:
    if not database_name or not table_name:
        return ValidationResult("glue_table", False, "database and table name required")
    ok, err = _run_cmd([
        "aws", "glue", "get-table",
        "--database-name", database_name,
        "--name", table_name,
    ])
    if ok:
        return ValidationResult("glue_table", True, f"Table {database_name}.{table_name} exists")
    return ValidationResult("glue_table", False, err or "Table not found")


def validate_local_path(path: str) -> ValidationResult:
    if not path:
        return ValidationResult("local_path", False, "path is empty")
    if path.startswith("gs://") or path.startswith("s3://") or path.startswith("s3a://"):
        return ValidationResult("local_path", False, f"Not a local path: {path}")
    if os.path.isdir(path):
        return ValidationResult("local_path", True, f"Path exists: {path}")
    return ValidationResult("local_path", False, f"Path not found or not a directory: {path}")


def validate_datahub_dataset(emitter_server: str, database_name: str, table_name: str) -> ValidationResult:
    if not emitter_server or not table_name:
        return ValidationResult("datahub_dataset", False, "emitter_server and table_name required")
    search_url = f"{emitter_server.rstrip('/')}/entities?action=search"
    payload = {"entity": "dataset", "start": 0, "count": 10, "input": f"{database_name}.{table_name}"}
    try:
        result = subprocess.run(
            ["curl", "-s", "-X", "POST", search_url, "-H", "Content-Type: application/json", "-d", json.dumps(payload)],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode != 0:
            return ValidationResult("datahub_dataset", False, result.stderr or "curl failed")
        out = result.stdout
        if not out:
            return ValidationResult("datahub_dataset", False, "Empty response from DataHub")
        data = json.loads(out)
        entities = (data.get("value") or {}).get("entities") or []
        match = f"{database_name}.{table_name}"
        for e in entities:
            if e.get("entity") == match or match in str(e.get("entity", "")):
                return ValidationResult("datahub_dataset", True, f"Dataset found in DataHub: {match}")
        return ValidationResult("datahub_dataset", False, f"Dataset not found in DataHub: {match}")
    except FileNotFoundError:
        return ValidationResult("datahub_dataset", False, "curl not found")
    except Exception as e:
        return ValidationResult("datahub_dataset", False, str(e))


def validate_table_path(base_path: str) -> ValidationResult:
    if not base_path or base_path.startswith("${"):
        return ValidationResult("table_path", False, "base_path not set or placeholder")
    if base_path.startswith("gs://"):
        return validate_gcs_path(base_path)
    if base_path.startswith("s3://") or base_path.startswith("s3a://"):
        return validate_s3_path(base_path)
    return validate_local_path(base_path)


# -----------------------------------------------------------------------------
# Sync tools (abstract base + Hive, BigQuery, Glue, DataHub)
# -----------------------------------------------------------------------------
class AbstractSyncTool(ABC):
    SYNC_TYPE: str = "base"

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        config_path: Optional[str] = None,
        **overrides: Any,
    ) -> None:
        self._merged_config = get_sync_config(self.SYNC_TYPE, config=config, config_path=config_path)
        for key, value in overrides.items():
            if value is not None:
                self._merged_config[key] = value

    @property
    def config(self) -> Dict[str, Any]:
        return self._merged_config

    @property
    @abstractmethod
    def sync_tool_class_name(self) -> str:
        ...

    @abstractmethod
    def get_streamer_hoodie_conf(self) -> Dict[str, str]:
        ...

    def get_datasource_hoodie_options(self) -> Dict[str, str]:
        options: Dict[str, str] = {
            "hoodie.datasource.meta.sync.enable": "true",
            "hoodie.meta.sync.client.tool.class": self.sync_tool_class_name,
        }
        options.update(self.get_streamer_hoodie_conf())
        return options

    @abstractmethod
    def get_standalone_tool_args(self) -> List[str]:
        ...

    def validate_config(self) -> List[str]:
        errors: List[str] = []
        if not self._merged_config.get("base_path"):
            errors.append(f"{self.SYNC_TYPE}: base_path is required")
        table = self._merged_config.get("table_name") or self._merged_config.get("table")
        if not table:
            errors.append(f"{self.SYNC_TYPE}: table_name or table is required")
        return errors

    def validate_environment(self) -> list:
        return []

    def is_enabled(self) -> bool:
        return bool(self._merged_config.get("enabled", True))


class HiveSyncTool(AbstractSyncTool):
    SYNC_TYPE = "hive"

    @property
    def sync_tool_class_name(self) -> str:
        return self._merged_config.get("sync_tool_class", "org.apache.hudi.hive.HiveSyncTool")

    def get_streamer_hoodie_conf(self) -> Dict[str, str]:
        cfg = self._merged_config
        database = cfg.get("database", "default")
        table = cfg.get("table_name") or cfg.get("table", "")
        partition_fields = cfg.get("partition_fields", "date")
        partition_extractor = cfg.get(
            "partition_value_extractor",
            "org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor",
        )
        mode = cfg.get("mode", "hms")
        conf: Dict[str, str] = {
            "hoodie.datasource.hive_sync.database": database,
            "hoodie.datasource.hive_sync.table": table,
            "hoodie.datasource.hive_sync.partition_fields": partition_fields,
            "hoodie.datasource.hive_sync.partition_extractor_class": partition_extractor,
            "hoodie.datasource.hive_sync.mode": mode,
        }
        if mode == "hms":
            conf["hoodie.datasource.hive_sync.metastore.uris"] = cfg.get("metastore_uris", "thrift://localhost:9083")
        else:
            conf["hoodie.datasource.hive_sync.jdbcurl"] = cfg.get("jdbc_url", "jdbc:hive2://localhost:10000")
        if cfg.get("schema_string_length_thresh") is not None:
            conf["hoodie.datasource.hive_sync.schema_string_length_thresh"] = str(cfg["schema_string_length_thresh"])
        return conf

    def get_standalone_tool_args(self) -> List[str]:
        cfg = self._merged_config
        mode = cfg.get("mode", "jdbc")
        args: List[str] = [
            "--database", cfg.get("database", "default"),
            "--table", cfg.get("table_name") or cfg.get("table", ""),
            "--base-path", cfg.get("base_path", ""),
            "--partitioned-by", cfg.get("partition_fields", "date"),
            "--partition-value-extractor",
            cfg.get("partition_value_extractor", "org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor"),
            "--sync-mode", mode,
        ]
        if mode == "jdbc":
            args.extend(["--jdbc-url", cfg.get("jdbc_url", "jdbc:hive2://localhost:10000")])
        return args

    def validate_environment(self) -> List[ValidationResult]:
        base_path = (self._merged_config.get("base_path") or "").strip()
        if not base_path or base_path.startswith("${"):
            return []
        return [validate_table_path(base_path)]


class BigQuerySyncTool(AbstractSyncTool):
    SYNC_TYPE = "bigquery"

    @property
    def sync_tool_class_name(self) -> str:
        return self._merged_config.get("sync_tool_class", "org.apache.hudi.gcp.bigquery.BigQuerySyncTool")

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
            "hoodie.gcp.bigquery.sync.use_file_listing_from_metadata": str(cfg.get("use_file_listing_from_metadata", True)).lower(),
            "hoodie.gcp.bigquery.sync.assume_date_partitioning": str(cfg.get("assume_date_partitioning", False)).lower(),
            "hoodie.gcp.bigquery.sync.use_bq_manifest_file": str(cfg.get("use_bq_manifest_file", True)).lower(),
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
            cfg.get("partition_value_extractor", "org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor"),
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
        return self._merged_config.get(
            "packages",
            "com.google.cloud:google-cloud-bigquery:2.44.0,com.google.api-client:google-api-client:1.32.1,com.google.http-client:google-http-client-jackson2:1.39.2",
        )


class GlueSyncTool(AbstractSyncTool):
    SYNC_TYPE = "glue"

    @property
    def sync_tool_class_name(self) -> str:
        return self._merged_config.get("sync_tool_class", "org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool")

    def get_streamer_hoodie_conf(self) -> Dict[str, str]:
        cfg = self._merged_config
        database = cfg.get("database") or cfg.get("hive_sync_database", "hudi_db")
        table = cfg.get("table_name") or cfg.get("table") or cfg.get("hive_sync_table", "")
        return {
            "hoodie.datasource.meta_sync.condition.sync": str(cfg.get("meta_sync_condition_sync", True)).lower(),
            "hoodie.datasource.hive_sync.mode": cfg.get("sync_mode", "jdbc"),
            "hoodie.datasource.hive_sync.jdbcurl": cfg.get("jdbc_url", "jdbc:hive2://localhost:10000"),
            "hoodie.datasource.hive_sync.database": database,
            "hoodie.datasource.hive_sync.table": table,
            "hoodie.datasource.hive_sync.partition_fields": cfg.get("partition_fields", "date"),
            "hoodie.datasource.hive_sync.partition_extractor_class": cfg.get(
                "partition_value_extractor", "org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor"
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
            cfg.get("partition_value_extractor", "org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor"),
            "--sync-mode", cfg.get("sync_mode", "jdbc"),
            "--jdbc-url", cfg.get("jdbc_url", "jdbc:hive2://localhost:10000"),
        ]

    def validate_config(self) -> List[str]:
        errors = super().validate_config()
        if not self._merged_config.get("database") and not self._merged_config.get("hive_sync_database"):
            errors.append("glue: database or hive_sync_database is required")
        return errors

    def validate_environment(self) -> List[ValidationResult]:
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


class DataHubSyncTool(AbstractSyncTool):
    SYNC_TYPE = "datahub"

    @property
    def sync_tool_class_name(self) -> str:
        return self._merged_config.get("sync_tool_class", "org.apache.hudi.sync.datahub.DataHubSyncTool")

    def get_streamer_hoodie_conf(self) -> Dict[str, str]:
        cfg = self._merged_config
        conf: Dict[str, str] = {
            "hoodie.meta.sync.datahub.emitter.server": cfg.get("emitter_server", "http://localhost:8080"),
            "hoodie.datasource.hive_sync.database": cfg.get("database", "datahub_db"),
            "hoodie.datasource.hive_sync.table": cfg.get("table_name") or cfg.get("table", ""),
        }
        if cfg.get("schema_string_length_thresh") is not None:
            conf["hoodie.datasource.hive_sync.schema_string_length_thresh"] = str(cfg["schema_string_length_thresh"])
        if cfg.get("table_properties"):
            conf["hoodie.meta.sync.datahub.table.properties"] = cfg["table_properties"]
        if cfg.get("emit_log_metrics") is not None:
            conf["hoodie.meta.sync.datahub.emit.log.metrics"] = str(cfg["emit_log_metrics"]).lower()
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


SYNC_TYPE_REGISTRY = {
    "hive": HiveSyncTool,
    "bigquery": BigQuerySyncTool,
    "glue": GlueSyncTool,
    "datahub": DataHubSyncTool,
}
VALID_SYNC_TYPES = tuple(SYNC_TYPE_REGISTRY.keys())


def get_sync_tool(
    sync_type: str,
    config_path: Optional[str] = None,
    config: Optional[dict] = None,
    **overrides: Any,
) -> AbstractSyncTool:
    sync_type = sync_type.lower().strip()
    if sync_type not in SYNC_TYPE_REGISTRY:
        raise ValueError(f"Unknown sync_type: {sync_type}. Valid types: {list(SYNC_TYPE_REGISTRY)}")
    return SYNC_TYPE_REGISTRY[sync_type](config_path=config_path, config=config, **overrides)


# -----------------------------------------------------------------------------
# Streamer args and sync runner
# -----------------------------------------------------------------------------
def build_base_streamer_args(
    config: Optional[dict] = None,
    config_path: Optional[str] = None,
    base_path: Optional[str] = None,
    table_name: Optional[str] = None,
    data_path: Optional[str] = None,
) -> List[str]:
    if config is None:
        config = load_config(config_path)
    global_cfg = get_global_config(config=config)
    resolved_data_path = data_path or global_cfg.get("data_path") or "${DATA_PATH}"
    resolved_base_path = base_path or global_cfg.get("base_path") or ""
    resolved_table_name = table_name or global_cfg.get("table_name") or DEFAULT_TABLE_NAME
    return [
        "--target-base-path", resolved_base_path,
        "--target-table", resolved_table_name,
        "--table-type", global_cfg.get("table_type", "COPY_ON_WRITE"),
        "--base-file-format", global_cfg.get("base_file_format", "PARQUET"),
        "--props", f"{resolved_data_path.rstrip('/')}/hoodie.properties",
        "--source-class", "org.apache.hudi.utilities.sources.JsonDFSSource",
        "--source-ordering-field", global_cfg.get("precombine_field", "ts"),
        "--payload-class", "org.apache.hudi.common.model.DefaultHoodieRecordPayload",
        "--schemaprovider-class", "org.apache.hudi.utilities.schema.FilebasedSchemaProvider",
        "--hoodie-conf", f"hoodie.streamer.schemaprovider.source.schema.file={resolved_data_path.rstrip('/')}/schema.avsc",
        "--hoodie-conf", f"hoodie.streamer.schemaprovider.target.schema.file={resolved_data_path.rstrip('/')}/schema.avsc",
        "--hoodie-conf", f"hoodie.streamer.source.dfs.root={resolved_data_path.rstrip('/')}/input/",
        "--hoodie-conf", f"hoodie.datasource.write.recordkey.field={global_cfg.get('record_key_field', 'symbol')}",
        "--hoodie-conf", f"hoodie.datasource.write.partitionpath.field={global_cfg.get('partition_path_field', 'date')}",
        "--hoodie-conf", f"hoodie.datasource.write.precombine.field={global_cfg.get('precombine_field', 'ts')}",
        "--hoodie-conf", f"hoodie.datasource.write.keygenerator.type={global_cfg.get('keygenerator_type', 'SIMPLE')}",
        "--hoodie-conf", f"hoodie.datasource.write.hive_style_partitioning={str(global_cfg.get('hive_style_partitioning', True)).lower()}",
        "--hoodie-conf", f"hoodie.metadata.enable={str(global_cfg.get('metadata_enable', True)).lower()}",
        "--op", "UPSERT",
    ]


def hoodie_conf_to_cli_args(hoodie_conf: dict[str, str]) -> List[str]:
    result: List[str] = []
    for key, value in hoodie_conf.items():
        if value is None or value == "":
            continue
        result.append("--hoodie-conf")
        result.append(f"{key}={value}")
    return result


def build_streamer_sync_args(
    sync_type: str,
    config_path: Optional[str] = None,
    base_path: Optional[str] = None,
    table_name: Optional[str] = None,
    config: Optional[dict] = None,
) -> List[str]:
    tool = get_sync_tool(sync_type, config_path=config_path, config=config, base_path=base_path, table_name=table_name)
    errors = tool.validate_config()
    if errors:
        raise ValueError("Invalid config: " + "; ".join(errors))
    args: List[str] = ["--enable-sync", "--sync-tool-classes", tool.sync_tool_class_name]
    args.extend(hoodie_conf_to_cli_args(tool.get_streamer_hoodie_conf()))
    return args


def build_standalone_sync_args(
    sync_type: str,
    config_path: Optional[str] = None,
    base_path: Optional[str] = None,
    table_name: Optional[str] = None,
    config: Optional[dict] = None,
) -> List[str]:
    tool = get_sync_tool(sync_type, config_path=config_path, config=config, base_path=base_path, table_name=table_name)
    errors = tool.validate_config()
    if errors:
        raise ValueError("Invalid config: " + "; ".join(errors))
    return tool.get_standalone_tool_args()


# -----------------------------------------------------------------------------
# Command builder
# -----------------------------------------------------------------------------
def _jar_base(config: dict) -> tuple:
    g = get_global_config(config=config)
    jars_path = (g.get("jars_path") or "").rstrip("/")
    if not jars_path:
        return None, None, g.get("hudi_version", "0.16.0-SNAPSHOT"), g.get("spark_version", "3.5")
    spark_version = g.get("spark_version", "3.5")
    hudi_version = g.get("hudi_version", "0.16.0-SNAPSHOT")
    scala = g.get("scala_version", "2.12")
    base = f"{jars_path}/{hudi_version}/{spark_version}"
    return base, scala, hudi_version, spark_version


def _command_to_string(cmd: List[str]) -> str:
    segments: List[str] = []
    i = 0
    while i < len(cmd):
        arg = cmd[i]
        if arg.startswith("--") and i + 1 < len(cmd) and not cmd[i + 1].startswith("--"):
            segments.append(shlex.quote(arg) + " " + shlex.quote(cmd[i + 1]))
            i += 2
        else:
            segments.append(shlex.quote(arg))
            i += 1
    return " \\\n  ".join(segments)


class CommandBuilder:
    def __init__(
        self,
        config: Optional[dict] = None,
        config_path: Optional[str] = None,
        base_path: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> None:
        if config is None:
            config = load_config(config_path)
        self._config = config
        self._config_path = config_path
        global_cfg = get_global_config(config=config)
        self._base_path = base_path or global_cfg.get("base_path") or ""
        self._table_name = table_name or global_cfg.get("table_name") or DEFAULT_TABLE_NAME

    def _get_jar_args_and_utilities_jar(self, sync_type: str) -> tuple[List[str], str]:
        base, scala, hudi_version, spark_version = _jar_base(self._config)
        if base is None:
            if sync_type == "bigquery":
                return (["--jars", "${HUDI_JARS}", "--packages", "${PACKAGES}"], "${HUDI_UTILITIES_SLIM_JAR}")
            return ["--jars", "${HUDI_JARS}"], "${HUDI_UTILITIES_SLIM_JAR}"
        hudi_spark_jar = f"{base}/hudi-spark{spark_version}-bundle_{scala}-{hudi_version}.jar"
        utilities_slim = f"{base}/hudi-utilities-slim-bundle_{scala}-{hudi_version}.jar"
        if sync_type == "bigquery":
            gcp_jar = f"{base}/hudi-gcp-bundle-{hudi_version}.jar"
            jars = f"{gcp_jar},{hudi_spark_jar}"
            tool = get_sync_tool(sync_type, config=self._config)
            packages = getattr(tool, "spark_packages", "") if hasattr(tool, "spark_packages") else ""
            args: List[str] = ["--jars", jars]
            if packages:
                args.extend(["--packages", packages])
            return args, utilities_slim
        if sync_type == "glue":
            aws_jar = f"{base}/hudi-aws-bundle-{hudi_version}.jar"
            jars = f"$S3_JARS,{hudi_spark_jar},{aws_jar}"
            return ["--jars", jars], utilities_slim
        if sync_type == "datahub":
            datahub_jar = f"{base}/hudi-datahub-sync-bundle-{hudi_version}.jar"
            return ["--jars", f"{hudi_spark_jar},{datahub_jar}"], utilities_slim
        return ["--jars", hudi_spark_jar], utilities_slim

    def _get_standalone_main_jar(self, sync_type: str) -> str:
        base, _, hudi_version, _ = _jar_base(self._config)
        if base is None:
            if sync_type == "bigquery":
                return "${HUDI_GCP_BUNDLE_JAR}"
            if sync_type == "glue":
                return "${HUDI_AWS_JAR}"
            return "${HUDI_SYNC_JAR}"
        if sync_type == "bigquery":
            return f"{base}/hudi-gcp-bundle-{hudi_version}.jar"
        if sync_type == "glue":
            return f"{base}/hudi-aws-bundle-{hudi_version}.jar"
        return "${HUDI_SYNC_JAR}"

    def build_inline_command(self, sync_type: str) -> List[str]:
        jar_args, utilities_jar = self._get_jar_args_and_utilities_jar(sync_type)
        return [
            "spark-submit", "--master", "yarn",
            *jar_args,
            "--conf", f"spark.driver.extraClassPath={utilities_jar}",
            "--conf", f"spark.executor.extraClassPath={utilities_jar}",
            "--class", "org.apache.hudi.utilities.streamer.HoodieStreamer",
            utilities_jar,
            *build_base_streamer_args(config=self._config, base_path=self._base_path or None, table_name=self._table_name or None),
            *build_streamer_sync_args(sync_type, config_path=self._config_path, config=self._config, base_path=self._base_path or None, table_name=self._table_name or None),
        ]

    def build_ingestion_only_command(self, sync_type: str) -> List[str]:
        jar_args, utilities_jar = self._get_jar_args_and_utilities_jar(sync_type)
        return [
            "spark-submit", "--master", "yarn",
            *jar_args,
            "--class", "org.apache.hudi.utilities.streamer.HoodieStreamer",
            utilities_jar,
            *build_base_streamer_args(config=self._config, base_path=self._base_path or None, table_name=self._table_name or None),
        ]

    def build_standalone_sync_command(self, sync_type: str) -> List[str]:
        tool = get_sync_tool(sync_type, config_path=self._config_path, config=self._config, base_path=self._base_path, table_name=self._table_name)
        jar_args, _ = self._get_jar_args_and_utilities_jar(sync_type)
        main_jar = self._get_standalone_main_jar(sync_type)
        return [
            "spark-submit", "--master", "yarn",
            *jar_args,
            "--class", tool.sync_tool_class_name,
            main_jar,
            *build_standalone_sync_args(sync_type, config_path=self._config_path, config=self._config, base_path=self._base_path or None, table_name=self._table_name or None),
        ]

    def build_datasource_snippet(self, sync_type: str) -> str:
        tool = get_sync_tool(sync_type, config_path=self._config_path, config=self._config, base_path=self._base_path, table_name=self._table_name)
        global_cfg = get_global_config(config=self._config)
        base_path = self._base_path or tool.config.get("base_path") or ""
        table_name = self._table_name or tool.config.get("table_name") or DEFAULT_TABLE_NAME
        opts = tool.get_datasource_hoodie_options()
        opts["hoodie.table.name"] = table_name
        opts["hoodie.datasource.write.recordkey.field"] = global_cfg.get("record_key_field", "symbol")
        opts["hoodie.datasource.write.precombine.field"] = global_cfg.get("precombine_field", "ts")
        opts["hoodie.datasource.write.partitionpath.field"] = global_cfg.get("partition_path_field", "date")
        opts["hoodie.datasource.write.hive_style_partitioning"] = str(global_cfg.get("hive_style_partitioning", True)).lower()
        option_lines = "\n    ".join(f'option("{k}", "{v}").\\' for k, v in opts.items() if v)
        return f"""# Datasource mode: Spark write with {sync_type} meta sync
# Run with: pyspark --jars $HUDI_JARS --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension ...

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col

spark = SparkSession.builder.getOrCreate()

input_path = "{base_path.rstrip("/")}/input/"  # or your JSON input path
base_path = "{base_path}"
table_name = "{table_name}"

df = spark.read.json(input_path)
df = df.withColumn("ts", to_timestamp(col("ts"), "yyyy-MM-dd HH:mm:ss"))

df.write.format("hudi").\\
    {option_lines}
    mode("overwrite").\\
    save(base_path)
"""

    @staticmethod
    def command_to_string(cmd: List[str]) -> str:
        return _command_to_string(cmd)


# -----------------------------------------------------------------------------
# Main CLI
# -----------------------------------------------------------------------------
def run_validation(sync_type: str, config: dict, base_path: Optional[str], table_name: Optional[str]) -> int:
    tool = get_sync_tool(sync_type, config=config, base_path=base_path, table_name=table_name)
    results = tool.validate_environment()
    if not results:
        print("No validation checks configured (e.g. base_path not set).")
        return 0
    all_pass = True
    for r in results:
        print(r)
        if not r.success:
            all_pass = False
    return 0 if all_pass else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Hudi catalog sync tests by sync type and mode (inline, separate, datasource).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--sync-type", required=True, choices=list(SYNC_TYPE_REGISTRY), help="Sync target: hive, bigquery, glue, datahub")
    parser.add_argument("--mode", required=True, choices=MODES, help="Test mode: inline, separate, datasource, validate")
    parser.add_argument("--config", default=None, help="Path to config.yaml (default: project config.yaml)")
    parser.add_argument("--base-path", default=None, help="Override base path for table")
    parser.add_argument("--table-name", default=None, help="Override table name")
    parser.add_argument("--run", action="store_true", help="Execute the command(s) with subprocess (default: print only)")
    parser.add_argument("--dry-run", action="store_true", default=True, help="Print command(s) only (default). Use --run to execute.")
    parser.add_argument("--validate", action="store_true", help="After running, run environment validation.")
    args = parser.parse_args()
    if args.run:
        args.dry_run = False

    setup_logging()
    logger = get_logger(__name__)
    logger.info("Starting sync_type=%s mode=%s config=%s", args.sync_type, args.mode, args.config or "(default)")

    try:
        config = load_config(args.config)
    except FileNotFoundError as e:
        logger.error("Config file not found: %s", e)
        return 1

    global_cfg = get_global_config(config=config)
    base_path: Optional[str] = args.base_path or global_cfg.get("base_path") or ""
    table_name: Optional[str] = args.table_name or global_cfg.get("table_name") or DEFAULT_TABLE_NAME
    if not base_path:
        base_path = "${TABLE_BASE_PATH}"
        logger.debug("base_path not set; using placeholder %s", base_path)

    builder = CommandBuilder(config=config, config_path=args.config, base_path=base_path, table_name=table_name)

    try:
        if args.mode == "validate":
            print("# Validation: environment checks for sync_type=%s\n" % args.sync_type)
            return run_validation(args.sync_type, config, base_path, table_name)

        if args.mode == "inline":
            cmd = builder.build_inline_command(args.sync_type)
            print("# Inline: HoodieStreamer with sync enabled\n")
            print(CommandBuilder.command_to_string(cmd))
            if not args.dry_run:
                logger.info("Executing inline command (spark-submit)")
                code = subprocess.run(cmd).returncode
                if code != 0:
                    return code
            if args.validate:
                print("\n# Validation\n")
                return run_validation(args.sync_type, config, base_path, table_name)
            return 0

        if args.mode == "separate":
            cmd1 = builder.build_ingestion_only_command(args.sync_type)
            cmd2 = builder.build_standalone_sync_command(args.sync_type)
            print("# Step 1: HoodieStreamer (no sync)\n")
            print(CommandBuilder.command_to_string(cmd1))
            print("\n# Step 2: Standalone sync\n")
            print(CommandBuilder.command_to_string(cmd2))
            if not args.dry_run:
                logger.info("Executing step 1: ingestion")
                r1 = subprocess.run(cmd1).returncode
                if r1 != 0:
                    logger.error("Step 1 failed with exit code %s", r1)
                    return r1
                logger.info("Executing step 2: standalone sync")
                code = subprocess.run(cmd2).returncode
                if code != 0:
                    return code
            if args.validate:
                print("\n# Validation\n")
                return run_validation(args.sync_type, config, base_path, table_name)
            return 0

        # datasource
        snippet = builder.build_datasource_snippet(args.sync_type)
        print("# Datasource mode: run in pyspark/spark-shell with Hudi JARs and HoodieSparkSessionExtension\n")
        print(snippet)
        if not args.dry_run:
            print("# --run is not supported for datasource mode; run the snippet manually in pyspark.")
        if args.validate:
            print("\n# Validation\n")
            return run_validation(args.sync_type, config, base_path, table_name)
        return 0

    except (ValueError, KeyError) as e:
        logger.exception("Configuration or build error: %s", e)
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
