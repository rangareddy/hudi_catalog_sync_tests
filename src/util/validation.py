"""
Environment-specific validation after sync: database/dataset exists, table exists, table path exists.
Uses CLI tools (bq, aws, gcloud/gsutil, curl) so no extra Python deps; runs in the environment where sync ran.
"""

import json
import os
import subprocess
from dataclasses import dataclass
from typing import List

from .logging_config import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class ValidationResult:
    """Result of a single validation check."""
    check_name: str
    success: bool
    message: str

    def __str__(self) -> str:
        status = "PASS" if self.success else "FAIL"
        return f"[{status}] {self.check_name}: {self.message}"


def _run_cmd(cmd: List[str], timeout_seconds: int = 30) -> tuple[bool, str]:
    """Run command; return (success, stderr_or_stdout on failure)."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
        if result.returncode == 0:
            return True, ""
        return False, (result.stderr or result.stdout or f"exit code {result.returncode}").strip()
    except FileNotFoundError:
        return False, f"Command not found: {cmd[0]}"
    except subprocess.TimeoutExpired:
        return False, "Command timed out"


def validate_gcs_path(path: str) -> ValidationResult:
    """Check that a GCS path (gs://...) exists. Uses gcloud storage ls."""
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
    """Check that the BigQuery dataset exists. Uses bq show."""
    if not project_id or not dataset_name:
        return ValidationResult("bigquery_dataset", False, "project_id and dataset_name required")
    ok, err = _run_cmd(["bq", "show", "--dataset", f"{project_id}:{dataset_name}"])
    if ok:
        return ValidationResult("bigquery_dataset", True, f"Dataset {project_id}:{dataset_name} exists")
    return ValidationResult("bigquery_dataset", False, err or "Dataset not found")


def validate_bigquery_table(project_id: str, dataset_name: str, table_name: str) -> ValidationResult:
    """Check that the BigQuery table exists. Uses bq show."""
    if not project_id or not dataset_name or not table_name:
        return ValidationResult("bigquery_table", False, "project_id, dataset_name, table_name required")
    ok, err = _run_cmd(["bq", "show", f"{project_id}:{dataset_name}.{table_name}"])
    if ok:
        return ValidationResult("bigquery_table", True, f"Table {dataset_name}.{table_name} exists")
    return ValidationResult("bigquery_table", False, err or "Table not found")


def validate_s3_path(path: str) -> ValidationResult:
    """Check that an S3 path (s3:// or s3a://) exists. Uses aws s3 ls."""
    s3_path = path.replace("s3a://", "s3://", 1).rstrip("/") + "/"
    if not s3_path.startswith("s3://"):
        return ValidationResult("s3_path", False, f"Not an S3 path: {path}")
    ok, err = _run_cmd(["aws", "s3", "ls", s3_path])
    if ok:
        return ValidationResult("s3_path", True, f"Path exists: {s3_path}")
    return ValidationResult("s3_path", False, err or f"Path not accessible: {s3_path}")


def validate_glue_database(database_name: str) -> ValidationResult:
    """Check that the Glue database exists. Uses aws glue get-database."""
    if not database_name:
        return ValidationResult("glue_database", False, "database name required")
    ok, err = _run_cmd(["aws", "glue", "get-database", "--name", database_name])
    if ok:
        return ValidationResult("glue_database", True, f"Database {database_name} exists")
    return ValidationResult("glue_database", False, err or "Database not found")


def validate_glue_table(database_name: str, table_name: str) -> ValidationResult:
    """Check that the Glue table exists. Uses aws glue get-table."""
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
    """Check that a local directory path exists."""
    if not path:
        return ValidationResult("local_path", False, "path is empty")
    if path.startswith("gs://") or path.startswith("s3://") or path.startswith("s3a://"):
        return ValidationResult("local_path", False, f"Not a local path: {path}")
    if os.path.isdir(path):
        return ValidationResult("local_path", True, f"Path exists: {path}")
    return ValidationResult("local_path", False, f"Path not found or not a directory: {path}")


def validate_datahub_dataset(
    emitter_server: str, database_name: str, table_name: str
) -> ValidationResult:
    """Check that the dataset/table entity exists in DataHub (search API). Uses curl."""
    if not emitter_server or not table_name:
        return ValidationResult("datahub_dataset", False, "emitter_server and table_name required")
    # DataHub search API: POST .../entities?action=search
    search_url = f"{emitter_server.rstrip('/')}/entities?action=search"
    payload = {
        "entity": "dataset",
        "start": 0,
        "count": 10,
        "input": f"{database_name}.{table_name}",
    }
    try:
        result = subprocess.run(
            [
                "curl", "-s", "-X", "POST", search_url,
                "-H", "Content-Type: application/json",
                "-d", json.dumps(payload),
            ],
            capture_output=True,
            text=True,
            timeout=15,
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
    """
    Check that the table base path exists. Dispatches to GCS, S3, or local based on prefix.
    """
    if not base_path or base_path.startswith("${"):
        return ValidationResult("table_path", False, "base_path not set or placeholder")
    if base_path.startswith("gs://"):
        return validate_gcs_path(base_path)
    if base_path.startswith("s3://") or base_path.startswith("s3a://"):
        return validate_s3_path(base_path)
    return validate_local_path(base_path)
