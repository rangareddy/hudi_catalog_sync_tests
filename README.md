# Hudi Catalog Sync Tests

A single-file Python script to build and run Apache Hudi catalog sync tests for **Hive**, **BigQuery**, **Glue**, and **DataHub**. It generates `spark-submit` commands (inline or separate mode), PySpark datasource snippets, and runs environment validation.

## Project layout

```
hudi_catalog_sync_tests/
├── README.md
├── hudi_catalog_sync.py   # Single-file implementation (CLI + config + sync tools + validation)
└── config.yaml            # Config for all sync types (minimal example included)
```

## Requirements

- Python 3.9+
- **PyYAML**: `pip install pyyaml`

## Quick start

1. Install dependency: `pip install pyyaml`
2. Ensure `config.yaml` is in the same directory as `hudi_catalog_sync.py` (a minimal example is included; edit for your environment).
3. Run the script:

```bash
# Print spark-submit command (inline: HoodieStreamer with sync)
python hudi_catalog_sync.py --sync-type bigquery --mode inline

# Run environment validation only
python hudi_catalog_sync.py --sync-type bigquery --mode validate

# Custom config path
python hudi_catalog_sync.py --sync-type glue --mode inline --config /path/to/config.yaml
```

## Modes

| Mode        | Description |
|------------|-------------|
| **inline** | One `spark-submit` for HoodieStreamer with `--enable-sync` and the chosen sync tool. |
| **separate** | Step 1: HoodieStreamer without sync; Step 2: standalone SyncTool. |
| **datasource** | PySpark snippet for `df.write.format("hudi")` with meta sync; run manually in pyspark/spark-shell. |
| **validate** | Run environment checks only (dataset/database exists, table exists, table path exists). |

Use **--validate** with any mode to run validation after the command(s). Use **--run** to execute the command(s) (inline/separate only; datasource prints a snippet to run manually).

## CLI options

| Option | Description |
|--------|-------------|
| `--sync-type` | **Required.** One of: `hive`, `bigquery`, `glue`, `datahub`. |
| `--mode` | **Required.** One of: `inline`, `separate`, `datasource`, `validate`. |
| `--config` | Path to `config.yaml` (default: `config.yaml` next to the script). |
| `--base-path` | Override table base path. |
| `--table-name` | Override table name. |
| `--run` | Execute the command(s) with subprocess (default is dry-run: print only). |
| `--dry-run` | Print command(s) only (default). |
| `--validate` | After running (or with dry-run), run environment validation. |

## Examples

```bash
# Inline + validation (dry-run)
python hudi_catalog_sync.py --sync-type bigquery --mode inline --validate

# Separate mode: print both commands
python hudi_catalog_sync.py --sync-type glue --mode separate

# Datasource: print PySpark snippet
python hudi_catalog_sync.py --sync-type datahub --mode datasource --base-path gs://bucket/path --table-name my_table

# Validate only (BigQuery: bq/gcloud; Glue: aws; Hive/DataHub: path checks)
python hudi_catalog_sync.py --sync-type bigquery --mode validate
python hudi_catalog_sync.py --sync-type glue --mode validate

# Execute inline command
python hudi_catalog_sync.py --sync-type hive --mode inline --run
```

## Configuration

The script looks for `config.yaml` in the same directory as `hudi_catalog_sync.py` unless you pass `--config`. The file must have a **global** section and one section per sync type: **hive**, **bigquery**, **glue**, **datahub**. Empty `table_name`, `table`, or `base_path` in a sync section are filled from `global`.

### Minimal `config.yaml` example

```yaml
global:
  spark_version: "3.5"
  scala_version: "2.12"
  hudi_version: "0.16.0-SNAPSHOT"
  table_name: stocks_sync_test
  base_path: "${TABLE_BASE_PATH}"
  data_path: "${DATA_PATH}"
  partition_fields: date
  record_key_field: symbol
  precombine_field: ts
  partition_path_field: date
  keygenerator_type: SIMPLE
  hive_style_partitioning: true
  metadata_enable: true

hive:
  enabled: true
  database: default
  mode: hms
  metastore_uris: thrift://localhost:9083

bigquery:
  enabled: true
  project_id: my-project
  dataset_name: my_dataset
  dataset_location: us-central1

glue:
  enabled: true
  database: hudi_db

datahub:
  enabled: true
  emitter_server: http://localhost:8080
  database: datahub_db
```

Set `base_path` (and optionally `jars_path`, `data_path`) when running on a cluster; the script uses `${TABLE_BASE_PATH}`, `${DATA_PATH}`, `${HUDI_JARS}`, `${HUDI_UTILITIES_SLIM_JAR}` etc. in generated commands when not overridden.

## Logging

- **LOG_LEVEL**: `DEBUG`, `INFO`, `WARNING`, `ERROR` (default: `INFO`).
- **LOG_FORMAT**: Custom format string (default: `%(asctime)s | %(levelname)-8s | %(name)s | %(message)s`).

## Post-sync validation

Validation uses CLI tools on the machine where you run the script; ensure the right tools and credentials are available:

| Sync type | Checks | Tools |
|-----------|--------|--------|
| **BigQuery** | Dataset exists, table exists, GCS path exists | `bq`, `gcloud storage` |
| **Glue** | Database exists, table exists, S3 path exists | `aws glue`, `aws s3` |
| **Hive** | Table path exists (local, GCS, or S3) | `gcloud` / `aws` / local filesystem |
| **DataHub** | Dataset entity in DataHub, table path exists | `curl` (DataHub search API), path check |

## Sync tool classes (reference)

| Sync type | Java class |
|-----------|------------|
| Hive | `org.apache.hudi.hive.HiveSyncTool` |
| BigQuery | `org.apache.hudi.gcp.bigquery.BigQuerySyncTool` |
| Glue | `org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool` |
| DataHub | `org.apache.hudi.sync.datahub.DataHubSyncTool` |

## Programmatic use

You can import from `hudi_catalog_sync` when the script is on `PYTHONPATH`:

```python
from hudi_catalog_sync import (
    load_config,
    get_global_config,
    get_sync_config,
    get_sync_tool,
    CommandBuilder,
    ValidationResult,
    SYNC_TYPE_REGISTRY,
)

config = load_config()
tool = get_sync_tool("glue", config=config, base_path="s3a://b/p", table_name="t")
print(tool.sync_tool_class_name)
print(tool.get_streamer_hoodie_conf())

builder = CommandBuilder(config=config, base_path="s3a://b/p", table_name="t")
cmd = builder.build_inline_command("glue")
print(CommandBuilder.command_to_string(cmd))
```

## License

See the script header and Apache Hudi documentation for license and attribution.
