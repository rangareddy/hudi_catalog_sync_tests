# Hudi Catalog Sync Tests

A Python framework to build and run Apache Hudi catalog sync tests for **Hive**, **BigQuery**, **Glue**, and **DataHub**. It generates and runs `spark-submit` commands (inline, separate, or datasource mode), runs environment validation, and writes all output to log files under `logs/`.

## Project layout

```
hudi_catalog_sync_tests/
├── README.md
├── config.yaml                    # Config for all sync types (global, hive, bigquery, glue, datahub)
├── hudi_catalog_sync_runner.py   # Main CLI: build/run sync commands, validation
├── validate_database.py          # PySpark script for Hive catalog table validation (run via spark-submit)
└── logs/                         # Log output from runs (created automatically)
    ├── {table_name}.log          # Spark-submit output for inline/separate/datasource runs
    └── validate_database_{sync_type}_{table_name}.log   # Hive validation output
```

Table names and base paths are derived from config:  
`table_name = {base_table_name}_{sync_type}_{mode}_{hudi_version_str}`  
`base_path = {base_table_path}/{table_name}`  
(e.g. `stock_ticks_hive_inline_0_16_0`).

## Requirements

- Python 3.9+
- **PyYAML**: `pip install pyyaml`
- **Spark**: Set `spark_home` in `config.yaml` or `SPARK_HOME` when running commands.

## Quick start

1. Install dependency: `pip install pyyaml`
2. Edit `config.yaml` (paths, project/dataset, metastore, etc.).
3. Run the runner:

```bash
# Print spark-submit command (inline: HoodieStreamer with sync)
python hudi_catalog_sync_runner.py --sync-type bigquery --mode inline

# Run the command and write output to logs/{table_name}.log
python hudi_catalog_sync_runner.py --sync-type hive --mode inline --run

# Run environment validation only
python hudi_catalog_sync_runner.py --sync-type bigquery --mode validate

# Custom config path
python hudi_catalog_sync_runner.py --sync-type glue --mode inline --config /path/to/config.yaml
```

## Modes

| Mode        | Description |
|------------|-------------|
| **inline** | One `spark-submit` for HoodieStreamer with sync enabled (ingestion + catalog sync in one job). Output in `logs/{table_name}.log`. |
| **separate** | Step 1: HoodieStreamer without sync; Step 2: standalone SyncTool. Both steps log to `logs/{table_name}.log`. |
| **datasource** | Spark DataSource write with catalog sync: generates and runs a PySpark script. Output in `logs/{table_name}.log`. |
| **validate** | Run environment checks only (dataset/database exists, table exists, table path exists). For Hive, uses `validate_database.py` and logs to `logs/validate_database_{sync_type}_{table_name}.log`. |

Use **--validate** with any mode to run validation after the command(s). Use **--run** to execute the command(s) (inline/separate/datasource); default is dry-run (print only).

## CLI options

| Option | Description |
|--------|-------------|
| `--sync-type` | **Required.** One of: `hive`, `bigquery`, `glue`, `datahub`. |
| `--mode` | **Required.** One of: `inline`, `separate`, `datasource`, `validate`. |
| `--config` | Path to `config.yaml` (default: `config.yaml` in script directory). |
| `--run` | Execute the command(s) with subprocess; spark-submit output goes to `logs/{table_name}.log`. |
| `--dry-run` | Print command(s) only (default). Use `--run` to execute. |
| `--validate` | After running (or with dry-run), run environment validation. |

Base path and table name are derived from `config.yaml`: `base_table_path`, `base_table_name`, plus sync type, mode, and Hudi version string.

## Examples

```bash
# Inline + validation (dry-run)
python hudi_catalog_sync_runner.py --sync-type bigquery --mode inline --validate

# Separate mode: print both commands
python hudi_catalog_sync_runner.py --sync-type glue --mode separate

# Run inline and write output to logs/stock_ticks_hive_inline_0_16_0.log
python hudi_catalog_sync_runner.py --sync-type hive --mode inline --run

# Run then validate
python hudi_catalog_sync_runner.py --sync-type hive --mode inline --run --validate

# Validate only (BigQuery: bq/gcloud; Glue: aws; Hive: validate_database.py + path checks; DataHub: API + path)
python hudi_catalog_sync_runner.py --sync-type bigquery --mode validate
python hudi_catalog_sync_runner.py --sync-type hive --mode validate
```

## Configuration

The script loads `config.yaml` from the script directory unless you pass `--config`. The file must have a **global** section and one section per sync type: **hive**, **bigquery**, **glue**, **datahub**. Sync sections inherit from global; empty `table_name`, `table`, or `base_path` are filled from global. The runner overwrites base path and table name per run using `base_table_path`, `base_table_name`, sync type, mode, and Hudi version.

### Global config (main keys)

```yaml
global:
  spark_home: ''                    # Optional; else set SPARK_HOME
  spark_master: "local[4]"
  spark_version: "3.4.4"
  scala_version: "2.12"
  hudi_version: "0.16.0-SNAPSHOT"
  base_table_name: "stock_ticks"
  base_table_path: "/tmp/hudi_catalog_sync/tables"
  base_data_path: "/tmp/hudi_catalog_sync/data"
  jars_path: "/tmp/hudi_catalog_sync/jars"
  table_type: "COPY_ON_WRITE"
  base_file_format: "PARQUET"
  database_name: "default"
  partition_fields: "date"
  record_key_field: "symbol"
  precombine_field: "ts"
  partition_path_field: "date"
  keygenerator_type: "SIMPLE"
  hive_style_partitioning: true
  metadata_enable: true
  extraclasspath_enabled: false
  mode: "hms"
  metastore_uris: "thrift://localhost:9083"
```

### Sync-type sections

Each sync type has its own block (e.g. `hive:`, `bigquery:`, `glue:`, `datahub:`) with `enabled`, `sync_tool_class`, and type-specific options (e.g. `database`, `project_id`, `dataset_name`, `emitter_server`). See `config.yaml` in the project for the full structure.

## Logging

- **Runner**: Uses Python `logging`; level and format can be adjusted in the script.
- **Spark-submit**: When using `--run`, all spark-submit stdout/stderr are written to `logs/{table_name}.log` (inline, separate, and datasource). For Hive validation, `validate_database.py` output is written to `logs/validate_database_{sync_type}_{table_name}.log`.

## Post-sync validation

Validation uses CLI tools and (for Hive) a PySpark script on the machine where you run the runner:

| Sync type | Checks | Tools / Script |
|-----------|--------|-----------------|
| **Hive** | Table path exists, table exists in Hive | `validate_database.py` (spark-submit with env: `val_database_name`, `val_table_name`, `val_hive_thrift_uri`) |
| **BigQuery** | Dataset exists, table exists, GCS path exists | `bq`, `gcloud storage` |
| **Glue** | Database exists, table exists, S3 path exists | `aws glue`, `aws s3` |
| **DataHub** | Dataset entity in DataHub, table path exists | `datahub` CLI / API, path check |

## Sync tool classes (reference)

| Sync type | Java class |
|-----------|------------|
| Hive | `org.apache.hudi.hive.HiveSyncTool` |
| BigQuery | `org.apache.hudi.gcp.bigquery.BigQuerySyncTool` |
| Glue | `org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool` |
| DataHub | `org.apache.hudi.sync.datahub.DataHubSyncTool` |

## License

See the script headers and Apache Hudi documentation for license and attribution.
