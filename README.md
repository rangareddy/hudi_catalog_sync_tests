# Hudi Catalog Sync Tests

Production-ready Python helpers and configuration for testing Apache Hudi catalog syncs: **Hive**, **BigQuery**, **Glue**, and **DataHub**. Uses abstract base classes, clear naming, structured logging, and pytest tests.

## Project layout

```
hudi_catalog_sync_tests/
├── config.yaml           # Configuration for all sync types
├── requirements.txt      # PyYAML, pytest
├── README.md
├── src/
│   ├── __init__.py       # Public API (sync tools, config, logging)
│   ├── __main__.py       # CLI: --sync-type, --mode (inline | separate | datasource | validate)
│   ├── validation.py    # Post-sync validation: dataset/DB, table, path (bq, aws, gcloud, curl)
│   ├── logging_config.py # Production logging (LOG_LEVEL, LOG_FORMAT env)
│   ├── config_loader.py  # Load config.yaml, get_global_config, get_sync_config
│   ├── streamer_args.py  # Base HoodieStreamer args (build_base_streamer_args)
│   ├── sync_runner.py    # build_streamer_sync_args, build_standalone_sync_args
│   ├── command_builder.py # CommandBuilder: inline / separate / datasource commands
│   └── sync/             # Sync type implementations
│       ├── __init__.py   # get_sync_tool, SYNC_TYPE_REGISTRY
│       ├── base.py       # AbstractSyncTool (abstract base)
│       ├── hive.py       # HiveSyncTool
│       ├── bigquery.py   # BigQuerySyncTool
│       ├── glue.py       # GlueSyncTool
│       └── datahub.py    # DataHubSyncTool
└── tests/
    ├── conftest.py       # Fixtures (sample_global_config, sample_full_config)
    ├── test_config_loader.py
    ├── test_sync_base.py  # AbstractSyncTool and all four implementations
    ├── test_streamer_args.py
    ├── test_sync_runner.py
    ├── test_command_builder.py
    └── test_main.py
```

## Logging

Logging is configured on first use (e.g. when running `python -m src`). You can control it via environment or code:

- **LOG_LEVEL**: `DEBUG`, `INFO`, `WARNING`, `ERROR` (default: `INFO`)
- **LOG_FORMAT**: Custom format string (default: `%(asctime)s | %(levelname)-8s | %(name)s | %(message)s`)

```python
from src.logging_config import setup_logging, get_logger

setup_logging(level="DEBUG")  # optional, called automatically in __main__
logger = get_logger(__name__)
logger.info("Message")
```

## Configuration (`config.yaml`)

- **global**: Spark/Hudi versions, table name, base path, data path, partition fields, etc.
- **hive**: Hive metastore URI or JDBC URL, database, table, partition fields.
- **bigquery**: GCP project ID, dataset name/location, source URIs, manifest options.
- **glue**: Glue database, table, base path, JDBC URL, partition extractor.
- **datahub**: DataHub emitter server URL, database, table.

Empty `table_name` / `table` / `base_path` in a section are filled from `global`.

## Main entrypoint (modes: inline, separate, datasource, validate)

```bash
# Print spark-submit command (inline: HoodieStreamer with sync)
python -m src --sync-type bigquery --mode inline

# Print two commands (separate: ingestion then standalone sync)
python -m src --sync-type glue --mode separate

# Print PySpark snippet (datasource: DataFrame write with meta sync)
python -m src --sync-type bigquery --mode datasource

# Run environment validation only (dataset/database exists, table exists, path exists)
python -m src --sync-type bigquery --mode validate
python -m src --sync-type glue --mode validate

# After running sync, run validation (or with dry-run to check current state)
python -m src --sync-type bigquery --mode inline --run --validate
python -m src --sync-type glue --mode inline --validate

# Overrides and custom config
python -m src --sync-type glue --mode inline --base-path s3a://bucket/path --table-name my_table
python -m src --sync-type datahub --mode inline --config /path/to/config.yaml

# Execute commands (inline/separate only)
python -m src --sync-type bigquery --mode inline --run
```

- **inline**: One `spark-submit` for HoodieStreamer with `--enable-sync` and the chosen sync tool.
- **separate**: Step 1 = HoodieStreamer without sync; Step 2 = standalone SyncTool.
- **datasource**: PySpark snippet for `df.write.format("hudi")` with meta sync; run manually in pyspark.
- **validate**: Run environment-specific checks (BigQuery: dataset + table + GCS path; Glue: database + table + S3 path; Hive/DataHub: path + optional DataHub entity).

Use **--validate** with any mode to run validation after the command(s); with dry-run it checks the current environment.

If `base_path` is not set, generated commands use `${TABLE_BASE_PATH}` and `${DATA_PATH}`; set these (and `${HUDI_JARS}`, `${HUDI_UTILITIES_SLIM_JAR}` etc.) when running on a cluster.

## Post-sync validation (by environment)

After running syncs, you can verify that the catalog and storage are correct:

| Sync type | Checks | Tools used |
|-----------|--------|------------|
| **BigQuery** (GCP) | Dataset exists, table exists, GCS path exists | `bq`, `gcloud storage` |
| **Glue** (AWS) | Database exists, table exists, S3 path exists | `aws glue`, `aws s3` |
| **Hive** | Table path exists (local, GCS, or S3) | `gcloud` / `aws` / `os.path` |
| **DataHub** | Dataset entity in DataHub, table path exists | `curl` (DataHub search API), path check |

Validation runs on the machine where you execute the CLI (ensure `bq`/`gcloud`, `aws`, or `curl` are installed and authenticated for the target environment). Use `--mode validate` to run only validation, or `--validate` after inline/separate to validate after sync.

## Usage

### 1. Config

```python
from src.config_loader import load_config, get_sync_config, get_global_config

config = load_config()  # or load_config("/path/to/config.yaml")
global_cfg = get_global_config(config=config)
hive_cfg = get_sync_config("hive", config=config)
```

### 2. Sync tool classes (abstract base + implementations)

All sync types implement `AbstractSyncTool` with:

- `sync_tool_class_name`: Java class for `--sync-tool-classes`
- `get_streamer_hoodie_conf()`: hoodie-conf dict for HoodieStreamer
- `get_standalone_tool_args()`: CLI args for standalone SyncTool
- `get_datasource_hoodie_options()`: options for `df.write.format("hudi")`
- `validate_config()`: list of error messages (empty if valid)

```python
from src import get_sync_tool, HiveSyncTool, BigQuerySyncTool, GlueSyncTool, DataHubSyncTool

tool = get_sync_tool("glue", config=config, base_path="s3a://b/p", table_name="t")
print(tool.sync_tool_class_name)
print(tool.get_streamer_hoodie_conf())
print(tool.get_standalone_tool_args())
print(tool.get_datasource_hoodie_options())
errors = tool.validate_config()
```

### 3. Command building

```python
from src.sync_runner import build_streamer_sync_args, build_standalone_sync_args
from src.command_builder import CommandBuilder, build_inline_command

# Just the sync-related args for HoodieStreamer
args = build_streamer_sync_args("glue", config=config, base_path="s3a://b/p", table_name="t")

# Full command building
builder = CommandBuilder(config=config, base_path="s3a://b/p", table_name="t")
cmd = builder.build_inline_command("glue")
print(CommandBuilder.command_to_string(cmd))
```

## Sync tool classes (reference)

| Sync type  | Class |
|------------|------|
| Hive       | `org.apache.hudi.hive.HiveSyncTool` |
| BigQuery   | `org.apache.hudi.gcp.bigquery.BigQuerySyncTool` |
| Glue       | `org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool` |
| DataHub    | `org.apache.hudi.sync.datahub.DataHubSyncTool` |

## Install and test

```bash
cd hudi_catalog_sync_tests
pip install -r requirements.txt
python -m pytest tests/ -v
```

Run with the project root as working directory so `config.yaml` is found, or pass `--config` / `config_path` explicitly.
