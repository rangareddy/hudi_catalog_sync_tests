"""
Main entrypoint: run Hudi catalog sync tests by sync type and mode.

Usage:
  python -m src --sync-type bigquery --mode inline
  python -m src --sync-type glue --mode separate --config config.yaml
  python -m src --sync-type bigquery --mode datasource --base-path gs://bucket/path --table-name my_table

Modes:
  inline    - HoodieStreamer with sync enabled (ingestion + sync in one job)
  separate  - HoodieStreamer without sync, then standalone SyncTool
  datasource - Spark DataFrame write with meta sync (snippet to run in spark-shell/pyspark)
  validate  - Run environment validation only (dataset/database exists, table exists, table path exists)
"""

import argparse
import subprocess
import sys
from typing import Optional

from .util.command_builder import CommandBuilder
from .util.config_loader import get_global_config, load_config
from .util.logging_config import get_logger, setup_logging
from .sync import SYNC_TYPE_REGISTRY, get_sync_tool

MODES = ("inline", "separate", "datasource", "validate")
logger = get_logger(__name__)


def run_validation(sync_type: str, config: dict, base_path: Optional[str], table_name: Optional[str]) -> int:
    """Run validate_environment() for the given sync type; print results. Return 0 if all pass, 1 otherwise."""
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
    parser.add_argument(
        "--sync-type",
        required=True,
        choices=list(SYNC_TYPE_REGISTRY),
        help="Sync target: hive, bigquery, glue, datahub",
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=MODES,
        help="Test mode: inline (streamer+sync), separate (streamer then standalone sync), datasource (Spark write + sync)",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to config.yaml (default: project config.yaml)",
    )
    parser.add_argument(
        "--base-path",
        default=None,
        help="Override base path for table (default from config)",
    )
    parser.add_argument(
        "--table-name",
        default=None,
        help="Override table name (default from config)",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Execute the command(s) with subprocess (default: print only)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Print command(s) only (default). Use --run to execute.",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="After running (or with dry-run), run environment validation: dataset/database, table, path.",
    )
    args = parser.parse_args()
    if args.run:
        args.dry_run = False

    setup_logging()
    logger.info(
        "Starting sync_type=%s mode=%s config=%s",
        args.sync_type,
        args.mode,
        args.config or "(default)",
    )

    try:
        config = load_config(args.config)
    except FileNotFoundError as e:
        logger.error("Config file not found: %s", e)
        return 1

    global_cfg = get_global_config(config=config)
    base_path: Optional[str] = args.base_path or global_cfg.get("base_path") or ""
    table_name: Optional[str] = args.table_name or global_cfg.get("table_name") or "stocks_sync_test"
    if not base_path:
        base_path = "${TABLE_BASE_PATH}"
        logger.debug("base_path not set; using placeholder %s", base_path)

    builder = CommandBuilder(
        config=config,
        config_path=args.config,
        base_path=base_path,
        table_name=table_name,
    )

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
            logger.info("Datasource mode: snippet printed (run manually)")
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
