"""
Builds spark-submit commands and PySpark snippets for Hudi catalog sync tests.
Supports three modes: inline, separate, datasource.
"""

import shlex
from typing import List, Optional

from .config_loader import get_global_config, load_config
from .logging_config import get_logger
from .streamer_args import build_base_streamer_args
from ..sync import get_sync_tool
from .sync_runner import (
    build_streamer_sync_args,
    build_standalone_sync_args,
)

logger = get_logger(__name__)

DEFAULT_TABLE_NAME = "stocks_sync_test"


def _jar_base(config: dict):
    """Return (base_path, scala, hudi_version, spark_version) or (None, None, None, None) if no jars_path."""
    g = get_global_config(config=config)
    jars_path = (g.get("jars_path") or "").rstrip("/")
    if not jars_path:
        return None, None, g.get("hudi_version", "0.16.0-SNAPSHOT"), g.get("spark_version", "3.5")
    spark_version = g.get("spark_version", "3.5")
    hudi_version = g.get("hudi_version", "0.16.0-SNAPSHOT")
    scala = g.get("scala_version", "2.12")
    base = f"{jars_path}/{hudi_version}/{spark_version}"
    return base, scala, hudi_version, spark_version


class CommandBuilder:
    """
    Builds commands for Hudi sync tests from config and overrides.
    Use build_inline_command(), build_ingestion_only_command(), build_standalone_sync_command(),
    and build_datasource_snippet() for the three test modes.
    """

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
        """Build spark-submit jar/packages args and utilities slim jar path."""
        base, scala, hudi_version, spark_version = _jar_base(self._config)
        if base is None:
            if sync_type == "bigquery":
                return (
                    ["--jars", "${HUDI_JARS}", "--packages", "${PACKAGES}"],
                    "${HUDI_UTILITIES_SLIM_JAR}",
                )
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
            jars = f"{hudi_spark_jar},{datahub_jar}"
            return ["--jars", jars], utilities_slim
        return ["--jars", hudi_spark_jar], utilities_slim

    def _get_standalone_main_jar(self, sync_type: str) -> str:
        """Path to the sync-tool bundle JAR used as main class JAR for standalone submit."""
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
        """Build spark-submit command for HoodieStreamer with sync enabled (inline mode)."""
        logger.info("Building inline command for sync_type=%s", sync_type)
        jar_args, utilities_jar = self._get_jar_args_and_utilities_jar(sync_type)
        cmd: List[str] = [
            "spark-submit",
            "--master", "yarn",
            *jar_args,
            "--conf", f"spark.driver.extraClassPath={utilities_jar}",
            "--conf", f"spark.executor.extraClassPath={utilities_jar}",
            "--class", "org.apache.hudi.utilities.streamer.HoodieStreamer",
            utilities_jar,
            *build_base_streamer_args(
                config=self._config,
                base_path=self._base_path or None,
                table_name=self._table_name or None,
            ),
            *build_streamer_sync_args(
                sync_type,
                config_path=self._config_path,
                config=self._config,
                base_path=self._base_path or None,
                table_name=self._table_name or None,
            ),
        ]
        return cmd

    def build_ingestion_only_command(self, sync_type: str) -> List[str]:
        """Build spark-submit command for HoodieStreamer without sync (step 1 of separate mode)."""
        logger.info("Building ingestion-only command for sync_type=%s", sync_type)
        jar_args, utilities_jar = self._get_jar_args_and_utilities_jar(sync_type)
        cmd: List[str] = [
            "spark-submit",
            "--master", "yarn",
            *jar_args,
            "--class", "org.apache.hudi.utilities.streamer.HoodieStreamer",
            utilities_jar,
            *build_base_streamer_args(
                config=self._config,
                base_path=self._base_path or None,
                table_name=self._table_name or None,
            ),
        ]
        return cmd

    def build_standalone_sync_command(self, sync_type: str) -> List[str]:
        """Build spark-submit command for standalone SyncTool (step 2 of separate mode)."""
        logger.info("Building standalone sync command for sync_type=%s", sync_type)
        tool = get_sync_tool(
            sync_type,
            config_path=self._config_path,
            config=self._config,
            base_path=self._base_path,
            table_name=self._table_name,
        )
        jar_args, _ = self._get_jar_args_and_utilities_jar(sync_type)
        main_jar = self._get_standalone_main_jar(sync_type)
        cmd: List[str] = [
            "spark-submit",
            "--master", "yarn",
            *jar_args,
            "--class", tool.sync_tool_class_name,
            main_jar,
            *build_standalone_sync_args(
                sync_type,
                config_path=self._config_path,
                config=self._config,
                base_path=self._base_path or None,
                table_name=self._table_name or None,
            ),
        ]
        return cmd

    def build_datasource_snippet(self, sync_type: str) -> str:
        """Build PySpark snippet for DataFrame write with meta sync (datasource mode)."""
        logger.info("Building datasource snippet for sync_type=%s", sync_type)
        tool = get_sync_tool(
            sync_type,
            config_path=self._config_path,
            config=self._config,
            base_path=self._base_path,
            table_name=self._table_name,
        )
        global_cfg = get_global_config(config=self._config)
        base_path = self._base_path or tool.config.get("base_path") or ""
        table_name = self._table_name or tool.config.get("table_name") or DEFAULT_TABLE_NAME
        opts = tool.get_datasource_hoodie_options()
        opts["hoodie.table.name"] = table_name
        opts["hoodie.datasource.write.recordkey.field"] = global_cfg.get("record_key_field", "symbol")
        opts["hoodie.datasource.write.precombine.field"] = global_cfg.get("precombine_field", "ts")
        opts["hoodie.datasource.write.partitionpath.field"] = global_cfg.get("partition_path_field", "date")
        opts["hoodie.datasource.write.hive_style_partitioning"] = str(
            global_cfg.get("hive_style_partitioning", True)
        ).lower()
        option_lines = "\n    ".join(
            f'option("{k}", "{v}").\\' for k, v in opts.items() if v
        )
        return f'''# Datasource mode: Spark write with {sync_type} meta sync
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
'''

    @staticmethod
    def command_to_string(cmd: List[str]) -> str:
        """Format command list as a shell-quoted string for display.
        Pairs --hoodie-conf with its value on the same line.
        """
        segments: List[str] = []
        i = 0
        while i < len(cmd):
            if cmd[i] == "--hoodie-conf" and i + 1 < len(cmd):
                segments.append(
                    shlex.quote(cmd[i]) + " " + shlex.quote(cmd[i + 1])
                )
                i += 2
            else:
                segments.append(shlex.quote(cmd[i]))
                i += 1
        return " \\\n  ".join(segments)


def _create_builder(
    config: Optional[dict] = None,
    config_path: Optional[str] = None,
    base_path: Optional[str] = None,
    table_name: Optional[str] = None,
) -> CommandBuilder:
    if config is None:
        config = load_config(config_path)
    return CommandBuilder(config=config, config_path=config_path, base_path=base_path, table_name=table_name)


def build_inline_command(
    sync_type: str,
    config_path: Optional[str] = None,
    base_path: Optional[str] = None,
    table_name: Optional[str] = None,
    config: Optional[dict] = None,
) -> List[str]:
    """Build inline command (HoodieStreamer with sync)."""
    return _create_builder(config, config_path, base_path, table_name).build_inline_command(sync_type)


def build_ingestion_only_command(
    sync_type: str,
    config_path: Optional[str] = None,
    base_path: Optional[str] = None,
    table_name: Optional[str] = None,
    config: Optional[dict] = None,
) -> List[str]:
    """Build ingestion-only command (step 1 of separate mode)."""
    return _create_builder(config, config_path, base_path, table_name).build_ingestion_only_command(sync_type)


def build_standalone_sync_command(
    sync_type: str,
    config_path: Optional[str] = None,
    base_path: Optional[str] = None,
    table_name: Optional[str] = None,
    config: Optional[dict] = None,
) -> List[str]:
    """Build standalone sync command (step 2 of separate mode)."""
    return _create_builder(config, config_path, base_path, table_name).build_standalone_sync_command(sync_type)


def build_datasource_snippet(
    sync_type: str,
    config_path: Optional[str] = None,
    base_path: Optional[str] = None,
    table_name: Optional[str] = None,
    config: Optional[dict] = None,
) -> str:
    """Build datasource PySpark snippet."""
    return _create_builder(config, config_path, base_path, table_name).build_datasource_snippet(sync_type)
