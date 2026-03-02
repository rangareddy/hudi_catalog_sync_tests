import os
import sys
import logging

from pyspark.sql import SparkSession

def get_spark_session(app_name: str, metastore_uris: str) -> SparkSession: 
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.hive.metastore.uris", metastore_uris) \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def validate_database_table(spark: SparkSession, database: str, table_name: str, logger: logging.Logger) -> int:   
    if not database or not table_name:
        logger.error("Database and table name are required")
        return 1
    tables_df = spark.sql(f"SHOW TABLES IN {database}")
    tables_list = [row.tableName for row in tables_df.collect()]  
    db_table_name = f"{database}.{table_name}"
    if table_name in tables_list:
        logger.info(f"Table {db_table_name} found in database {database}.")
        return 0
    else:
        logger.error(f"Table {db_table_name} not found in database {database}. Table list: {tables_list}")
        return 1

def main() -> int:
    database = os.getenv("val_database_name")
    table_name = os.getenv("val_table_name")
    hive_thrift_uri = os.getenv("val_hive_thrift_uri", "thrift://localhost:9083")
    spark = get_spark_session("Hive Database Table Validation", hive_thrift_uri)
    log4j_logger = spark._jvm.org.apache.log4j
    logger = log4j_logger.LogManager.getLogger(f"validate_database_{database}_{table_name}")
    result = validate_database_table(spark, database, table_name, logger) 
    spark.stop()
    return result

if __name__ == "__main__":
    sys.exit(main())