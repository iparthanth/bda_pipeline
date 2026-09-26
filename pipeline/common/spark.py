"""SparkSession factory (local mode by default, Delta Lake + Hive catalog).

* ING-2: all heavy transformations run on Spark.
* ING-3: tables are registered in a Derby-backed Hive metastore (``episcope_*`` databases).
* ING-5: when ``paths.lake_root`` is an ``s3a://`` URI (MinIO), the S3A connector is configured
  from ``S3_ENDPOINT`` / ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` (read from .env).
* ING-7: Delta Lake gives ACID MERGE (idempotent loads) and time travel.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from pipeline.common.config import load_config, path

HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"
_SESSION: SparkSession | None = None


def get_spark(app_suffix: str = "", extra_conf: dict[str, str] | None = None) -> SparkSession:
    global _SESSION
    if _SESSION is not None and extra_conf is None:
        return _SESSION

    cfg = load_config()["spark"]
    # Python workers (applyInPandas / pandas UDFs) must use the same interpreter/venv as the driver
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    Path(path("event_log_dir")).mkdir(parents=True, exist_ok=True)
    lake_root = path("lake_root")

    builder = (
        SparkSession.builder.master(os.environ.get("SPARK_MASTER", cfg["master"]))
        .appName(f"{cfg['app_name']}{('-' + app_suffix) if app_suffix else ''}")
        .config("spark.driver.memory", cfg["driver_memory"])
        .config("spark.sql.shuffle.partitions", str(cfg["shuffle_partitions"]))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.warehouse.dir", path("warehouse"))
        .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={path('metastore')};create=true")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file://" + path("event_log_dir"))
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .enableHiveSupport()
    )
    packages: list[str] = []
    if lake_root.startswith("s3a://"):
        packages.append(HADOOP_AWS)
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", os.environ.get("S3_ENDPOINT", "http://localhost:9000"))
            .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", ""))
            .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.delta.logStore.s3a.impl", "io.delta.storage.S3SingleDriverLogStore")
        )
    for k, v in (extra_conf or {}).items():
        builder = builder.config(k, v)

    from delta import configure_spark_with_delta_pip

    spark = configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()
    spark.sparkContext.setLogLevel(cfg.get("log_level", "WARN"))
    for db in ("episcope_bronze", "episcope_silver", "episcope_gold"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    if extra_conf is None:
        _SESSION = spark
    return spark


def register_table(spark: SparkSession, layer: str, table: str, location: str) -> None:
    """Register (or refresh) a Delta table in the Spark SQL / Hive catalog (ING-3)."""
    spark.sql(f"CREATE TABLE IF NOT EXISTS episcope_{layer}.{table} USING DELTA LOCATION '{location}'")


def table_exists(spark: SparkSession, location: str) -> bool:
    from delta.tables import DeltaTable

    try:
        return DeltaTable.isDeltaTable(spark, location)
    except Exception:  # pragma: no cover - storage not reachable
        return False
