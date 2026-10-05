"""
spark_utils.py
--------------
Shared SparkSession factory for the automation-orchestrator.

Extracted from ``automation_orchestrator_api.py`` so the same Hudi/S3A
configuration is available to one-off maintenance scripts (e.g.
``rebuild_metrics_tms.py``) without importing the FastAPI app.
"""

from __future__ import annotations

import os

from lakehouse_automation_pipeline import _env


def get_spark_session():
    """
    Build and return a SparkSession configured for Tazama / Hudi / S3A.
    Identical configuration to the monolith's get_spark_session().
    """
    from pyspark.sql import SparkSession

    spark_home = _env("SPARK_HOME", "/opt/spark")
    os.environ["SPARK_HOME"] = spark_home

    spark_master = _env("SPARK_MASTER", "local[2]")
    spark_local_dir = _env("SPARK_LOCAL_DIR", "/tmp/spark")
    os.environ.setdefault("SPARK_LOCAL_DIRS", spark_local_dir)

    default_jars = [
        "/opt/jars/hudi-spark3.4-bundle_2.12-0.14.1.jar",
        "/opt/jars/hadoop-aws-3.3.4.jar",
        "/opt/jars/aws-java-sdk-bundle-1.12.262.jar",
    ]
    jars_env = _env("SPARK_JARS", ",".join(default_jars))
    jar_files = [j.strip() for j in jars_env.split(",") if j.strip()]

    s3_endpoint = _env("S3A_ENDPOINT", "")
    s3_access_key = _env("S3A_ACCESS_KEY", "")
    s3_secret_key = _env("S3A_SECRET_KEY", "")

    spark = (
        SparkSession.builder.appName("Tazama_Hudi_ETL")
        .master(spark_master)
        .config("spark.jars", ",".join(jar_files))
        .config("spark.driver.extraClassPath", ":".join(jar_files))
        .config("spark.executor.extraClassPath", ":".join(jar_files))
        # S3A / Ozone
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.impl.disable.cache", "true")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        # Hudi
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
        .config(
            "spark.sql.extensions",
            "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        )
        .config("spark.hadoop.parquet.avro.write-old-list-structure", "false")
        # Memory & performance
        .config("spark.local.dir", _env("SPARK_LOCAL_DIR", "/tmp/spark"))
        .config("spark.driver.memory", _env("SPARK_DRIVER_MEMORY", "6g"))
        .config(
            "spark.driver.memoryOverhead", _env("SPARK_DRIVER_MEMORY_OVERHEAD", "2g")
        )
        .config(
            "spark.driver.maxResultSize", _env("SPARK_DRIVER_MAX_RESULT_SIZE", "4g")
        )
        .config("spark.executor.memory", _env("SPARK_EXECUTOR_MEMORY", "6g"))
        .config(
            "spark.executor.memoryOverhead",
            _env("SPARK_EXECUTOR_MEMORY_OVERHEAD", "2g"),
        )
        .config(
            "spark.sql.shuffle.partitions", _env("SPARK_SQL_SHUFFLE_PARTITIONS", "48")
        )
        .config("spark.default.parallelism", _env("SPARK_DEFAULT_PARALLELISM", "48"))
        .config("spark.executor.cores", _env("SPARK_EXECUTOR_CORES", "2"))
        .config("spark.driver.cores", _env("SPARK_DRIVER_CORES", "2"))
        .config("spark.memory.fraction", _env("SPARK_MEMORY_FRACTION", "0.6"))
        .config(
            "spark.memory.storageFraction", _env("SPARK_MEMORY_STORAGE_FRACTION", "0.3")
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config(
            "spark.sql.adaptive.advisoryPartitionSizeInBytes",
            _env("SPARK_SQL_ADVISORY_PARTITION_SIZE", "64mb"),
        )
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark Version: {spark.version}")
    return spark
