"""
base.py
-------
Abstract base class and shared helpers for all Tazama ETL pipelines.
Every domain ETL class inherits from BaseETL and gets access to:
  - hudi_opts() - build Hudi writer options dict
  - write_hudi() - single-call Hudi append/upsert
  - infer_json_schema() - schema inference from a JSON string column
  - read_latest_hudi() - deduplicated Hudi read (latest commit per record key)
  - ensure_columns() - add missing columns as typed nulls
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class BaseETL(ABC):
    """
    Abstract base for all domain ETL pipelines.

    Concrete subclasses must implement:
        bronze(source_path) -> str   (returns bronze path)
        silver()            -> str   (reads bronze, writes silver)
        gold()              -> str   (reads silver, writes gold)
        run(source_path)    -> str   (orchestrates bronze→silver→gold)
    """

    def __init__(self, spark: SparkSession, warehouse_root: str) -> None:
        self.spark = spark
        self.warehouse_root = warehouse_root

    # ------------------------------------------------------------------
    # HUDI HELPERS
    # ------------------------------------------------------------------

    @staticmethod
    def hudi_opts(
        table_name: str,ik,,
        record_key: str,
        precombine: str,
        partition: Optional[str] = None,
        payload_class: Optional[str] = None,
    ) -> dict:
        """Return a base Hudi writer options dict (COPY_ON_WRITE + upsert)."""
        opts = {
            "hoodie.table.name": table_name,
            "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.datasource.write.recordkey.field": record_key,
            "hoodie.datasource.write.precombine.field": precombine,
            # Schema evolution
            "hoodie.datasource.write.schema.evolution.enable": "true",
            "hoodie.datasource.read.schema.evolution.enable": "true",
            "hoodie.datasource.write.reconcile.schema": "true",
            "hoodie.schema.on.read.enable": "true",
            # Performance / reliability
            "hoodie.metadata.enable": "false",
            "hoodie.index.type": "BLOOM",
        }

        if partition:
            opts.update(
                {
                    "hoodie.datasource.write.partitionpath.field": partition,
                    "hoodie.datasource.write.keygenerator.class": (
                        "org.apache.hudi.keygen.SimpleKeyGenerator"
                    ),
                    "hoodie.datasource.write.hive_style_partitioning": "true",
                }
            )
        else:
            opts["hoodie.datasource.write.keygenerator.class"] = (
                "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
            )

        if payload_class:
            opts["hoodie.datasource.write.payload.class"] = payload_class

        return opts

    @staticmethod
    def write_hudi(df: DataFrame, path: str, opts: dict) -> None:
        """Append-upsert a DataFrame to a Hudi table at *path*."""
        df.write.format("hudi").options(**opts).mode("append").save(path)

    def infer_json_schema(self, df: DataFrame, col_name: str):
        """Infer a StructType by schema-on-read from a JSON string column."""
        return self.spark.read.json(
            df.select(col_name)
            .where(F.col(col_name).isNotNull())
            .rdd.map(lambda r: r[0])
        ).schema

    def read_latest_hudi(self, path: str) -> DataFrame:
        """
        Read a Hudi table and keep only the latest commit per record key.
        Strips all _hoodie_* metadata columns.
        """
        raw = self.spark.read.format("hudi").load(path)
        w = Window.partitionBy("_hoodie_record_key").orderBy(
            F.col("_hoodie_commit_time").desc()
        )
        return (
            raw.withColumn("_rn", F.row_number().over(w))
            .where("_rn = 1")
            .drop("_rn")
            .drop(*[c for c in raw.columns if c.startswith("_hoodie_")])
        )

    @staticmethod
    def drop_hoodie_cols(df: DataFrame) -> DataFrame:
        """Remove all Hudi internal metadata columns."""
        return df.drop(*[c for c in df.columns if c.startswith("_hoodie_")])

    @staticmethod
    def ensure_columns(df: DataFrame, col_types: dict) -> DataFrame:
        """
        Ensure every (col_name -> spark_type_string) in *col_types* exists
        in *df*; add null-cast columns for any that are missing.
        """
        for col_name, col_type in col_types.items():
            if col_name not in df.columns:
                df = df.withColumn(col_name, F.lit(None).cast(col_type))
        return df

    # ------------------------------------------------------------------
    # ABSTRACT INTERFACE
    # ------------------------------------------------------------------

    @abstractmethod
    def bronze(self, source_path: str) -> str:
        """Ingest raw data → bronze Hudi table. Returns bronze path."""

    @abstractmethod
    def silver(self) -> str:
        """Transform bronze → silver Hudi table. Returns silver path."""

    @abstractmethod
    def gold(self) -> str:
        """Aggregate/enrich silver → gold Hudi table. Returns gold path."""

    @abstractmethod
    def run(self, source_path: str) -> str:
        """Orchestrate the full bronze → silver → gold pipeline."""