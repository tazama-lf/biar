"""
case_priority_thresholds.py
---------------------------
Bronze -> Silver -> Gold ETL for case priority threshold configuration.
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .BaseETL import BaseETL


class CasePriorityThresholdsETL(BaseETL):
    """Full Bronze -> Silver -> Gold pipeline for case_priority_thresholds."""

    @property
    def bronze_path(self) -> str:
        return f"{self.warehouse_root}/bronze/case_priority_thresholds"

    @property
    def silver_path(self) -> str:
        return f"{self.warehouse_root}/silver/case_priority_thresholds"

    @property
    def gold_path(self) -> str:
        return f"{self.warehouse_root}/gold/case_priority_thresholds"

    def bronze(self, source_path: str) -> str:
        raw = self.spark.read.json(source_path)

        bronze = (
            raw
            .withColumn("id", F.col("id").cast("long"))
            .withColumn("tenant_id", F.col("tenant_id").cast("string"))
            .withColumn("high_threshold", F.col("high_threshold").cast("double"))
            .withColumn("medium_threshold", F.col("medium_threshold").cast("double"))
            .withColumn("created_at", F.col("created_at").cast("long"))
            .withColumn("updated_at", F.col("updated_at").cast("long"))
            .withColumn("ingested_at_ts", F.current_timestamp())
            .withColumn("source_file_path", F.input_file_name())
            .withColumn("_row_payload_json", F.to_json(F.struct(*[F.col(c) for c in raw.columns])))
        )

        self.write_hudi(
            bronze,
            self.bronze_path,
            self.hudi_opts("bronze_case_priority_thresholds", "id", "updated_at"),
        )
        print(f"[CasePriorityThresholdsETL] Bronze written -> {self.bronze_path}")
        return self.bronze_path

    def silver(self) -> str:
        b = self.spark.read.format("hudi").load(self.bronze_path)

        silver = (
            b
            .withColumn("id", F.col("id").cast("long"))
            .withColumn("tenant_id", F.col("tenant_id").cast("string"))
            .withColumn("high_threshold", F.col("high_threshold").cast("double"))
            .withColumn("medium_threshold", F.col("medium_threshold").cast("double"))
            .withColumn("created_at", F.col("created_at").cast("long"))
            .withColumn("updated_at", F.col("updated_at").cast("long"))
            .withColumn("created_at_ts", F.to_timestamp((F.col("created_at") / 1000).cast("double")))
            .withColumn("updated_at_ts", F.to_timestamp((F.col("updated_at") / 1000).cast("double")))
            .drop("_row_payload_json")
        )

        w = Window.partitionBy("id").orderBy(F.col("updated_at").desc_nulls_last())
        silver = silver.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

        self.write_hudi(
            silver,
            self.silver_path,
            self.hudi_opts("silver_case_priority_thresholds", "id", "updated_at"),
        )
        print(f"[CasePriorityThresholdsETL] Silver written -> {self.silver_path}")
        return self.silver_path

    def gold(self) -> str:
        s = self.spark.read.format("hudi").load(self.silver_path)

        gold = s.select(
            F.col("id").cast("long").alias("id"),
            F.col("tenant_id").cast("string").alias("tenant_id"),
            F.col("high_threshold").cast("double").alias("high_threshold"),
            F.col("medium_threshold").cast("double").alias("medium_threshold"),
            F.col("created_at").cast("long").alias("created_at"),
            F.col("updated_at").cast("long").alias("updated_at"),
            F.col("created_at_ts").cast("timestamp").alias("created_at_ts"),
            F.col("updated_at_ts").cast("timestamp").alias("updated_at_ts"),
            F.col("ingested_at_ts").cast("timestamp").alias("ingested_at_ts"),
            F.col("source_file_path").cast("string").alias("source_file_path"),
        )

        self.write_hudi(
            gold,
            self.gold_path,
            self.hudi_opts("gold_case_priority_thresholds", "id", "updated_at"),
        )
        print(f"[CasePriorityThresholdsETL] Gold written -> {self.gold_path}")
        return self.gold_path

    def run(self, source_path: str) -> str:
        print(f"[CasePriorityThresholdsETL] Starting Bronze -> Silver -> Gold from {source_path}")
        self.bronze(source_path)
        self.silver()
        self.gold()
        print("[CasePriorityThresholdsETL] ETL complete.")
        return self.gold_path
