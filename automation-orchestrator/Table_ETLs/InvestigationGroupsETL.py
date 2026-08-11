"""
investigation_groups.py
-----------------------
Bronze -> Silver -> Gold ETL for investigation groups.
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .BaseETL import BaseETL


class InvestigationGroupsETL(BaseETL):
    """Full Bronze -> Silver -> Gold pipeline for investigation_groups."""

    @property
    def bronze_path(self) -> str:
        return f"{self.warehouse_root}/bronze/investigation_groups"

    @property
    def silver_path(self) -> str:
        return f"{self.warehouse_root}/silver/investigation_groups"

    @property
    def gold_path(self) -> str:
        return f"{self.warehouse_root}/gold/investigation_groups"

    def bronze(self, source_path: str) -> str:
        raw = self.spark.read.json(source_path)

        bronze = (
            raw
            .withColumn("id", F.col("id").cast("long"))
            .withColumn("alert_id", F.col("alert_id").cast("long"))
            .withColumn("tenant_id", F.col("tenant_id").cast("string"))
            .withColumn("created_at", F.col("created_at").cast("long"))
            .withColumn("ingested_at_ts", F.current_timestamp())
            .withColumn("source_file_path", F.input_file_name())
            .withColumn("_row_payload_json", F.to_json(F.struct(*[F.col(c) for c in raw.columns])))
        )

        self.write_hudi(
            bronze,
            self.bronze_path,
            self.hudi_opts("bronze_investigation_groups", "id", "created_at"),
        )
        print(f"[InvestigationGroupsETL] Bronze written -> {self.bronze_path}")
        return self.bronze_path

    def silver(self) -> str:
        b = self.spark.read.format("hudi").load(self.bronze_path)

        silver = (
            b
            .withColumn("id", F.col("id").cast("long"))
            .withColumn("alert_id", F.col("alert_id").cast("long"))
            .withColumn("tenant_id", F.col("tenant_id").cast("string"))
            .withColumn("created_at", F.col("created_at").cast("long"))
            .withColumn("created_at_ts", F.to_timestamp((F.col("created_at") / 1000).cast("double")))
            .drop("_row_payload_json")
        )

        w = Window.partitionBy("id").orderBy(F.col("created_at").desc_nulls_last())
        silver = silver.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

        self.write_hudi(
            silver,
            self.silver_path,
            self.hudi_opts("silver_investigation_groups", "id", "created_at"),
        )
        print(f"[InvestigationGroupsETL] Silver written -> {self.silver_path}")
        return self.silver_path

    def gold(self) -> str:
        s = self.spark.read.format("hudi").load(self.silver_path)

        gold = s.select(
            F.col("id").cast("long").alias("id"),
            F.col("alert_id").cast("long").alias("alert_id"),
            F.col("tenant_id").cast("string").alias("tenant_id"),
            F.col("created_at").cast("long").alias("created_at"),
            F.col("created_at_ts").cast("timestamp").alias("created_at_ts"),
            F.col("ingested_at_ts").cast("timestamp").alias("ingested_at_ts"),
            F.col("source_file_path").cast("string").alias("source_file_path"),
        )

        self.write_hudi(
            gold,
            self.gold_path,
            self.hudi_opts("gold_investigation_groups", "id", "created_at"),
        )
        print(f"[InvestigationGroupsETL] Gold written -> {self.gold_path}")
        return self.gold_path

    def run(self, source_path: str) -> str:
        print(f"[InvestigationGroupsETL] Starting Bronze -> Silver -> Gold from {source_path}")
        self.bronze(source_path)
        self.silver()
        self.gold()
        print("[InvestigationGroupsETL] ETL complete.")
        return self.gold_path
