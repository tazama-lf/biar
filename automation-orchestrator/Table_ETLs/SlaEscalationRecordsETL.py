"""
sla_escalation_records.py
-------------------------
Bronze -> Silver -> Gold ETL for SLA escalation records.
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .BaseETL import BaseETL


class SlaEscalationRecordsETL(BaseETL):
    """Full Bronze -> Silver -> Gold pipeline for sla_escalation_records."""

    @property
    def bronze_path(self) -> str:
        return f"{self.warehouse_root}/bronze/sla_escalation_records"

    @property
    def silver_path(self) -> str:
        return f"{self.warehouse_root}/silver/sla_escalation_records"

    @property
    def gold_path(self) -> str:
        return f"{self.warehouse_root}/gold/sla_escalation_records"

    def bronze(self, source_path: str) -> str:
        raw = self.spark.read.json(source_path)

        bronze = (
            raw
            .withColumn("id", F.col("id").cast("long"))
            .withColumn("tenant_id", F.col("tenant_id").cast("string"))
            .withColumn("case_id", F.col("case_id").cast("long"))
            .withColumn("notified_at", F.col("notified_at").cast("long"))
            .withColumn("sla_state", F.col("sla_state").cast("string"))
            .withColumn("ingested_at_ts", F.current_timestamp())
            .withColumn("source_file_path", F.input_file_name())
            .withColumn("_row_payload_json", F.to_json(F.struct(*[F.col(c) for c in raw.columns])))
        )

        self.write_hudi(
            bronze,
            self.bronze_path,
            self.hudi_opts("bronze_sla_escalation_records", "id", "notified_at"),
        )
        print(f"[SlaEscalationRecordsETL] Bronze written -> {self.bronze_path}")
        return self.bronze_path

    def silver(self) -> str:
        b = self.spark.read.format("hudi").load(self.bronze_path)

        silver = (
            b
            .withColumn("id", F.col("id").cast("long"))
            .withColumn("tenant_id", F.col("tenant_id").cast("string"))
            .withColumn("case_id", F.col("case_id").cast("long"))
            .withColumn("notified_at", F.col("notified_at").cast("long"))
            .withColumn("sla_state", F.upper(F.col("sla_state").cast("string")))
            .withColumn("notified_at_ts", F.to_timestamp((F.col("notified_at") / 1000).cast("double")))
            .drop("_row_payload_json")
        )

        w = Window.partitionBy("id").orderBy(F.col("notified_at").desc_nulls_last())
        silver = silver.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

        self.write_hudi(
            silver,
            self.silver_path,
            self.hudi_opts("silver_sla_escalation_records", "id", "notified_at"),
        )
        print(f"[SlaEscalationRecordsETL] Silver written -> {self.silver_path}")
        return self.silver_path

    def gold(self) -> str:
        s = self.spark.read.format("hudi").load(self.silver_path)

        gold = s.select(
            F.col("id").cast("long").alias("id"),
            F.col("tenant_id").cast("string").alias("tenant_id"),
            F.col("case_id").cast("long").alias("case_id"),
            F.col("notified_at").cast("long").alias("notified_at"),
            F.col("notified_at_ts").cast("timestamp").alias("notified_at_ts"),
            F.col("sla_state").cast("string").alias("sla_state"),
            F.col("ingested_at_ts").cast("timestamp").alias("ingested_at_ts"),
            F.col("source_file_path").cast("string").alias("source_file_path"),
        )

        self.write_hudi(
            gold,
            self.gold_path,
            self.hudi_opts("gold_sla_escalation_records", "id", "notified_at"),
        )
        print(f"[SlaEscalationRecordsETL] Gold written -> {self.gold_path}")
        return self.gold_path

    def run(self, source_path: str) -> str:
        print(f"[SlaEscalationRecordsETL] Starting Bronze -> Silver -> Gold from {source_path}")
        self.bronze(source_path)
        self.silver()
        self.gold()
        print("[SlaEscalationRecordsETL] ETL complete.")
        return self.gold_path
