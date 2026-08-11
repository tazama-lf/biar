"""
entity.py
---------
Bronze → Silver → Gold ETL for the Entity domain.

Raw source columns:
  credttm   - epoch milliseconds (creation timestamp)
  id        - entity identifier
  tenantid  - tenant identifier
"""

from __future__ import annotations

from pyspark.sql import functions as F

from .BaseETL import BaseETL


class EntityETL(BaseETL):
    """Full Bronze → Silver → Gold pipeline for Entity data."""

    @property
    def bronze_path(self) -> str:
        return f"{self.warehouse_root}/bronze/entity"

    @property
    def silver_path(self) -> str:
        return f"{self.warehouse_root}/silver/entity"

    @property
    def gold_path(self) -> str:
        return f"{self.warehouse_root}/gold/entity"

    # ------------------------------------------------------------------
    # BRONZE
    # ------------------------------------------------------------------

    def bronze(self, source_path: str) -> str:
        df = self.spark.read.json(source_path)

        bronze = (
            df
            .withColumn("credttm",          F.col("credttm").cast("long"))
            .withColumn("id",               F.col("id").cast("string"))
            .withColumn("tenantid",         F.col("tenantid").cast("string"))
            .withColumn("ingested_at_ts",   F.current_timestamp())
            .withColumn("source_file_path", F.input_file_name())
            .withColumn(
                "record_hash",
                F.sha2(
                    F.concat_ws("||",
                        F.coalesce(F.col("id"),       F.lit("")),
                        F.coalesce(F.col("tenantid"), F.lit("")),
                        F.coalesce(F.col("credttm").cast("string"), F.lit("")),
                    ),
                    256,
                ),
            )
            .withColumn("_row_payload_json", F.to_json(F.struct(*[F.col(c) for c in df.columns])))
        )

        self.write_hudi(
            bronze,
            self.bronze_path,
            self.hudi_opts("bronze_entity", "record_hash", "ingested_at_ts"),
        )
        print(f"[EntityETL] Bronze written → {self.bronze_path}")
        return self.bronze_path

    # ------------------------------------------------------------------
    # SILVER
    # ------------------------------------------------------------------

    def silver(self) -> str:
        bronze_in = self.spark.read.format("hudi").load(self.bronze_path)

        # credttm is epoch milliseconds → timestamp
        entity_created_ts = F.to_timestamp((F.col("credttm").cast("double") / F.lit(1_000)))

        silver = (
            bronze_in
            # Normalize column names
            .withColumnRenamed("id",       "entity_id")
            .withColumnRenamed("tenantid", "tenant_id")
            # Parse timestamp
            .withColumn("entity_created_ts",   entity_created_ts)
            .withColumn("entity_created_date", F.to_date(entity_created_ts))
            .withColumn("ingested_at_ts",      F.coalesce(F.col("ingested_at_ts"), F.current_timestamp()))
            # _row_payload_json is bronze-only
            .drop("_row_payload_json")
        )

        self.write_hudi(
            silver,
            self.silver_path,
            self.hudi_opts("silver_entity", "record_hash", "ingested_at_ts"),
        )
        print(f"[EntityETL] Silver written → {self.silver_path}")
        return self.silver_path

    # ------------------------------------------------------------------
    # GOLD
    # ------------------------------------------------------------------

    def gold(self) -> str:
        silver_in = self.spark.read.format("hudi").load(self.silver_path)

        gold = (
            silver_in
            .withColumn(
                "pk",
                F.sha2(
                    F.concat_ws("||",
                        F.lit("entity"),
                        F.coalesce(F.col("entity_id"), F.lit("")),
                        F.coalesce(F.col("tenant_id"), F.lit("")),
                    ),
                    256,
                ),
            )
            .withColumn("ingested_at_ts", F.current_timestamp())
            .select(
                F.col("pk").cast("string").alias("pk"),
                F.col("entity_id").cast("string").alias("entity_id"),
                F.col("tenant_id").cast("string").alias("tenant_id"),
                F.col("credttm").cast("long").alias("credttm_ms"),
                F.col("entity_created_ts").cast("timestamp").alias("entity_created_ts"),
                F.col("entity_created_date").cast("date").alias("entity_created_date"),
                F.col("source_file_path").cast("string").alias("source_file_path"),
                F.col("record_hash").cast("string").alias("record_hash"),
                F.col("ingested_at_ts").cast("timestamp").alias("ingested_at_ts"),
            )
        )

        self.write_hudi(
            gold,
            self.gold_path,
            self.hudi_opts("gold_entity", "pk", "ingested_at_ts"),
        )
        print(f"[EntityETL] Gold written → {self.gold_path}")
        return self.gold_path

    # ------------------------------------------------------------------
    # ORCHESTRATOR
    # ------------------------------------------------------------------

    def run(self, source_path: str) -> str:
        print(f"[EntityETL] Starting Bronze → Silver → Gold from {source_path}")
        self.bronze(source_path)
        self.silver()
        self.gold()
        print("[EntityETL] ETL complete.")
        return self.gold_path