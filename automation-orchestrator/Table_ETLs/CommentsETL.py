"""
comments.py
-----------
Bronze → Silver → Gold ETL for the Comments domain.
"""

from __future__ import annotations

from pyspark.sql import functions as F

from .BaseETL import BaseETL


class CommentsETL(BaseETL):
    """Full Bronze → Silver → Gold pipeline for Comments."""

    @property
    def bronze_path(self) -> str:
        return f"{self.warehouse_root}/bronze/comments"

    @property
    def silver_path(self) -> str:
        return f"{self.warehouse_root}/silver/comments"

    @property
    def gold_path(self) -> str:
        return f"{self.warehouse_root}/gold/comments"

    # ------------------------------------------------------------------
    # BRONZE
    # ------------------------------------------------------------------

    def bronze(self, source_path: str) -> str:
        df = self.spark.read.json(source_path)

        bronze = (
            df
            .withColumn("ingested_at_ts",    F.current_timestamp())
            .withColumn("source_file_path",  F.input_file_name())
            .withColumn("_row_payload_json", F.to_json(F.struct(*[F.col(c) for c in df.columns])))
        )

        self.write_hudi(
            bronze,
            self.bronze_path,
            self.hudi_opts("bronze_comments", "comment_id", "ingested_at_ts"),
        )
        print(f"[CommentsETL] Bronze written → {self.bronze_path}")
        return self.bronze_path

    # ------------------------------------------------------------------
    # SILVER
    # ------------------------------------------------------------------

    def silver(self) -> str:
        bronze_in = self.spark.read.format("hudi").load(self.bronze_path)

        # created_at / updated_at are stored as epoch microseconds
        created_ts = F.to_timestamp((F.col("created_at").cast("double") / F.lit(1_000)))
        updated_ts = F.to_timestamp((F.col("updated_at").cast("double") / F.lit(1_000)))

        silver = (
            bronze_in
            .withColumn("comment_created_ts",   created_ts)
            .withColumn("comment_updated_ts",   updated_ts)
            .withColumn("comment_created_date", F.to_date(created_ts))
            .withColumn("comment_updated_date", F.to_date(updated_ts))
            .withColumn("ingested_at_ts",       F.coalesce(F.col("ingested_at_ts"), F.current_timestamp()))
            # _row_payload_json is bronze-only — drop it at silver
            .drop("_row_payload_json")
        )

        self.write_hudi(
            silver,
            self.silver_path,
            self.hudi_opts("silver_comments", "comment_id", "ingested_at_ts"),
        )
        print(f"[CommentsETL] Silver written → {self.silver_path}")
        return self.silver_path

    # ------------------------------------------------------------------
    # GOLD
    # ------------------------------------------------------------------

    def gold(self) -> str:
        silver_in = self.spark.read.format("hudi").load(self.silver_path)

        gold = (
            silver_in
            .withColumn("note_len",   F.length(F.col("note")))
            .withColumn("has_task_id", F.when(F.col("task_id").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
        )

        self.write_hudi(
            gold,
            self.gold_path,
            self.hudi_opts("gold_comments", "comment_id", "ingested_at_ts"),
        )
        print(f"[CommentsETL] Gold written → {self.gold_path}")
        return self.gold_path

    # ------------------------------------------------------------------
    # ORCHESTRATOR
    # ------------------------------------------------------------------

    def run(self, source_path: str) -> str:
        print(f"[CommentsETL] Starting Bronze → Silver → Gold from {source_path}")
        self.bronze(source_path)
        self.silver()
        self.gold()
        print("[CommentsETL] ETL complete.")
        return self.gold_path