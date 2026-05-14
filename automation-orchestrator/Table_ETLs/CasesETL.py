"""
cases.py
--------
Bronze → Silver → Gold ETL for the Cases domain.
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .BaseETL import BaseETL


class CasesETL(BaseETL):
    """Full Bronze → Silver → Gold pipeline for raw case JSON payloads."""

    @property
    def bronze_path(self) -> str:
        return f"{self.warehouse_root}/bronze/cases"

    @property
    def silver_path(self) -> str:
        return f"{self.warehouse_root}/silver/cases"

    @property
    def gold_path(self) -> str:
        return f"{self.warehouse_root}/gold/cases"

    # ------------------------------------------------------------------
    # BRONZE
    # ------------------------------------------------------------------

    def bronze(self, source_path: str) -> str:
        cases_df = self.spark.read.json(source_path)
        print("[CasesETL] Raw cases read.")

        bronze = (
            cases_df
            .withColumn("case_id",               F.col("case_id").cast("long"))
            .withColumn("parent_id",             F.col("parent_id").cast("long"))
            .withColumn("tenant_id",             F.col("tenant_id").cast("string"))
            .withColumn("case_creation_type",    F.col("case_creation_type").cast("string"))
            .withColumn("case_creator_user_id",  F.col("case_creator_user_id").cast("string"))
            .withColumn("case_owner_user_id",    F.col("case_owner_user_id").cast("string"))
            .withColumn("case_type",             F.col("case_type").cast("string"))
            .withColumn("priority",              F.col("priority").cast("string"))
            .withColumn("status",                F.col("status").cast("string"))
            .withColumn("created_at",            F.col("created_at").cast("string"))
            .withColumn("updated_at",            F.col("updated_at").cast("string"))
            .withColumn("created_at_ts",         F.current_timestamp())
            .withColumn("source_file_path",      F.lit(source_path))
            .withColumn("case_pk",               F.concat_ws("#", F.col("tenant_id"), F.col("case_id")))
        )

        hash_cols = [c for c in bronze.columns if c != "created_at_ts"]
        bronze = bronze.withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in hash_cols]),
                256,
            ),
        )
        bronze = bronze.withColumn("_row_payload_json", F.to_json(F.struct(*[F.col(c) for c in bronze.columns])))

        self.write_hudi(
            bronze,
            self.bronze_path,
            self.hudi_opts("cases", "case_pk", "created_at_ts"),
        )
        print(f"[CasesETL] Bronze written → {self.bronze_path}")
        return self.bronze_path

    # ------------------------------------------------------------------
    # SILVER
    # ------------------------------------------------------------------

    def silver(self) -> str:
        b = self.spark.read.format("hudi").load(self.bronze_path)

        silver = (
            b
            .withColumn("case_id",                    F.col("case_id").cast("long"))
            .withColumn("parent_id",                  F.col("parent_id").cast("long"))
            .withColumn("tenant_id",                  F.col("tenant_id").cast("string"))
            .withColumn("case_creation_type",         F.col("case_creation_type").cast("string"))
            .withColumn("case_creator_user_id",       F.col("case_creator_user_id").cast("string"))
            .withColumn("case_owner_user_id",         F.col("case_owner_user_id").cast("string"))
            .withColumn("case_type",                  F.col("case_type").cast("string"))
            .withColumn("priority",                   F.col("priority").cast("string"))
            .withColumn("status",                     F.col("status").cast("string"))
            .withColumn("created_at_ms",              F.col("created_at").cast("long"))
            .withColumn("updated_at_ms",              F.col("updated_at").cast("long"))
            .withColumn("case_created_ts",            F.to_timestamp((F.col("created_at_ms") / 1000).cast("double")))
            .withColumn("case_updated_ts",            F.to_timestamp((F.col("updated_at_ms") / 1000).cast("double")))
            .withColumn("case_created_date",          F.to_date("case_created_ts"))
            .withColumn("case_updated_date",          F.to_date("case_updated_ts"))
            .withColumn("priority_norm",              F.upper("priority"))
            .withColumn("status_norm",                F.upper("status"))
            .withColumn("case_creation_type_norm",    F.upper("case_creation_type"))
        )

        w = Window.partitionBy("case_id").orderBy(F.col("created_at_ts").desc())
        silver = silver.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

        silver = silver.select(
            "_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key",
            "_hoodie_partition_path", "_hoodie_file_name",
            "case_id", "case_pk", "tenant_id", "parent_id",
            "case_creation_type", "case_creation_type_norm",
            "case_creator_user_id", "case_owner_user_id",
            "case_type", "priority", "priority_norm", "status", "status_norm",
            "created_at", "updated_at", "created_at_ms", "updated_at_ms",
            "case_created_ts", "case_updated_ts", "case_created_date", "case_updated_date",
            "created_at_ts", "source_file_path", "record_hash",
        )

        self.write_hudi(
            silver,
            self.silver_path,
            self.hudi_opts("cases", "case_pk", "created_at_ts"),
        )
        print(f"[CasesETL] Silver written → {self.silver_path}")
        return self.silver_path

    # ------------------------------------------------------------------
    # GOLD
    # ------------------------------------------------------------------

    def gold(self) -> str:
        s = self.spark.read.format("hudi").load(self.silver_path)

        w = Window.partitionBy("case_id").orderBy(F.col("created_at_ts").desc())
        s = s.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

        if "record_hash" not in s.columns:
            s = s.withColumn(
                "record_hash",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.coalesce(F.col("case_id").cast("string"), F.lit("")),
                        F.coalesce(F.col("tenant_id").cast("string"), F.lit("")),
                        F.coalesce(F.col("created_at_ts").cast("string"), F.lit("")),
                    ),
                    256,
                ),
            )

        g = (
            s
            .withColumn("security_tag", F.concat(F.lit("TENANT:"), F.col("tenant_id")))
            .withColumn(
                "case_age_ms_at_ingest",
                F.when(
                    F.col("case_created_ts").isNotNull(),
                    (F.col("created_at_ts").cast("long") - F.col("case_created_ts").cast("long")) * 1000,
                ).otherwise(F.lit(None).cast("long")),
            )
            .withColumn(
                "created_to_updated_ms",
                F.when(
                    F.col("case_created_ts").isNotNull() & F.col("case_updated_ts").isNotNull(),
                    (F.col("case_updated_ts").cast("long") - F.col("case_created_ts").cast("long")) * 1000,
                ).otherwise(F.lit(0).cast("long")),
            )
            .withColumn("has_parent_case", F.when(F.col("parent_id").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
            .withColumn("has_owner",       F.when(F.col("case_owner_user_id").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
        )

        gold = g.select(
            F.col("case_id").cast("long").alias("case_id"),
            F.col("case_pk").cast("string").alias("case_pk"),
            F.col("tenant_id").cast("string").alias("tenant_id"),
            F.col("parent_id").cast("long").alias("parent_id"),
            F.col("case_creation_type_norm").cast("string").alias("case_creation_type"),
            F.col("priority_norm").cast("string").alias("priority"),
            F.col("status_norm").cast("string").alias("status"),
            F.col("case_type").cast("string").alias("case_type"),
            F.col("case_creator_user_id").cast("string").alias("case_creator_user_id"),
            F.col("case_owner_user_id").cast("string").alias("case_owner_user_id"),
            F.col("case_created_ts").cast("timestamp").alias("case_created_ts"),
            F.col("case_updated_ts").cast("timestamp").alias("case_updated_ts"),
            F.col("case_created_date").cast("date").alias("case_created_date"),
            F.col("case_updated_date").cast("date").alias("case_updated_date"),
            F.col("created_at_ts").cast("timestamp").alias("ingested_at_ts"),
            F.col("case_age_ms_at_ingest").cast("long").alias("case_age_ms_at_ingest"),
            F.col("created_to_updated_ms").cast("long").alias("created_to_updated_ms"),
            F.col("has_parent_case").cast("int").alias("has_parent_case"),
            F.col("has_owner").cast("int").alias("has_owner"),
            F.col("security_tag").cast("string").alias("security_tag"),
            F.col("source_file_path").cast("string").alias("source_file_path"),
            F.col("record_hash").cast("string").alias("record_hash"),
        )

        gold_opts = {
            **self.hudi_opts("cases", "case_pk", "ingested_at_ts", partition="case_created_date"),
            "hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
        }
        self.write_hudi(gold, self.gold_path, gold_opts)
        print(f"[CasesETL] Gold written → {self.gold_path}")
        return self.gold_path

    # ------------------------------------------------------------------
    # ORCHESTRATOR
    # ------------------------------------------------------------------

    def run(self, source_path: str) -> str:
        print(f"[CasesETL] Starting Bronze → Silver → Gold from {source_path}")
        self.bronze(source_path)
        self.silver()
        self.gold()
        print("[CasesETL] ETL complete.")
        return self.gold_path