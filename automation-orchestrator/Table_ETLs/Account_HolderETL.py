from __future__ import annotations
 
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
 
from .BaseETL import BaseETL

class AccountHolderETL(BaseETL):
    """Full Bronze → Silver → Gold pipeline for Account Holder (relationships)."""
 
    @property
    def bronze_path(self) -> str:
        return f"{self.warehouse_root}/bronze/account_holder"
 
    @property
    def silver_path(self) -> str:
        return f"{self.warehouse_root}/silver/account_holder"
 
    @property
    def gold_path(self) -> str:
        return f"{self.warehouse_root}/gold/account_holder"
 
    def bronze(self, source_path: str) -> str:
        df = self.spark.read.json(source_path)
        bronze = (
            df
            .withColumn("ingested_at_ts",   F.current_timestamp())
            .withColumn("source_file_path", F.input_file_name())
            .withColumn("_row_payload_json", F.to_json(F.struct("*")))
            .withColumn("record_hash",       F.sha2(F.col("_row_payload_json"), 256))
        )
        self.write_hudi(bronze, self.bronze_path, self.hudi_opts("bronze_account_holder", "record_hash", "ingested_at_ts"))
        print(f"[AccountHolderETL] Bronze written → {self.bronze_path}")
        return self.bronze_path
 
    def silver(self) -> str:
        df = self.spark.read.format("hudi").load(self.bronze_path)
        silver = (
            df
            .withColumn("tenant_id",        F.col("tenantid"))
            .withColumn("event_ts",         F.to_timestamp(F.col("credttm")))
            .withColumn("event_date",       F.to_date(F.to_timestamp(F.col("credttm"))))
            .withColumn("account_id",       F.col("destination"))
            .withColumn("counterparty_id",  F.col("source"))
            .withColumn("pk", F.sha2(F.concat_ws("||",
                F.lit("account_holder"),
                F.coalesce(F.col("tenant_id"),      F.lit("")),
                F.coalesce(F.col("counterparty_id"), F.lit("")),
                F.coalesce(F.col("account_id"),     F.lit("")),
                F.coalesce(F.col("credttm").cast("string"), F.lit("")),
            ), 256))
            .withColumn("ingested_at_ts", F.coalesce(F.col("ingested_at_ts"), F.current_timestamp()))
        )
        self.write_hudi(silver, self.silver_path, self.hudi_opts("silver_account_holder", "pk", "ingested_at_ts"))
        print(f"[AccountHolderETL] Silver written → {self.silver_path}")
        return self.silver_path
 
    def gold(self) -> str:
        df = self.spark.read.format("hudi").load(self.silver_path)
        gold = (
            df
            .withColumn("relationship_type", F.lit("ACCOUNT_HOLDER"))
            .drop("_row_payload_json", "tenantid", "credttm")
        )
        self.write_hudi(gold, self.gold_path, self.hudi_opts("gold_account_holder", "pk", "ingested_at_ts"))
        print(f"[AccountHolderETL] Gold written → {self.gold_path}")
        return self.gold_path
 
    def run(self, source_path: str) -> str:
        print(f"[AccountHolderETL] Starting Bronze → Silver → Gold from {source_path}")
        self.bronze(source_path)
        self.silver()
        self.gold()
        print("[AccountHolderETL] ETL complete.")
        return self.gold_path
