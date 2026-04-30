from __future__ import annotations
 
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
 
from .BaseETL import BaseETL

class AccountETL(BaseETL):
    """Full Bronze → Silver → Gold pipeline for Account data."""
 
    @property
    def bronze_path(self) -> str:
        return f"{self.warehouse_root}/bronze/account"
 
    @property
    def silver_path(self) -> str:
        return f"{self.warehouse_root}/silver/account"
 
    @property
    def gold_path(self) -> str:
        return f"{self.warehouse_root}/gold/account"
 
    def bronze(self, source_path: str) -> str:
        if source_path.lower().endswith(".json"):
            df = self.spark.read.json(source_path)
        else:
            df = (
                self.spark.read
                .option("header", True)
                .option("escape", '"')
                .option("multiLine", True)
                .csv(source_path)
            )
 
        bronze = (
            df
            .withColumnRenamed("id",       "account_id")
            .withColumnRenamed("tenantid", "tenant_id")
            .withColumn("ingested_at_ts", F.current_timestamp())
            .withColumn(
                "record_hash",
                F.sha2(F.concat_ws("||",
                    F.coalesce(F.col("account_id"), F.lit("")),
                    F.coalesce(F.col("tenant_id"),  F.lit("")),
                ), 256),
            )
        )
 
        self.write_hudi(bronze, self.bronze_path, self.hudi_opts("bronze_account", "record_hash", "ingested_at_ts"))
        print(f"[AccountETL] Bronze written → {self.bronze_path}")
        return self.bronze_path
 
    def silver(self) -> str:
        df = self.spark.read.format("hudi").load(self.bronze_path)
        silver = df.withColumn("ingested_at_ts", F.current_timestamp())
        self.write_hudi(silver, self.silver_path, self.hudi_opts("silver_account", "record_hash", "ingested_at_ts"))
        print(f"[AccountETL] Silver written → {self.silver_path}")
        return self.silver_path
 
    def gold(self) -> str:
        df = self.spark.read.format("hudi").load(self.silver_path)
        gold = (
            df
            .withColumn("pk", F.sha2(F.concat_ws("||",
                F.lit("account"),
                F.coalesce(F.col("account_id"), F.lit("")),
                F.coalesce(F.col("tenant_id"),  F.lit("")),
            ), 256))
            .withColumn("ingested_at_ts", F.current_timestamp())
        )
        self.write_hudi(gold, self.gold_path, self.hudi_opts("gold_account", "pk", "ingested_at_ts"))
        print(f"[AccountETL] Gold written → {self.gold_path}")
        return self.gold_path
 
    def run(self, source_path: str) -> str:
        print(f"[AccountETL] Starting Bronze → Silver → Gold from {source_path}")
        self.bronze(source_path)
        self.silver()
        self.gold()
        print("[AccountETL] ETL complete.")
        return self.gold_path
