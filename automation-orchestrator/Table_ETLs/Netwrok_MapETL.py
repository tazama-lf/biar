from __future__ import annotations
 
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
 
from .BaseETL import BaseETL
 
 
class NetworkMapETL(BaseETL):
    """Full Bronze → Silver → Gold pipeline for Network Map configs."""
 
    @property
    def bronze_path(self) -> str:
        return f"{self.warehouse_root}/bronze/network_map"
 
    @property
    def silver_path(self) -> str:
        return f"{self.warehouse_root}/silver/network_map"
 
    @property
    def gold_path(self) -> str:
        return f"{self.warehouse_root}/gold/network_map"
 
    def bronze(self, source_path: str) -> str:
        nmap_df = self.spark.read.json(source_path)
        bronze = (
            nmap_df
            .withColumnRenamed("tenantid", "tenant_id")
            .withColumn("configuration", F.col("configuration").cast("string"))
            .withColumn(
                "network_map_id",
                F.sha2(F.concat_ws("||", F.col("tenant_id"), F.col("configuration")), 256),
            )
            .withColumn("created_at_ts",    F.current_timestamp())
            .withColumn("source_file_path", F.input_file_name())
            .withColumn(
                "record_hash",
                F.sha2(F.concat_ws("||", F.col("tenant_id"), F.col("configuration")), 256),
            )
            .withColumn("_row_payload_json", F.to_json(F.struct("*")))
        )
        self.write_hudi(bronze, self.bronze_path, self.hudi_opts("network_map", "network_map_id", "created_at_ts"))
        print(f"[NetworkMapETL] Bronze written → {self.bronze_path}")
        return self.bronze_path
 
    def silver(self) -> str:
        bronze_df = self.spark.read.format("hudi").load(self.bronze_path)
        config_schema = self.infer_json_schema(bronze_df, "configuration")

        w = Window.partitionBy("network_map_id").orderBy(F.col("created_at_ts").desc())
        bronze_df = bronze_df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
 
        silver = (
            bronze_df
            .withColumn("config_obj",     F.from_json("configuration", config_schema))
            .withColumn("network_cfg",    F.col("config_obj.cfg"))
            .withColumn("network_active", F.col("config_obj.active").cast("boolean"))
            .withColumn("message",        F.explode(F.col("config_obj.messages")))
            .withColumn("message_id",     F.col("message.id"))
            .withColumn("message_cfg",    F.col("message.cfg"))
            .withColumn("tx_type",        F.col("message.txTp"))
            .withColumn("typology",       F.explode(F.col("message.typologies")))
            .withColumn("typology_id",    F.col("typology.id"))
            .withColumn("typology_cfg",   F.col("typology.cfg"))
            .withColumn("rule_ids",       F.expr("transform(typology.rules, x -> x.id)"))
            .withColumn("rule_count",     F.size("rule_ids"))
            .select(
                "network_map_id", "tenant_id", "network_cfg", "network_active",
                "message_id", "message_cfg", "tx_type",
                "typology_id", "typology_cfg", "rule_ids", "rule_count",
                "created_at_ts", "record_hash", "source_file_path",
                "credttm", "upddttm",
            )
        )
 
        self.write_hudi(silver, self.silver_path, self.hudi_opts("network_map", "network_map_id", "created_at_ts"))
        print(f"[NetworkMapETL] Silver written → {self.silver_path}")
        return self.silver_path
 
    def gold(self) -> str:
        silver_df = self.spark.read.format("hudi").load(self.silver_path)
 
        gold = (
            silver_df
            .groupBy("tenant_id", "tx_type", "typology_id", "typology_cfg")
            .agg(
                F.max("network_active").alias("network_active"),
                F.sum("rule_count").alias("rule_count"),
                F.countDistinct("message_id").alias("message_count"),
                F.countDistinct("typology_id").alias("typology_count"),
                F.max("created_at_ts").alias("ingested_at_ts"),
                F.max("credttm").alias("credttm"),
                F.max("upddttm").alias("upddttm"),
            )
            .withColumn("network_map_key", F.sha2(F.concat_ws("||", "tenant_id", "tx_type", "typology_id"), 256))
            .withColumn("event_date",      F.to_date("ingested_at_ts"))
            .select(
                "network_map_key", "tenant_id", "tx_type", "typology_id", "typology_cfg",
                "rule_count", "network_active", "message_count", "typology_count",
                "ingested_at_ts", "event_date", "credttm", "upddttm",
            )
        )
 
        gold_opts = {
            **self.hudi_opts("network_map", "network_map_key", "ingested_at_ts", partition="event_date"),
            "hoodie.datasource.write.schema.evolution.enable": "true",
            "hoodie.datasource.write.reconcile.schema": "true",
        }
        self.write_hudi(gold, self.gold_path, gold_opts)
        print(f"[NetworkMapETL] Gold written → {self.gold_path}")
        return self.gold_path
 
    def run(self, source_path: str) -> str:
        print(f"[NetworkMapETL] Starting Bronze → Silver → Gold from {source_path}")
        self.bronze(source_path)
        self.silver()
        self.gold()
        print("[NetworkMapETL] ETL complete.")
        return self.gold_path
