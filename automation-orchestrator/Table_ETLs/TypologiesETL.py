from __future__ import annotations
 
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
 
from .BaseETL import BaseETL

class TypologiesETL(BaseETL):
    """Full Bronze → Silver → Gold pipeline for Typology configurations."""
 
    @property
    def bronze_path(self) -> str:
        return f"{self.warehouse_root}/bronze/typologies"
 
    @property
    def silver_path(self) -> str:
        return f"{self.warehouse_root}/silver/typologies"
 
    @property
    def gold_path(self) -> str:
        return f"{self.warehouse_root}/gold/typologies"
 
    def bronze(self, source_path: str) -> str:
        typ_df = self.spark.read.json(source_path)
 
        bronze = (
            typ_df
            .withColumnRenamed("tenantid",    "tenant_id")
            .withColumnRenamed("typologycfg", "typology_cfg")
            .withColumnRenamed("typologyid",  "typology_id")
            .withColumn("configuration_json", F.col("configuration").cast("string"))
            .withColumn("ingested_at_ts",     F.current_timestamp())
            .withColumn("source_file_path",   F.lit(None).cast("string"))
        )
        bronze = bronze.withColumn(
            "record_hash",
            F.sha2(F.concat_ws("||",
                F.coalesce(F.col("tenant_id"),          F.lit("")),
                F.coalesce(F.col("typology_id"),        F.lit("")),
                F.coalesce(F.col("typology_cfg"),       F.lit("")),
                F.coalesce(F.col("configuration_json"), F.lit("")),
            ), 256),
        ).withColumn("_row_payload_json", F.to_json(F.struct("*")))
 
        self.write_hudi(bronze, self.bronze_path, self.hudi_opts("bronze_typologies", "record_hash", "ingested_at_ts"))
        print(f"[TypologiesETL] Bronze written → {self.bronze_path}")
        return self.bronze_path
 
    def silver(self) -> str:
        b = self.spark.read.format("hudi").load(self.bronze_path)
        typ_schema = self.spark.read.json(
            b.select("configuration_json").where(F.col("configuration_json").isNotNull()).rdd.map(lambda r: r[0])
        ).schema
 
        silver = (
            b
            .withColumn("typology_obj",       F.from_json(F.col("configuration_json"), typ_schema))
            .withColumn("typology_id_in_json", F.col("typology_obj.id"))
            .withColumn("typology_cfg_in_json", F.col("typology_obj.cfg"))
            #.withColumn("typology_desc",       F.col("typology_obj").getField("desc"))
            .withColumn("typology_name",       F.col("typology_obj.typology_name"))
            .withColumn("flow_processor",      F.col("typology_obj.workflow.flowProcessor"))
            .withColumn("alert_threshold",     F.col("typology_obj.workflow.alertThreshold").cast("int"))
            .withColumn("interdiction_threshold", F.col("typology_obj.workflow.interdictionThreshold").cast("int"))
            .withColumn("rule_count",          F.size(F.col("typology_obj.rules")).cast("int"))
            .withColumn("expression_count",    F.size(F.col("typology_obj.expression")).cast("int"))
            .withColumn(
                "pk",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.coalesce(F.col("tenant_id"), F.lit("")),
                        F.coalesce(F.col("typology_id"), F.lit("")),
                        F.coalesce(F.col("typology_cfg"), F.lit("")),
                    ),
                    256,
                ),
            )
            .withColumn("_row_payload_json",   F.to_json(F.struct("*")))
        )
 
        w = Window.partitionBy("tenant_id", "typology_id", "typology_cfg").orderBy(F.col("ingested_at_ts").desc_nulls_last())
        silver = silver.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
 
        self.write_hudi(silver, self.silver_path, self.hudi_opts("silver_typologies", "pk", "ingested_at_ts"))
        print(f"[TypologiesETL] Silver written → {self.silver_path}")
        return self.silver_path
 
    def gold(self) -> str:
        s = self.spark.read.format("hudi").load(self.silver_path)
 
        max_rules = s.select(F.max(F.size(F.col("typology_obj.rules"))).alias("m")).collect()[0]["m"] or 0
        max_expr  = s.select(F.max(F.size(F.col("typology_obj.expression"))).alias("m")).collect()[0]["m"] or 0
        max_wghts = (
            s.select(F.explode_outer(F.col("typology_obj.rules")).alias("r"))
             .select(F.max(F.size(F.col("r.wghts"))).alias("m"))
             .collect()[0]["m"] or 0
        )
 
        wide_cols = []
        for i in range(1, max_rules + 1):
            r = F.element_at(F.col("typology_obj.rules"), i)
            wide_cols += [
                r.getField("id").cast("string").alias(f"rule_{i:03d}_id"),
                r.getField("cfg").cast("string").alias(f"rule_{i:03d}_cfg"),
                r.getField("termId").cast("string").alias(f"rule_{i:03d}_term_id"),
                F.size(r.getField("wghts")).cast("int").alias(f"rule_{i:03d}_weight_count"),
            ]
            for j in range(1, max_wghts + 1):
                wj = F.element_at(r.getField("wghts"), j)
                wide_cols += [
                    wj.getField("ref").cast("string").alias(f"rule_{i:03d}_w{j:02d}_ref"),
                    wj.getField("wght").cast("string").cast("double").cast("long").alias(f"rule_{i:03d}_w{j:02d}_wght"),
                ]
 
        for k in range(1, max_expr + 1):
            wide_cols.append(F.element_at(F.col("typology_obj.expression"), k).cast("string").alias(f"expr_{k:03d}_token"))
 
        gold_pk = F.sha2(F.concat_ws("||",
            F.lit("gold_typology"),
            F.coalesce(F.col("tenant_id"),    F.lit("")),
            F.coalesce(F.col("typology_id"),  F.lit("")),
            F.coalesce(F.col("typology_cfg"), F.lit("")),
            F.coalesce(F.col("record_hash"),  F.lit("")),
        ), 256)
 
        gold = s.withColumn("pk", gold_pk).select(
            "pk",
            F.col("tenant_id").cast("string"),
            F.col("typology_id").cast("string"),
            F.col("typology_cfg").cast("string"),
            F.col("typology_id_in_json").cast("string"),
            F.col("typology_cfg_in_json").cast("string"),
            F.col("typology_desc").cast("string"),
            F.col("typology_name").cast("string"),
            F.col("flow_processor").cast("string"),
            F.col("alert_threshold").cast("int"),
            F.col("interdiction_threshold").cast("int"),
            F.col("rule_count").cast("int"),
            F.col("expression_count").cast("int"),
            F.col("ingested_at_ts").cast("timestamp"),
            *wide_cols,
        )
 
        bad = [c for c, t in gold.dtypes if t.startswith(("array", "struct", "map"))]
        if bad:
            raise RuntimeError(f"[TypologiesETL] GOLD has non-scalar columns: {bad}")
 
        self.write_hudi(gold, self.gold_path, self.hudi_opts("typologies", "pk", "ingested_at_ts"))
        print(f"[TypologiesETL] Gold written → {self.gold_path}")
        return self.gold_path
 
    def run(self, source_path: str) -> str:
        print(f"[TypologiesETL] Starting Bronze → Silver → Gold from {source_path}")
        self.bronze(source_path)
        self.silver()
        self.gold()
        print("[TypologiesETL] ETL complete.")
        return self.gold_path
