from __future__ import annotations
 
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
 
from .BaseETL import BaseETL


_MAX_BANDS = 20
_MAX_EXITS = 20
 
class RulesETL(BaseETL):
    """Full Bronze → Silver → Gold pipeline for Rule configurations."""
 
    @property
    def bronze_path(self) -> str:
        return f"{self.warehouse_root}/bronze/rules"
 
    @property
    def silver_path(self) -> str:
        return f"{self.warehouse_root}/silver/rules"
 
    @property
    def gold_path(self) -> str:
        return f"{self.warehouse_root}/gold/rules"
 
    def bronze(self, source_path: str) -> str:
        rule_df = self.spark.read.json(source_path)
 
        bronze = (
            rule_df
            .withColumnRenamed("tenantid", "tenant_id")
            .withColumnRenamed("ruleid",   "rule_id")
            .withColumnRenamed("rulecfg",  "rule_cfg")
        )
 
        # Normalize configuration – could be struct or string
        conf_dtype = dict(bronze.dtypes).get("configuration", "")
        if conf_dtype.startswith("string"):
            bronze = bronze.withColumn("configuration_json", F.col("configuration").cast("string"))
        else:
            bronze = bronze.withColumn(
                "configuration_json",
                F.when(F.col("configuration").isNull(), F.lit(None).cast("string"))
                 .otherwise(F.to_json(F.col("configuration"))),
            )
 
        bronze = (
            bronze
            .select("tenant_id", "rule_id", "rule_cfg", "configuration_json")
            .withColumn("created_at_ts",    F.current_timestamp())
            .withColumn("source_file_path", F.lit("api_or_file_ingestion"))
            .withColumn(
                "record_hash",
                F.sha2(F.concat_ws("||",
                    F.coalesce(F.col("tenant_id").cast("string"),          F.lit("")),
                    F.coalesce(F.col("rule_id").cast("string"),            F.lit("")),
                    F.coalesce(F.col("rule_cfg").cast("string"),           F.lit("")),
                    F.coalesce(F.col("configuration_json").cast("string"), F.lit("")),
                ), 256),
            )
            .withColumn("_row_payload_json", F.to_json(F.struct("*")))
        )
 
        self.write_hudi(bronze, self.bronze_path, self.hudi_opts("bronze_rules", "record_hash", "created_at_ts"))
        print(f"[RulesETL] Bronze written → {self.bronze_path}")
        return self.bronze_path
 
    def silver(self) -> str:
        b = self.spark.read.format("hudi").load(self.bronze_path)
        rule_schema = self.spark.read.json(
            b.where(F.col("configuration_json").isNotNull()).select("configuration_json").rdd.map(lambda r: r[0])
        ).schema
 
        s = (
            b
            .withColumn("rule_obj",            F.from_json("configuration_json", rule_schema))
            .withColumn("config_obj",          F.col("rule_obj.config"))
            .withColumn("rule_id_in_json",     F.col("rule_obj.id").cast("string"))
            .withColumn("rule_cfg_in_json",    F.col("rule_obj.cfg").cast("string"))
            .withColumn("rule_desc",           F.col("rule_obj.desc").cast("string"))
            .withColumn("band_count",          F.coalesce(F.size(F.col("config_obj.bands")), F.lit(0)).cast("int"))
            .withColumn("exit_condition_count", F.coalesce(F.size(F.col("config_obj.exitConditions")), F.lit(0)).cast("int"))
            #.withColumn("evaluation_interval_time_ms", F.col("config_obj.parameters").getField("evaluationIntervalTime").cast("long"))
            #.withColumn("tolerance",           F.col("config_obj.parameters.tolerance").cast("double"))
            .withColumn("commission",          F.col("config_obj.parameters.commission").cast("double"))
            .withColumn("max_query_range_ms",  F.col("config_obj.parameters.maxQueryRange").cast("long"))
            .withColumn("config_json",         F.to_json(F.col("rule_obj.config")))
            .withColumn("parameters_json",     F.to_json(F.col("config_obj.parameters")))
            .withColumn("bands_json",          F.to_json(F.col("config_obj.bands")))
            .withColumn("exit_conditions_json", F.to_json(F.col("config_obj.exitConditions")))
            .withColumn("configuration_parsed_json", F.to_json(F.col("rule_obj")))
            .withColumn("pk", F.sha2(F.concat_ws("||",
                F.coalesce(F.col("tenant_id").cast("string"), F.lit("")),
                F.coalesce(F.col("rule_id").cast("string"),   F.lit("")),
                F.coalesce(F.col("rule_cfg").cast("string"),  F.lit("")),
            ), 256))
        )
 
        w = Window.partitionBy("pk").orderBy(F.col("created_at_ts").desc())
        silver = s.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
        silver = silver.select(
            "pk", "tenant_id", "rule_id", "rule_cfg", "rule_desc",
            "band_count", "exit_condition_count", "evaluation_interval_time_ms", "commission", "max_query_range_ms",
            "configuration_json", "configuration_parsed_json", "config_json",
            "parameters_json", "bands_json", "exit_conditions_json",
            "created_at_ts", "source_file_path", "record_hash", "_row_payload_json",
        )
 
        self.write_hudi(silver, self.silver_path, self.hudi_opts("silver_rules", "pk", "created_at_ts"))
        print(f"[RulesETL] Silver written → {self.silver_path}")
        return self.silver_path
 
    def gold(self) -> str:
        s = self.spark.read.format("hudi").load(self.silver_path)
        rule_schema = self.spark.read.json(
            s.where(F.col("configuration_json").isNotNull()).select("configuration_json").rdd.map(lambda r: r[0])
        ).schema
 
        g = (
            s
            .withColumn("rule_obj",    F.from_json("configuration_json", rule_schema))
            .withColumn("config_obj",  F.col("rule_obj.config"))
            .withColumn("bands_arr",   F.col("config_obj.bands"))
            .withColumn("exits_arr",   F.col("config_obj.exitConditions"))
            .withColumn("params",      F.col("config_obj.parameters"))
            .withColumn("pk", F.sha2(F.concat_ws("||",
                F.coalesce(F.col("tenant_id").cast("string"), F.lit("")),
                F.coalesce(F.col("rule_id").cast("string"),   F.lit("")),
                F.coalesce(F.col("rule_cfg").cast("string"),  F.lit("")),
            ), 256))
            .withColumn("rule_desc",                   F.col("rule_obj.desc").cast("string"))
            .withColumn("band_count",                  F.coalesce(F.size("bands_arr"), F.lit(0)).cast("int"))
            .withColumn("exit_condition_count",        F.coalesce(F.size("exits_arr"), F.lit(0)).cast("int"))
            .withColumn("evaluation_interval_time_ms", F.col("params.evaluationIntervalTime").cast("long"))
            #.withColumn("tolerance",                   F.col("params.tolerance").cast("double"))
            .withColumn("commission",                  F.col("params.commission").cast("double"))
            .withColumn("max_query_range_ms",          F.col("params.maxQueryRange").cast("long"))
            .withColumn("tenant_id_norm",              F.upper(F.col("tenant_id")))
            .withColumn("rule_id_norm",                F.upper(F.col("rule_id")))
            .withColumn("rule_cfg_norm",               F.col("rule_cfg").cast("string"))
            .withColumn("ingested_at_ts",              F.current_timestamp())
            .withColumn("as_of_date",                  F.to_date("ingested_at_ts"))
        )
 
        base_cols = [
            F.col("pk"), F.col("tenant_id"), F.col("tenant_id_norm"),
            F.col("rule_id"), F.col("rule_id_norm"), F.col("rule_cfg"), F.col("rule_cfg_norm"),
            F.col("rule_desc"), F.col("band_count"), F.col("exit_condition_count"),
            #F.col("evaluation_interval_time_ms"), F.col("tolerance"), F.col("commission"),
            F.col("max_query_range_ms"), F.col("source_file_path"), F.col("record_hash"),
            F.col("created_at_ts"), F.col("ingested_at_ts"), F.col("as_of_date"),
        ]
 
        band_cols = []
        for i in range(1, _MAX_BANDS + 1):
            p = f"band_{i:02d}"
            band_cols += [
                F.element_at(F.col("bands_arr"), i).getField("reason").cast("string").alias(f"{p}_reason"),
                F.element_at(F.col("bands_arr"), i).getField("subRuleRef").cast("string").alias(f"{p}_sub_rule_ref"),
                F.element_at(F.col("bands_arr"), i).getField("lowerLimit").cast("double").alias(f"{p}_lower_limit"),
                F.element_at(F.col("bands_arr"), i).getField("upperLimit").cast("double").alias(f"{p}_upper_limit"),
            ]
 
        exit_cols = []
        for i in range(1, _MAX_EXITS + 1):
            p = f"exit_{i:02d}"
            exit_cols += [
                F.element_at(F.col("exits_arr"), i).getField("reason").cast("string").alias(f"{p}_reason"),
                F.element_at(F.col("exits_arr"), i).getField("subRuleRef").cast("string").alias(f"{p}_sub_rule_ref"),
            ]
 
        gold_rules = g.select(*(base_cols + band_cols + exit_cols))
 
        bad = [c for c, t in gold_rules.dtypes if t.startswith(("array", "struct"))]
        if bad:
            raise RuntimeError(f"[RulesETL] Gold has non-scalar columns: {bad}")
 
        w = Window.partitionBy("pk").orderBy(F.col("ingested_at_ts").desc())
        gold_rules = gold_rules.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
 
        gold_opts = {
            **self.hudi_opts("rules", "pk", "ingested_at_ts", partition="as_of_date"),
            "hoodie.datasource.write.schema.evolution.enable": "true",
            "hoodie.datasource.write.reconcile.schema": "true",
        }
        self.write_hudi(gold_rules, self.gold_path, gold_opts)
        print(f"[RulesETL] Gold written → {self.gold_path}")
        return self.gold_path
 
    def run(self, source_path: str) -> str:
        print(f"[RulesETL] Starting Bronze → Silver → Gold from {source_path}")
        self.bronze(source_path)
        self.silver()
        self.gold()
        print("[RulesETL] ETL complete.")
        return self.gold_path
