from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .BaseETL import BaseETL


class RulesETL(BaseETL):
    """Full Bronze → Silver → Gold pipeline for Rule data."""

    @property
    def bronze_path(self) -> str:
        return f"{self.warehouse_root}/bronze/rule"

    @property
    def silver_path(self) -> str:
        return f"{self.warehouse_root}/silver/rule"

    @property
    def gold_path(self) -> str:
        return f"{self.warehouse_root}/gold/rule"

    # ------------------------------------------------------------------
    # BRONZE
    # ------------------------------------------------------------------

    def bronze(self, source_path: str) -> str:
        raw = self.spark.read.json(source_path)

        bronze = (
            raw
            .withColumnRenamed("ruleid",   "rule_id")
            .withColumnRenamed("rulecfg",  "rule_cfg")
            .withColumnRenamed("tenantid", "tenant_id")
            .withColumn("configuration",    F.col("configuration").cast("string"))
            .withColumn("credttm",          F.col("credttm").cast("string"))
            .withColumn("upddttm",          F.col("upddttm").cast("string"))
            .withColumn("ingested_at_ts",   F.current_timestamp())
            .withColumn("source_file_path", F.input_file_name())
            .withColumn(
                "record_hash",
                F.sha2(
                    F.concat_ws("||",
                        F.coalesce(F.col("rule_id"),       F.lit("")),
                        F.coalesce(F.col("rule_cfg"),      F.lit("")),
                        F.coalesce(F.col("tenant_id"),     F.lit("")),
                        F.coalesce(F.col("configuration"), F.lit("")),
                    ),
                    256,
                ),
            )
        )
        # _row_payload_json must be added after renames are resolved so
        # bronze.columns reflects the new names, not the raw source names.
        bronze = bronze.withColumn(
            "_row_payload_json",
            F.to_json(F.struct(*[F.col(c) for c in bronze.columns])),
        )

        self.write_hudi(
            bronze,
            self.bronze_path,
            self.hudi_opts("bronze_rule", "record_hash", "ingested_at_ts"),
        )
        print(f"[RuleETL] Bronze written → {self.bronze_path}")
        return self.bronze_path

    # ------------------------------------------------------------------
    # SILVER
    # ------------------------------------------------------------------

    def silver(self) -> str:
        bronze_in = self.spark.read.format("hudi").load(self.bronze_path)

        config_schema = self.infer_json_schema(bronze_in, "configuration")
        b = bronze_in.withColumn("cfg_obj", F.from_json("configuration", config_schema))

        # --- top-level config fields ---
        rule_id_in_json  = F.col("cfg_obj.id").cast("string")
        rule_cfg_in_json = F.col("cfg_obj.cfg").cast("string")
        rule_desc        = F.col("cfg_obj.desc").cast("string")
        rule_tenant_id   = F.col("cfg_obj.tenantId").cast("string")

        # --- timestamps: top-level columns first, embedded JSON as fallback ---
        rule_created_ts = F.coalesce(
            F.to_timestamp(F.col("credttm")),
            F.to_timestamp(F.col("cfg_obj.creDtTm")),
        )
        rule_updated_ts = F.coalesce(
            F.to_timestamp(F.col("upddttm")),
            F.to_timestamp(F.col("cfg_obj.updDtTm")),
        )

        # --- parameters ---
        tolerance       = F.col("cfg_obj.config.parameters.tolerance").cast("double")
        max_query_range = F.col("cfg_obj.config.parameters.maxQueryRange").cast("long")

        # --- counts ---
        band_count           = F.coalesce(F.size(F.col("cfg_obj.config.bands")),          F.lit(0)).cast("int")
        exit_condition_count = F.coalesce(F.size(F.col("cfg_obj.config.exitConditions")), F.lit(0)).cast("int")

        # --- cases block (present in rules like country-case, taxcase) ---
        has_cases_block = (
            F.when(F.col("cfg_obj.config.cases").isNotNull(), F.lit(1))
             .otherwise(F.lit(0))
             .cast("int")
        )

        # --- serialise sub-structures as JSON strings ---
        bands_json           = F.to_json(F.col("cfg_obj.config.bands"))
        exit_conditions_json = F.to_json(F.col("cfg_obj.config.exitConditions"))
        parameters_json      = F.to_json(F.col("cfg_obj.config.parameters"))
        cases_json           = F.to_json(F.col("cfg_obj.config.cases"))

        # --- composite PK: tenant_id + rule_id + rule_cfg ---
        pk = F.sha2(
            F.concat_ws("||",
                F.coalesce(F.col("tenant_id"), F.lit("")),
                F.coalesce(F.col("rule_id"),   F.lit("")),
                F.coalesce(F.col("rule_cfg"),  F.lit("")),
            ),
            256,
        )

        silver = (
            b
            .withColumn("rule_id_in_json",      rule_id_in_json)
            .withColumn("rule_cfg_in_json",      rule_cfg_in_json)
            .withColumn("rule_desc",             rule_desc)
            .withColumn("rule_tenant_id",        rule_tenant_id)
            .withColumn("rule_created_ts",       rule_created_ts)
            .withColumn("rule_updated_ts",       rule_updated_ts)
            .withColumn("rule_created_date",     F.to_date(rule_created_ts))
            .withColumn("rule_updated_date",     F.to_date(rule_updated_ts))
            .withColumn("tolerance",             tolerance)
            .withColumn("max_query_range_ms",    max_query_range)
            .withColumn("band_count",            band_count)
            .withColumn("exit_condition_count",  exit_condition_count)
            .withColumn("has_cases_block",       has_cases_block)
            .withColumn("bands_json",            bands_json)
            .withColumn("exit_conditions_json",  exit_conditions_json)
            .withColumn("parameters_json",       parameters_json)
            .withColumn("cases_json",            cases_json)
            .withColumn("pk",                    pk)
            .withColumn("ingested_at_ts",        F.coalesce(F.col("ingested_at_ts"), F.current_timestamp()))
            # Drop bronze-only and intermediate columns
            .drop("_row_payload_json", "cfg_obj", "credttm", "upddttm")
        )

        # Dedup — keep latest per pk
        w = Window.partitionBy("pk").orderBy(F.col("ingested_at_ts").desc())
        silver = silver.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

        self.write_hudi(
            silver,
            self.silver_path,
            self.hudi_opts("silver_rule", "pk", "ingested_at_ts"),
        )
        print(f"[RuleETL] Silver written → {self.silver_path}")
        return self.silver_path

    # ------------------------------------------------------------------
    # GOLD
    # ------------------------------------------------------------------

    def gold(self) -> str:
        silver_in = self.spark.read.format("hudi").load(self.silver_path)

        gold = (
            silver_in
            .withColumn("tenant_id_norm", F.upper(F.col("tenant_id")))
            .withColumn("rule_id_norm",   F.upper(F.col("rule_id")))
            .withColumn(
                "has_tolerance",
                F.when(F.col("tolerance").isNotNull(), F.lit(1))
                 .otherwise(F.lit(0))
                 .cast("int"),
            )
            .withColumn(
                "has_max_query_range",
                F.when(F.col("max_query_range_ms").isNotNull(), F.lit(1))
                 .otherwise(F.lit(0))
                 .cast("int"),
            )
            .withColumn(
                "rule_type",
                F.when(F.col("has_cases_block") == 1, F.lit("CASE"))
                 .when(F.col("band_count") > 0,       F.lit("BAND"))
                 .otherwise(F.lit("OTHER"))
                 .cast("string"),
            )
            .select(
                F.col("pk").cast("string").alias("pk"),
                F.col("tenant_id").cast("string").alias("tenant_id"),
                F.col("tenant_id_norm").cast("string").alias("tenant_id_norm"),
                F.col("rule_id").cast("string").alias("rule_id"),
                F.col("rule_id_norm").cast("string").alias("rule_id_norm"),
                F.col("rule_cfg").cast("string").alias("rule_cfg"),
                F.col("rule_id_in_json").cast("string").alias("rule_id_in_json"),
                F.col("rule_cfg_in_json").cast("string").alias("rule_cfg_in_json"),
                F.col("rule_desc").cast("string").alias("rule_desc"),
                F.col("rule_tenant_id").cast("string").alias("rule_tenant_id"),
                F.col("rule_type").cast("string").alias("rule_type"),
                F.col("rule_created_ts").cast("timestamp").alias("rule_created_ts"),
                F.col("rule_updated_ts").cast("timestamp").alias("rule_updated_ts"),
                F.col("rule_created_date").cast("date").alias("rule_created_date"),
                F.col("rule_updated_date").cast("date").alias("rule_updated_date"),
                F.col("band_count").cast("int").alias("band_count"),
                F.col("exit_condition_count").cast("int").alias("exit_condition_count"),
                F.col("has_cases_block").cast("int").alias("has_cases_block"),
                F.col("has_tolerance").cast("int").alias("has_tolerance"),
                F.col("has_max_query_range").cast("int").alias("has_max_query_range"),
                F.col("tolerance").cast("double").alias("tolerance"),
                F.col("max_query_range_ms").cast("long").alias("max_query_range_ms"),
                F.col("bands_json").cast("string").alias("bands_json"),
                F.col("exit_conditions_json").cast("string").alias("exit_conditions_json"),
                F.col("parameters_json").cast("string").alias("parameters_json"),
                F.col("cases_json").cast("string").alias("cases_json"),
                F.col("source_file_path").cast("string").alias("source_file_path"),
                F.col("record_hash").cast("string").alias("record_hash"),
                F.col("ingested_at_ts").cast("timestamp").alias("ingested_at_ts"),
            )
        )

        bad = [c for c, t in gold.dtypes if t.startswith(("array", "struct", "map"))]
        if bad:
            raise RuntimeError(f"[RuleETL] Gold has non-scalar columns: {bad}")

        gold_opts = {
            **self.hudi_opts("gold_rule", "pk", "ingested_at_ts", partition="rule_created_date"),
            "hoodie.datasource.write.payload.class":
                "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
        }
        self.write_hudi(gold, self.gold_path, gold_opts)
        print(f"[RuleETL] Gold written → {self.gold_path}")
        return self.gold_path

    # ------------------------------------------------------------------
    # ORCHESTRATOR
    # ------------------------------------------------------------------

    def run(self, source_path: str) -> str:
        print(f"[RuleETL] Starting Bronze → Silver → Gold from {source_path}")
        self.bronze(source_path)
        self.silver()
        self.gold()
        print("[RuleETL] ETL complete.")
        return self.gold_path