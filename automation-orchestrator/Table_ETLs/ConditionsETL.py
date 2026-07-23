from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from .BaseETL import BaseETL


class ConditionsETL(BaseETL):
    """Full Bronze → Silver → Gold pipeline for Conditions (entity + account)."""

    @property
    def bronze_path(self) -> str:
        return f"{self.warehouse_root}/bronze/condition"

    @property
    def silver_path(self) -> str:
        return f"{self.warehouse_root}/silver/condition"

    @property
    def gold_path(self) -> str:
        return f"{self.warehouse_root}/gold/condition"

    # ------------------------------------------------------------------
    # BRONZE
    # ------------------------------------------------------------------

    def bronze(self, source_path: str) -> str:
        df = self.spark.read.json(source_path)

        # Normalize tenant_id column name variants
        for src in ("tenantid", "tenantId"):
            if src in df.columns and "tenant_id" not in df.columns:
                df = df.withColumnRenamed(src, "tenant_id")

        bronze = (
            df
            .withColumn("condition", F.col("condition").cast("string"))
            .withColumn("id",        F.col("id").cast("string") if "id" in df.columns else F.lit(None).cast("string"))
            .withColumn("tenant_id", F.col("tenant_id").cast("string") if "tenant_id" in df.columns else F.lit(None).cast("string"))
            .withColumn("created_at_ts", F.current_timestamp())
            .withColumn(
                "record_hash",
                F.sha2(F.concat_ws("||",
                    F.coalesce(F.col("condition"), F.lit("")),
                    F.coalesce(F.col("id"),        F.lit("")),
                    F.coalesce(F.col("tenant_id"), F.lit("")),
                ), 256),
            )
            .withColumn("_row_payload_json", F.to_json(F.struct("*")))
        )

        self.write_hudi(bronze, self.bronze_path, self.hudi_opts("bronze_condition", "record_hash", "created_at_ts"))
        print(f"[ConditionsETL] Bronze written → {self.bronze_path}")
        return self.bronze_path

    # ------------------------------------------------------------------
    # SILVER
    # ------------------------------------------------------------------

    def silver(self) -> str:
        bronze_df = self.spark.read.format("hudi").load(self.bronze_path)

        # --- Core extractors ---
        cond_id       = F.coalesce(F.col("id"), F.get_json_object("condition", "$.condId"))
        tenant_id_eff = F.coalesce(F.col("tenant_id"), F.get_json_object("condition", "$.tenantId"))
        usr           = F.get_json_object("condition", "$.usr")

        # --- ACCOUNT (acct) ---
        acct_id     = F.get_json_object("condition", "$.acct.id")
        acct_scheme = F.get_json_object("condition", "$.acct.schmeNm.prtry")
        acct_mmb_id = F.get_json_object("condition", "$.acct.agt.finInstnId.clrSysMmbId.mmbId")

        # --- ENTITY (ntty) ---
        ntty_id     = F.get_json_object("condition", "$.ntty.id")
        ntty_scheme = F.get_json_object("condition", "$.ntty.schmeNm.prtry")

        # --- Target type + condition_key_key ---
        target_type = (
            F.when(ntty_id.isNotNull(), F.lit("ENTITY"))
             .when(acct_id.isNotNull(), F.lit("ACCOUNT"))
             .otherwise(F.lit(None).cast("string"))
        )

        condition_key_key = (
            F.when(target_type == "ENTITY",
                   F.concat(F.coalesce(ntty_id, F.lit("")), F.coalesce(ntty_scheme, F.lit(""))))
             .when(target_type == "ACCOUNT",
                   F.concat(F.coalesce(acct_id, F.lit("")), F.coalesce(acct_scheme, F.lit("")), F.coalesce(acct_mmb_id, F.lit(""))))
             .otherwise(F.lit(None).cast("string"))
        )

        # --- Remaining field extractors ---
        cond_tp    = F.get_json_object("condition", "$.condTp")
        prsptv     = F.get_json_object("condition", "$.prsptv")
        cond_rsn   = F.get_json_object("condition", "$.condRsn")
        force_cret = F.get_json_object("condition", "$.forceCret").cast("boolean")
        cre_dt_tm  = F.to_timestamp(F.get_json_object("condition", "$.creDtTm"))
        inc_dt_tm  = F.to_timestamp(F.get_json_object("condition", "$.incptnDtTm"))
        xpr_dt_tm  = F.to_timestamp(F.get_json_object("condition", "$.xprtnDtTm"))

        evt_tp_json  = F.get_json_object("condition", "$.evtTp")
        evt_tp_count = F.coalesce(
            F.size(F.from_json(evt_tp_json, T.ArrayType(T.StringType()))),
            F.lit(0),
        ).cast("int")

        # --- PK includes ntty support ---
        pk = F.sha2(F.concat_ws("||",
            F.coalesce(cond_id,       F.lit("")),
            F.coalesce(tenant_id_eff, F.lit("")),
            F.coalesce(acct_id,       F.lit("")),
            F.coalesce(ntty_id,       F.lit("")),
            F.coalesce(cond_tp,       F.lit("")),
            F.coalesce(F.col("record_hash"), F.lit("")),
        ), 256)

        silver = (
            bronze_df
            .withColumn("condition_id",      cond_id)
            .withColumn("tenant_id",         tenant_id_eff)
            .withColumn("usr",               usr)
            .withColumn("entity_id",         ntty_id)
            .withColumn("acct_id",           acct_id)
            .withColumn("acct_scheme",       acct_scheme)
            .withColumn("acct_mmb_id",       acct_mmb_id)
            .withColumn("ntty_scheme",       ntty_scheme)
            .withColumn("target_type",       target_type)
            .withColumn("condition_key_key", condition_key_key)
            .withColumn("evt_tp_json",       evt_tp_json)
            .withColumn("evt_tp_count",      evt_tp_count)
            .withColumn("cond_type",         cond_tp)
            .withColumn("perspective",       prsptv)
            .withColumn("condition_reason",  cond_rsn)
            .withColumn("force_create",      force_cret)
            .withColumn("created_dt_ts",     cre_dt_tm)
            .withColumn("inception_dt_ts",   inc_dt_tm)
            .withColumn("expiry_dt_ts",      xpr_dt_tm)
            .withColumn("created_date",      F.to_date("created_dt_ts"))
            .withColumn("pk",                pk)
            .select(
                "_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key",
                "_hoodie_partition_path", "_hoodie_file_name",
                "pk", "condition_id", "tenant_id", "usr",
                "entity_id", "acct_id", "acct_scheme", "acct_mmb_id", "ntty_scheme",
                "target_type", "condition_key_key",
                "evt_tp_json", "evt_tp_count",
                "cond_type", "perspective", "condition_reason", "force_create",
                "created_dt_ts", "inception_dt_ts", "expiry_dt_ts", "created_date",
                "created_at_ts", "condition",
            )
            .withColumn("_row_payload_json", F.to_json(F.struct("*")))
        )

        w = Window.partitionBy("pk").orderBy(F.col("created_at_ts").desc_nulls_last())
        silver = silver.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

        self.write_hudi(silver, self.silver_path, self.hudi_opts("silver_condition", "pk", "created_at_ts"))
        print(f"[ConditionsETL] Silver written → {self.silver_path}")
        return self.silver_path

    # ------------------------------------------------------------------
    # GOLD
    # ------------------------------------------------------------------

    def gold(self) -> str:
        silver_df = self.spark.read.format("hudi").load(self.silver_path)

        evt_tp_csv = F.concat_ws(",", F.from_json(F.col("evt_tp_json"), T.ArrayType(T.StringType())))

        gold = (
            silver_df
            .withColumn("evt_types",        evt_tp_csv)
            .withColumn("evt_type_primary", F.element_at(F.split(evt_tp_csv, ","), 1))
            .withColumn("is_expired", F.when(
                F.col("expiry_dt_ts").isNotNull() & (F.col("expiry_dt_ts") < F.current_timestamp()),
                F.lit(1)).otherwise(F.lit(0)))
            .withColumn("is_active", F.when(
                F.col("expiry_dt_ts").isNull() | (F.col("expiry_dt_ts") >= F.current_timestamp()),
                F.lit(1)).otherwise(F.lit(0)))
            .select(
                "pk",
                F.col("condition_id").cast("string").alias("condition_id"),
                F.col("tenant_id").cast("string").alias("tenant_id"),
                F.col("usr").cast("string").alias("created_by_user"),
                F.col("entity_id").cast("string").alias("entity_id"),
                F.col("acct_id").cast("string").alias("account_id"),
                F.col("acct_scheme").cast("string").alias("account_scheme"),
                F.col("acct_mmb_id").cast("string").alias("account_agent_mmb_id"),
                F.col("target_type").cast("string").alias("target_type"),
                F.col("condition_key_key").cast("string").alias("condition_key_key"),
                F.col("ntty_scheme").cast("string").alias("entity_scheme"),
                F.col("evt_types").cast("string").alias("event_types_csv"),
                F.col("evt_type_primary").cast("string").alias("event_type_primary"),
                F.col("evt_tp_count").cast("int").alias("event_type_count"),
                F.col("cond_type").cast("string").alias("condition_type"),
                F.col("perspective").cast("string").alias("perspective"),
                F.col("condition_reason").cast("string").alias("condition_reason"),
                F.col("force_create").cast("boolean").alias("force_create"),
                F.col("created_dt_ts").cast("timestamp").alias("condition_created_ts"),
                F.col("inception_dt_ts").cast("timestamp").alias("condition_inception_ts"),
                F.col("expiry_dt_ts").cast("timestamp").alias("condition_expiry_ts"),
                F.col("created_date").cast("date").alias("condition_created_date"),
                F.col("is_active").cast("int").alias("is_active"),
                F.col("is_expired").cast("int").alias("is_expired"),
                F.col("created_at_ts").cast("timestamp").alias("ingested_at_ts"),
            )
        )

        bad = [c for c, t in gold.dtypes if t.startswith(("array", "struct"))]
        if bad:
            raise RuntimeError(f"[ConditionsETL] Gold contains non-scalar columns: {bad}")

        self.write_hudi(gold, self.gold_path, self.hudi_opts("condition", "pk", "ingested_at_ts"))
        print(f"[ConditionsETL] Gold written → {self.gold_path}")
        return self.gold_path

    # ------------------------------------------------------------------
    # ORCHESTRATOR
    # ------------------------------------------------------------------

    def run(self, source_path: str) -> str:
        print(f"[ConditionsETL] Starting Bronze → Silver → Gold from {source_path}")
        self.bronze(source_path)
        self.silver()
        self.gold()
        print("[ConditionsETL] ETL complete.")
        return self.gold_path
