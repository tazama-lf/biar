"""
tasks.py
--------
Bronze → Silver → Gold ETL for the Tasks domain.
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .BaseETL import BaseETL

# Required pacs008-style columns that may be absent in the Tasks silver table.
_PACS008_COLS = {
    "tx_tenant_id": "string",
    "dc_cdtr_id": "string",
    "dc_dbtr_id": "string",
    "dc_cre_dt_tm": "timestamp",
    "dc_instd_amt": "double",
    "dc_instd_ccy": "string",
    "dc_xchg_rate": "string",
    "dc_cdtr_acct_id": "string",
    "dc_dbtr_acct_id": "string",
    "dc_intrbk_amt": "double",
    "dc_intrbk_ccy": "string",
    "grp_msg_id": "string",
    "grp_cre_dt_tm": "timestamp",
    "grp_nb_of_txs": "int",
    "sttlm_mtd": "string",
    "rmt_ustrd": "string",
    "purp_cd": "string",
    "pmt_instr_id": "string",
    "pmt_e2e_id": "string",
    "chrg_br": "string",
    "cdtr_agt_mmb_id": "string",
    "dbtr_agt_mmb_id": "string",
    "cdtr_name": "string",
    "dbtr_name": "string",
    "cdtr_id": "string",
    "dbtr_id": "string",
    "cdtr_acct_scheme": "string",
    "dbtr_acct_scheme": "string",
    "intrbk_amt": "double",
    "intrbk_ccy": "string",
    "xchg_rate": "string",
    "charge_amt": "double",
    "charge_ccy": "string",
    "charge_agent_mmb_id": "string",
    "event_ts": "timestamp",
}


class TasksETL(BaseETL):
    """Full Bronze → Silver → Gold pipeline for raw task JSON payloads."""

    @property
    def bronze_path(self) -> str:
        return f"{self.warehouse_root}/bronze/tasks"

    @property
    def silver_path(self) -> str:
        return f"{self.warehouse_root}/silver/tasks"

    # @property
    # def silver_dlq_path(self) -> str:
    #     return f"{self.warehouse_root}/silver/tasks_dlq"

    @property
    def gold_path(self) -> str:
        return f"{self.warehouse_root}/gold/tasks"

    # ------------------------------------------------------------------
    # BRONZE
    # ------------------------------------------------------------------

    def bronze(self, source_path: str) -> str:
        df = self.spark.read.json(source_path)
        print("[TasksETL] Raw tasks read.")

        bronze = (
            df
            .withColumn("task_id",            F.col("task_id").cast("long"))
            .withColumn("case_id",            F.col("case_id").cast("long"))
            .withColumn("tenant_id",            F.col("tenant_id").cast("long"))
            .withColumn("created_at",         F.col("created_at").cast("string"))
            .withColumn("updated_at",         F.col("updated_at").cast("string"))
            .withColumn("completed_at",       F.col("completed_at").cast("string"))
            .withColumn("sla_deadline",       F.col("sla_deadline").cast("string"))
            .withColumn("sla_duration_hours", F.col("sla_duration_hours").cast("double"))
            .withColumn("assigned_user_id",   F.col("assigned_user_id").cast("string"))
            .withColumn("candidateGroup",     F.col("candidateGroup").cast("string"))
            .withColumn("description",        F.col("description").cast("string"))
            .withColumn("name",               F.col("name").cast("string"))
            .withColumn("status",             F.col("status").cast("string"))
            .withColumn("task_type",          F.col("task_type").cast("string"))
            .withColumn("created_at_ts",      F.current_timestamp())
            .withColumn("source_file_path",   F.lit(source_path))
            .withColumn("task_pk",            F.concat_ws("#", F.col("tenant_id"), F.col("task_id")))
        )

        hash_cols = [c for c in bronze.columns if c != "created_at_ts"]
        bronze = bronze.withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in hash_cols]),
                256,
            ),
        ).withColumn("_row_payload_json", F.to_json(F.struct("*")))

        self.write_hudi(
            bronze,
            self.bronze_path,
            self.hudi_opts("tasks", "task_pk", "created_at_ts"),
        )
        print(f"[TasksETL] Bronze written → {self.bronze_path}")
        return self.bronze_path

    # ------------------------------------------------------------------
    # SILVER
    # ------------------------------------------------------------------

    def silver(self) -> str:
        b = self.spark.read.format("hudi").load(self.bronze_path)

        silver = (
            b
            .withColumn("task_id",             F.col("task_id").cast("long"))
            .withColumn("case_id",             F.col("case_id").cast("long"))
            .withColumn("created_at_ms",       F.col("created_at").cast("long"))
            .withColumn("updated_at_ms",       F.col("updated_at").cast("long"))
            .withColumn("completed_at_ms",     F.col("completed_at").cast("long"))
            .withColumn("sla_deadline_ms",     F.col("sla_deadline").cast("long"))
            .withColumn("task_created_ts",     F.to_timestamp((F.col("created_at_ms")   / 1000).cast("double")))
            .withColumn("task_updated_ts",     F.to_timestamp((F.col("updated_at_ms")   / 1000).cast("double")))
            .withColumn("task_completed_ts",   F.to_timestamp((F.col("completed_at_ms") / 1000).cast("double")))
            .withColumn("sla_deadline_ts",     F.to_timestamp((F.col("sla_deadline_ms") / 1000).cast("double")))
            .withColumn("task_created_date",   F.to_date("task_created_ts"))
            .withColumn("task_updated_date",   F.to_date("task_updated_ts"))
            .withColumn("task_completed_date", F.to_date("task_completed_ts"))
            .withColumn("status_norm",         F.upper("status"))
            .withColumn("task_type_norm",      F.upper("task_type"))
            .withColumn("candidate_group_norm", F.upper("candidateGroup"))
            .withColumn("is_assigned",         F.when(F.col("assigned_user_id").isNotNull(), 1).otherwise(0))
            .withColumn("is_completed",        F.when(F.col("task_completed_ts").isNotNull(), 1).otherwise(0))
            .withColumn(
                "task_age_ms_at_ingest",
                F.when(F.col("task_created_ts").isNotNull(),
                       (F.col("created_at_ts").cast("long") - F.col("task_created_ts").cast("long")) * 1000
                ).otherwise(F.lit(None).cast("long")),
            )
            .withColumn(
                "task_duration_ms",
                F.when(F.col("task_created_ts").isNotNull() & F.col("task_completed_ts").isNotNull(),
                       (F.col("task_completed_ts").cast("long") - F.col("task_created_ts").cast("long")) * 1000
                ).otherwise(F.lit(None).cast("long")),
            )
            .withColumn(
                "sla_remaining_ms",
                F.when(F.col("sla_deadline_ts").isNotNull(),
                       (F.col("sla_deadline_ts").cast("long") - F.col("created_at_ts").cast("long")) * 1000
                ).otherwise(F.lit(None).cast("long")),
            )
            .withColumn(
                "sla_breached",
                F.when(
                    F.col("sla_deadline_ts").isNotNull() & F.col("task_completed_ts").isNotNull(),
                    (F.col("task_completed_ts") > F.col("sla_deadline_ts")).cast("int"),
                ).otherwise(F.lit(0)),
            )
            .withColumn("_row_payload_json", F.to_json(F.struct("*")))
        )

        # Dedup
        w = Window.partitionBy("task_id").orderBy(F.col("created_at_ts").desc())
        silver = silver.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

        # DQ split
        #silver_pass, silver_fail = self._apply_dq(silver)

        # silver_fail = (
        #     silver_fail
        #     .withColumn(
        #         "dlq_id",
        #         F.sha2(
        #             F.coalesce(F.col("record_hash").cast("string"), F.col("_row_payload_json").cast("string")),
        #             256,
        #         ),
        #     )
        #     .withColumn("dlq_ingested_at", F.current_timestamp())
        # )

        self.write_hudi(silver, self.silver_path, self.hudi_opts("tasks", "task_pk", "created_at_ts"))
        print(f"[TasksETL] Silver + DLQ written → {self.silver_path}")
        return self.silver_path

    def _apply_dq(self, silver):
        dq_rules = [
            ("TASK_ID_NULL",         F.col("task_id").isNull()),
            ("CASE_ID_NULL",         F.col("case_id").isNull()),
            ("TASK_CREATED_TS_NULL", F.col("task_created_ts").isNull()),
            ("TASK_UPDATED_TS_NULL", F.col("task_updated_ts").isNull()),
            ("INGEST_TS_NULL",       F.col("created_at_ts").isNull()),
            ("STATUS_NULL",          F.col("status").isNull()),
            ("TASK_TYPE_NULL",       F.col("task_type").isNull()),
        ]
        reason_cols = [
            F.when(cond, F.lit(code)).otherwise(F.lit(None).cast("string"))
            for code, cond in dq_rules
        ]
        dq = (
            silver
            .withColumn("dq_reason_codes_raw", F.array(*reason_cols))
            .withColumn("dq_reason_codes", F.expr("filter(dq_reason_codes_raw, x -> x is not null)"))
            .withColumn("dq_failed", F.size("dq_reason_codes") > 0)
            .drop("dq_reason_codes_raw")
        )
        return (
            dq.filter(~F.col("dq_failed")).drop("dq_failed", "dq_reason_codes"),
            dq.filter(F.col("dq_failed")),
        )

    # ------------------------------------------------------------------
    # GOLD
    # ------------------------------------------------------------------

    def gold(self) -> str:
        s = self.spark.read.format("hudi").load(self.silver_path)
        s = self.ensure_columns(s, _PACS008_COLS)
        s = s.withColumn("tx_tenant_id", F.coalesce(F.col("tx_tenant_id"), F.col("tenant_id").cast("string")))

        if "event_ts" in s.columns and "creation_dt_tm" in s.columns:
            s = s.withColumn("event_ts", F.coalesce(F.col("event_ts"), F.col("creation_dt_tm")))

        w = Window.partitionBy("task_id").orderBy(F.col("created_at_ts").desc())
        s = s.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

        gold = s.select(
            F.col("task_id").cast("long").alias("task_id"),
            F.col("task_pk").cast("string").alias("task_pk"),
            F.col("case_id").cast("long").alias("case_id"),
            F.col("task_type").cast("string").alias("task_type"),
            F.col("candidate_group_norm").cast("string").alias("candidate_group"),
            F.col("assigned_user_id").cast("string").alias("assigned_user_id"),
            F.col("name").cast("string").alias("task_name"),
            F.col("status_norm").cast("string").alias("status"),
            F.col("task_created_ts").cast("timestamp").alias("task_created_ts"),
            F.col("task_updated_ts").cast("timestamp").alias("task_updated_ts"),
            F.col("task_completed_ts").cast("timestamp").alias("task_completed_ts"),
            F.col("sla_deadline_ts").cast("timestamp").alias("sla_deadline_ts"),
            F.col("task_created_date").cast("date").alias("task_created_date"),
            F.col("created_at_ts").cast("timestamp").alias("ingested_at_ts"),
            F.col("sla_duration_hours").cast("double").alias("sla_duration_hours"),
            F.col("is_assigned").cast("int").alias("is_assigned"),
            F.col("is_completed").cast("int").alias("is_completed"),
            F.col("sla_breached").cast("int").alias("sla_breached"),
            F.col("task_age_ms_at_ingest").cast("long").alias("task_age_ms_at_ingest"),
            F.col("task_duration_ms").cast("long").alias("task_duration_ms"),
            F.col("sla_remaining_ms").cast("long").alias("sla_remaining_ms"),
            F.col("source_file_path").cast("string").alias("source_file_path"),
            F.col("record_hash").cast("string").alias("record_hash"),
        )

        gold_opts = {
            **self.hudi_opts("tasks", "task_pk", "ingested_at_ts", partition="task_created_date"),
            "hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
        }
        self.write_hudi(gold, self.gold_path, gold_opts)
        print(f"[TasksETL] Gold written → {self.gold_path}")
        return self.gold_path

    # ------------------------------------------------------------------
    # ORCHESTRATOR
    # ------------------------------------------------------------------

    def run(self, source_path: str) -> str:
        print(f"[TasksETL] Starting Bronze → Silver → Gold from {source_path}")
        self.bronze(source_path)
        self.silver()
        self.gold()
        print("[TasksETL] ETL complete.")
        return self.gold_path