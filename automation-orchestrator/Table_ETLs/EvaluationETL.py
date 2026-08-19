"""
evaluation.py
-------------
Bronze → Silver → Gold ETL for the Evaluation domain.

Raw source columns:
  credttm    - epoch ms (nullable / consistently null in practice)
  evaluation - large JSON blob (report, dataCache, networkMap, transaction, transactionID, evaluationID)
  messageid  - transaction message identifier (primary key)
  tenantid   - tenant identifier
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .BaseETL import BaseETL


class EvaluationETL(BaseETL):
    """Full Bronze → Silver → Gold pipeline for Evaluation data."""

    @property
    def bronze_path(self) -> str:
        return f"{self.warehouse_root}/bronze/evaluation"

    @property
    def silver_path(self) -> str:
        return f"{self.warehouse_root}/silver/evaluation"

    @property
    def gold_path(self) -> str:
        return f"{self.warehouse_root}/gold/evaluation"

    # ------------------------------------------------------------------
    # BRONZE
    # ------------------------------------------------------------------

    def bronze(self, source_path: str) -> str:
        raw = self.spark.read.json(source_path)

        bronze = (
            raw
            .withColumn("credttm",          F.col("credttm").cast("long"))
            .withColumn("messageid",         F.col("messageid").cast("string"))
            .withColumn("tenantid",          F.col("tenantid").cast("string"))
            .withColumn("evaluation",        F.col("evaluation").cast("string"))
            .withColumn("ingested_at_ts",    F.current_timestamp())
            .withColumn("source_file_path",  F.input_file_name())
            .withColumn(
                "record_hash",
                F.sha2(
                    F.concat_ws("||",
                        F.coalesce(F.col("messageid"),  F.lit("")),
                        F.coalesce(F.col("tenantid"),   F.lit("")),
                        F.coalesce(F.col("evaluation"), F.lit("")),
                    ),
                    256,
                ),
            )
            .withColumn("_row_payload_json", F.to_json(F.struct(*[F.col(c) for c in raw.columns])))
        )

        self.write_hudi(
            bronze,
            self.bronze_path,
            self.hudi_opts("bronze_evaluation", "messageid", "ingested_at_ts"),
        )
        print(f"[EvaluationETL] Bronze written → {self.bronze_path}")
        return self.bronze_path

    # ------------------------------------------------------------------
    # SILVER
    # ------------------------------------------------------------------

    def silver(self) -> str:
        bronze_in = self.spark.read.format("hudi").load(self.bronze_path)

        eval_schema = self.infer_json_schema(bronze_in, "evaluation")
        b = bronze_in.withColumn("eval_obj", F.from_json("evaluation", eval_schema))

        # --- report fields ---
        report_ts      = F.to_timestamp(F.col("eval_obj.report.timestamp"))
        evaluation_id  = F.col("eval_obj.report.evaluationID")
        prcg_tm_dp     = F.col("eval_obj.report.metaData.prcgTmDP").cast("long")
        prcg_tm_ed     = F.col("eval_obj.report.metaData.prcgTmED").cast("long")
        report_status  = F.col("eval_obj.report.status")

        # --- tadpResult ---
        tadp_id        = F.col("eval_obj.report.tadpResult.id")
        tadp_cfg       = F.col("eval_obj.report.tadpResult.cfg")
        tadp_prcg_tm   = F.col("eval_obj.report.tadpResult.prcgTm").cast("long")
        typology_count = F.size(F.col("eval_obj.report.tadpResult.typologyResult"))

        # --- dataCache ---
        dc_cdtr_id      = F.col("eval_obj.dataCache.cdtrId")
        dc_dbtr_id      = F.col("eval_obj.dataCache.dbtrId")
        dc_cre_dt_tm    = F.to_timestamp(F.col("eval_obj.dataCache.creDtTm"))
        dc_instd_amt    = F.col("eval_obj.dataCache.instdAmt.amt").cast("double")
        dc_instd_ccy    = F.col("eval_obj.dataCache.instdAmt.ccy")
        dc_xchg_rate    = F.col("eval_obj.dataCache.xchgRate").cast("double")
        dc_cdtr_acct_id = F.col("eval_obj.dataCache.cdtrAcctId")
        dc_dbtr_acct_id = F.col("eval_obj.dataCache.dbtrAcctId")
        dc_intrbk_amt   = F.col("eval_obj.dataCache.intrBkSttlmAmt.amt").cast("double")
        dc_intrbk_ccy   = F.col("eval_obj.dataCache.intrBkSttlmAmt.ccy")

        # --- transaction fields ---
        tx_type           = F.col("eval_obj.transaction.TxTp")
        tx_tenant_id      = F.col("eval_obj.transaction.TenantId")
        tx_msg_id         = F.col("eval_obj.transaction.FIToFIPmtSts.GrpHdr.MsgId")
        tx_cre_dt_tm      = F.to_timestamp(F.col("eval_obj.transaction.FIToFIPmtSts.GrpHdr.CreDtTm"))
        tx_status         = F.col("eval_obj.transaction.FIToFIPmtSts.TxInfAndSts.TxSts")
        tx_accptnc_dt_tm  = F.to_timestamp(F.col("eval_obj.transaction.FIToFIPmtSts.TxInfAndSts.AccptncDtTm"))
        tx_orgnl_instr_id = F.col("eval_obj.transaction.FIToFIPmtSts.TxInfAndSts.OrgnlInstrId")
        tx_orgnl_e2e_id   = F.col("eval_obj.transaction.FIToFIPmtSts.TxInfAndSts.OrgnlEndToEndId")
        tx_instg_mmb_id   = F.col("eval_obj.transaction.FIToFIPmtSts.TxInfAndSts.InstgAgt.FinInstnId.ClrSysMmbId.MmbId")
        tx_instd_mmb_id   = F.col("eval_obj.transaction.FIToFIPmtSts.TxInfAndSts.InstdAgt.FinInstnId.ClrSysMmbId.MmbId")

        # Effective event timestamp: prefer report timestamp, fall back to tx creation time
        event_ts = F.coalesce(report_ts, tx_cre_dt_tm)

        silver = (
            b
            .withColumn("evaluation_id",      evaluation_id)
            .withColumn("tenant_id",          F.coalesce(F.col("tenantid"), tx_tenant_id))
            .withColumn("message_id",         F.col("messageid"))
            .withColumn("report_status",      report_status)
            .withColumn("event_ts",           event_ts)
            .withColumn("event_date",         F.to_date(event_ts))
            .withColumn("prcg_tm_dp",         prcg_tm_dp)
            .withColumn("prcg_tm_ed",         prcg_tm_ed)
            .withColumn("tadp_id",            tadp_id)
            .withColumn("tadp_cfg",           tadp_cfg)
            .withColumn("tadp_prcg_tm",       tadp_prcg_tm)
            .withColumn("typology_count",     typology_count)
            .withColumn("dc_cdtr_id",         dc_cdtr_id)
            .withColumn("dc_dbtr_id",         dc_dbtr_id)
            .withColumn("dc_cre_dt_tm",       dc_cre_dt_tm)
            .withColumn("dc_instd_amt",       dc_instd_amt)
            .withColumn("dc_instd_ccy",       dc_instd_ccy)
            .withColumn("dc_xchg_rate",       dc_xchg_rate)
            .withColumn("dc_cdtr_acct_id",    dc_cdtr_acct_id)
            .withColumn("dc_dbtr_acct_id",    dc_dbtr_acct_id)
            .withColumn("dc_intrbk_amt",      dc_intrbk_amt)
            .withColumn("dc_intrbk_ccy",      dc_intrbk_ccy)
            .withColumn("tx_type",            tx_type)
            .withColumn("tx_msg_id",          tx_msg_id)
            .withColumn("tx_cre_dt_tm",       tx_cre_dt_tm)
            .withColumn("tx_status",          tx_status)
            .withColumn("tx_accptnc_dt_tm",   tx_accptnc_dt_tm)
            .withColumn("tx_orgnl_instr_id",  tx_orgnl_instr_id)
            .withColumn("tx_orgnl_e2e_id",    tx_orgnl_e2e_id)
            .withColumn("tx_instg_mmb_id",    tx_instg_mmb_id)
            .withColumn("tx_instd_mmb_id",    tx_instd_mmb_id)
            .withColumn("ingested_at_ts",     F.coalesce(F.col("ingested_at_ts"), F.current_timestamp()))
            # Drop bronze-only columns
            .drop("_row_payload_json", "eval_obj", "credttm", "tenantid", "messageid")
        )

        # Dedup — keep latest per message_id
        w = Window.partitionBy("message_id").orderBy(F.col("ingested_at_ts").desc())
        silver = silver.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

        self.write_hudi(
            silver,
            self.silver_path,
            self.hudi_opts("silver_evaluation", "message_id", "ingested_at_ts"),
        )
        print(f"[EvaluationETL] Silver written → {self.silver_path}")
        return self.silver_path

    # ------------------------------------------------------------------
    # GOLD
    # ------------------------------------------------------------------

    def gold(self) -> str:
        silver_in = self.spark.read.format("hudi").load(self.silver_path)

        gold = (
            silver_in
            # Latency: report event ts vs dataCache creation time
            .withColumn(
                "dc_to_report_ms",
                F.when(
                    F.col("event_ts").isNotNull() & F.col("dc_cre_dt_tm").isNotNull(),
                    (F.col("event_ts").cast("double") - F.col("dc_cre_dt_tm").cast("double")) * 1000.0,
                ).otherwise(F.lit(None).cast("double")),
            )
            # Latency: report event ts vs ingest time
            .withColumn(
                "event_to_ingest_ms",
                F.when(
                    F.col("event_ts").isNotNull(),
                    (F.col("ingested_at_ts").cast("double") - F.col("event_ts").cast("double")) * 1000.0,
                ).otherwise(F.lit(None).cast("double")),
            )
            # Total processing time across all pipeline components
            .withColumn(
                "total_prcg_tm_ns",
                (
                    F.coalesce(F.col("prcg_tm_dp"),   F.lit(0))
                    + F.coalesce(F.col("prcg_tm_ed"),   F.lit(0))
                    + F.coalesce(F.col("tadp_prcg_tm"), F.lit(0))
                ).cast("long"),
            )
            # Alert flag
            .withColumn(
                "is_alert",
                F.when(F.upper(F.col("report_status")) == "ALRT", F.lit(1))
                 .otherwise(F.lit(0))
                 .cast("int"),
            )
            # Normalized statuses
            .withColumn("report_status_norm", F.upper(F.col("report_status")))
            .withColumn("tx_status_norm",     F.upper(F.col("tx_status")))
            .select(
                F.col("evaluation_id").cast("string").alias("evaluation_id"),
                F.col("message_id").cast("string").alias("message_id"),
                F.col("tenant_id").cast("string").alias("tenant_id"),
                F.col("report_status_norm").cast("string").alias("report_status"),
                F.col("is_alert").cast("int").alias("is_alert"),
                F.col("event_ts").cast("timestamp").alias("event_ts"),
                F.col("event_date").cast("date").alias("event_date"),
                F.col("prcg_tm_dp").cast("long").alias("prcg_tm_dp_ns"),
                F.col("prcg_tm_ed").cast("long").alias("prcg_tm_ed_ns"),
                F.col("tadp_id").cast("string").alias("tadp_id"),
                F.col("tadp_cfg").cast("string").alias("tadp_cfg"),
                F.col("tadp_prcg_tm").cast("long").alias("tadp_prcg_tm_ns"),
                F.col("total_prcg_tm_ns").cast("long").alias("total_prcg_tm_ns"),
                F.col("typology_count").cast("int").alias("typology_count"),
                F.col("dc_cdtr_id").cast("string").alias("dc_cdtr_id"),
                F.col("dc_dbtr_id").cast("string").alias("dc_dbtr_id"),
                F.col("dc_cre_dt_tm").cast("timestamp").alias("dc_cre_dt_tm"),
                F.col("dc_instd_amt").cast("double").alias("dc_instd_amt"),
                F.col("dc_instd_ccy").cast("string").alias("dc_instd_ccy"),
                F.col("dc_xchg_rate").cast("double").alias("dc_xchg_rate"),
                F.col("dc_cdtr_acct_id").cast("string").alias("dc_cdtr_acct_id"),
                F.col("dc_dbtr_acct_id").cast("string").alias("dc_dbtr_acct_id"),
                F.col("dc_intrbk_amt").cast("double").alias("dc_intrbk_amt"),
                F.col("dc_intrbk_ccy").cast("string").alias("dc_intrbk_ccy"),
                F.col("tx_type").cast("string").alias("tx_type"),
                F.col("tx_msg_id").cast("string").alias("tx_msg_id"),
                F.col("tx_cre_dt_tm").cast("timestamp").alias("tx_cre_dt_tm"),
                F.col("tx_status_norm").cast("string").alias("tx_status"),
                F.col("tx_accptnc_dt_tm").cast("timestamp").alias("tx_accptnc_dt_tm"),
                F.col("tx_orgnl_instr_id").cast("string").alias("tx_orgnl_instr_id"),
                F.col("tx_orgnl_e2e_id").cast("string").alias("tx_orgnl_e2e_id"),
                F.col("tx_instg_mmb_id").cast("string").alias("tx_instg_mmb_id"),
                F.col("tx_instd_mmb_id").cast("string").alias("tx_instd_mmb_id"),
                F.col("dc_to_report_ms").cast("long").alias("dc_to_report_ms"),
                F.col("event_to_ingest_ms").cast("long").alias("event_to_ingest_ms"),
                F.col("source_file_path").cast("string").alias("source_file_path"),
                F.col("record_hash").cast("string").alias("record_hash"),
                F.col("ingested_at_ts").cast("timestamp").alias("ingested_at_ts"),
            )
        )

        bad = [c for c, t in gold.dtypes if t.startswith(("array", "struct", "map"))]
        if bad:
            raise RuntimeError(f"[EvaluationETL] Gold has non-scalar columns: {bad}")

        gold_opts = {
            **self.hudi_opts("gold_evaluation", "evaluation_id", "ingested_at_ts", partition="event_date"),
            "hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
        }
        self.write_hudi(gold, self.gold_path, gold_opts)
        print(f"[EvaluationETL] Gold written → {self.gold_path}")
        return self.gold_path

    # ------------------------------------------------------------------
    # ORCHESTRATOR
    # ------------------------------------------------------------------

    def run(self, source_path: str) -> str:
        print(f"[EvaluationETL] Starting Bronze → Silver → Gold from {source_path}")
        self.bronze(source_path)
        self.silver()
        self.gold()
        print("[EvaluationETL] ETL complete.")
        return self.gold_path