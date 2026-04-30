"""
pacs008.py
----------
Bronze → Silver → Gold ETL for pacs.008.001.10 payment instruction messages.
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .BaseETL import BaseETL

_GOLD_ENSURE_COLS = {
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


class Pacs008ETL(BaseETL):
    """Full Bronze → Silver → Gold pipeline for pacs.008 messages."""

    @property
    def bronze_path(self) -> str:
        return f"{self.warehouse_root}/bronze/pacs008"

    @property
    def silver_path(self) -> str:
        return f"{self.warehouse_root}/silver/pacs008"

    @property
    def gold_path(self) -> str:
        return f"{self.warehouse_root}/gold/pacs008"

    # ------------------------------------------------------------------
    # BRONZE
    # ------------------------------------------------------------------

    def bronze(self, source_path: str) -> str:
        df = self.spark.read.json(source_path)
        print("[Pacs008ETL] Raw messages read.")

        bronze = (
            df
            .withColumnRenamed("tenantid",           "tenant_id")
            .withColumnRenamed("messageid",          "message_id")
            .withColumnRenamed("endtoendid",         "end_to_end_id")
            .withColumnRenamed("credttm",            "credttm_raw")
            .withColumnRenamed("creditoraccountid",  "creditor_account_id")
            .withColumnRenamed("debtoraccountid",    "debtor_account_id")
            .withColumn("document_json",  F.col("document").cast("string"))
            .withColumn("credttm_ts",     F.to_timestamp(F.col("credttm_raw")))
            .withColumn("event_date",     F.to_date(F.col("credttm_ts")))
            .withColumn("ingested_at_ts", F.current_timestamp())
            .withColumn("source_file_path", F.lit(None).cast("string"))
        )

        bronze = bronze.withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("tenant_id"),          F.lit("")),
                    F.coalesce(F.col("message_id"),         F.lit("")),
                    F.coalesce(F.col("end_to_end_id"),      F.lit("")),
                    F.coalesce(F.col("creditor_account_id"), F.lit("")),
                    F.coalesce(F.col("debtor_account_id"),  F.lit("")),
                    F.coalesce(F.col("credttm_raw"),        F.lit("")),
                    F.coalesce(F.col("document_json"),      F.lit("")),
                ),
                256,
            ),
        ).withColumn("_row_payload_json", F.to_json(F.struct("*")))

        self.write_hudi(
            bronze,
            self.bronze_path,
            self.hudi_opts("bronze_pacs008", "end_to_end_id", "ingested_at_ts"),
        )
        print(f"[Pacs008ETL] Bronze written → {self.bronze_path}")
        return self.bronze_path

    # ------------------------------------------------------------------
    # SILVER
    # ------------------------------------------------------------------

    def silver(self) -> str:
        bronze_df = self.spark.read.format("hudi").load(self.bronze_path)
        doc_schema = self.infer_json_schema(bronze_df, "document")

        silver = (
            bronze_df
            .withColumn("doc_obj",        F.from_json("document", doc_schema))
            .withColumn("msg_id",         F.get_json_object("document", "$.FIToFICstmrCdtTrf.GrpHdr.MsgId"))
            .withColumn("creation_dt_tm", F.to_timestamp(F.get_json_object("document", "$.FIToFICstmrCdtTrf.GrpHdr.CreDtTm")))
            .withColumn("tx_type",        F.lit("pacs.008.001.10"))
            .withColumn("instd_amt",      F.get_json_object("document", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.InstdAmt.Amt.Amt").cast("double"))
            .withColumn("instd_ccy",      F.get_json_object("document", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.InstdAmt.Amt.Ccy"))
            .withColumn("dbtr_mmb_id",    F.get_json_object("document", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.DbtrAgt.FinInstnId.ClrSysMmbId.MmbId"))
            .withColumn("cdtr_mmb_id",    F.get_json_object("document", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAgt.FinInstnId.ClrSysMmbId.MmbId"))
            .withColumn("dbtr_acct_id",   F.get_json_object("document", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.DbtrAcct.Id.Othr.Id"))
            .withColumn("cdtr_acct_id",   F.get_json_object("document", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAcct.Id.Othr.Id"))
            .withColumn("event_date",     F.to_date("creation_dt_tm"))
            .withColumn("created_at_ts",  F.current_timestamp())
        )

        w = Window.partitionBy("end_to_end_id").orderBy(F.col("created_at_ts").desc())
        silver = silver.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

        self.write_hudi(
            silver,
            self.silver_path,
            self.hudi_opts("silver_pacs008", "end_to_end_id", "ingested_at_ts"),
        )
        print(f"[Pacs008ETL] Silver written → {self.silver_path}")
        return self.silver_path

    # ------------------------------------------------------------------
    # GOLD
    # ------------------------------------------------------------------

    def gold(self) -> str:
        s = self.spark.read.format("hudi").load(self.silver_path)
        s = self.ensure_columns(s, _GOLD_ENSURE_COLS)
        s = s.withColumn("tx_tenant_id", F.coalesce(F.col("tx_tenant_id"), F.col("tenant_id").cast("string")))
        s = s.withColumn("event_ts",     F.coalesce(F.col("event_ts"), F.col("credttm_ts").cast("timestamp")))

        gold_pk = F.sha2(
            F.concat_ws(
                "||",
                F.lit("gold_pacs008"),
                F.coalesce(F.col("tenant_id"),    F.lit("")),
                F.coalesce(F.col("message_id"),   F.lit("")),
                F.coalesce(F.col("end_to_end_id"), F.lit("")),
                F.coalesce(F.col("record_hash"),  F.lit("")),
            ),
            256,
        )

        gold = (
            s.withColumn("pk", gold_pk)
            .select(
                "pk",
                F.col("tenant_id").cast("string"),
                F.col("message_id").cast("string"),
                F.col("end_to_end_id").cast("string"),
                F.col("creditor_account_id").cast("string"),
                F.col("debtor_account_id").cast("string"),
                F.col("credttm_raw").cast("string"),
                F.col("credttm_ts").cast("timestamp"),
                F.col("tx_type").cast("string"),
                F.col("tx_tenant_id").cast("string"),
                F.col("dc_cdtr_id").cast("string"),
                F.col("dc_dbtr_id").cast("string"),
                F.col("dc_cre_dt_tm").cast("timestamp"),
                F.col("dc_instd_amt").cast("double"),
                F.col("dc_instd_ccy").cast("string"),
                F.col("dc_xchg_rate").cast("string"),
                F.col("dc_cdtr_acct_id").cast("string"),
                F.col("dc_dbtr_acct_id").cast("string"),
                F.col("dc_intrbk_amt").cast("double"),
                F.col("dc_intrbk_ccy").cast("string"),
                F.col("grp_msg_id").cast("string"),
                F.col("grp_cre_dt_tm").cast("timestamp"),
                F.col("grp_nb_of_txs").cast("int"),
                F.col("sttlm_mtd").cast("string"),
                F.col("rmt_ustrd").cast("string"),
                F.col("purp_cd").cast("string"),
                F.col("pmt_instr_id").cast("string"),
                F.col("pmt_e2e_id").cast("string"),
                F.col("chrg_br").cast("string"),
                F.col("cdtr_agt_mmb_id").cast("string"),
                F.col("dbtr_agt_mmb_id").cast("string"),
                F.col("cdtr_name").cast("string"),
                F.col("dbtr_name").cast("string"),
                F.col("cdtr_id").cast("string"),
                F.col("dbtr_id").cast("string"),
                F.col("cdtr_acct_id").cast("string"),
                F.col("dbtr_acct_id").cast("string"),
                F.col("cdtr_acct_scheme").cast("string"),
                F.col("dbtr_acct_scheme").cast("string"),
                F.col("instd_amt").cast("double"),
                F.col("instd_ccy").cast("string"),
                F.col("intrbk_amt").cast("double"),
                F.col("intrbk_ccy").cast("string"),
                F.col("xchg_rate").cast("string"),
                F.col("charge_amt").cast("double"),
                F.col("charge_ccy").cast("string"),
                F.col("charge_agent_mmb_id").cast("string"),
                F.col("event_ts").cast("timestamp"),
                F.to_date(F.col("event_ts")).cast("date").alias("event_date"),
                F.col("record_hash").cast("string"),
                F.col("ingested_at_ts").cast("timestamp"),
            )
        )

        # Guard against accidentally-nested columns
        bad = [c for c, t in gold.dtypes if t.startswith(("array", "struct", "map"))]
        if bad:
            raise RuntimeError(f"[Pacs008ETL] GOLD has non-scalar columns: {bad}")

        self.write_hudi(
            gold,
            self.gold_path,
            self.hudi_opts(
                "pacs008", "end_to_end_id", "ingested_at_ts",
                partition="event_date",
                payload_class="org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
            ),
        )
        print(f"[Pacs008ETL] Gold written → {self.gold_path}")
        return self.gold_path

    # ------------------------------------------------------------------
    # ORCHESTRATOR
    # ------------------------------------------------------------------

    def run(self, source_path: str) -> str:
        print(f"[Pacs008ETL] Starting Bronze → Silver → Gold from {source_path}")
        self.bronze(source_path)
        self.silver()
        self.gold()
        print("[Pacs008ETL] ETL complete.")
        return self.gold_path