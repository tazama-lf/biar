"""
pacs002.py
----------
Bronze → Silver → Gold ETL for pacs.002.001.12 payment status messages.
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .BaseETL import BaseETL


class Pacs002ETL(BaseETL):
    """Full Bronze → Silver → Gold pipeline for pacs.002 messages."""

    @property
    def bronze_path(self) -> str:
        return f"{self.warehouse_root}/bronze/pacs002"

    @property
    def silver_path(self) -> str:
        return f"{self.warehouse_root}/silver/pacs002"

    @property
    def gold_path(self) -> str:
        return f"{self.warehouse_root}/gold/pacs002"

    # ------------------------------------------------------------------
    # BRONZE
    # ------------------------------------------------------------------

    def bronze(self, source_path: str) -> str:
        df_bronze = self.spark.read.json(source_path)

        doc_schema = self.spark.read.json(
            df_bronze.select("document").where(F.col("document").isNotNull()).rdd.map(lambda r: r[0])
        ).schema

        pacs002_df = df_bronze.withColumn("doc", F.from_json(F.col("document"), doc_schema))
        pacs002_df = (
            pacs002_df
            .withColumn("messageid",  F.col("doc.FIToFIPmtSts.GrpHdr.MsgId"))
            .withColumn("endtoendid", F.col("doc.FIToFIPmtSts.TxInfAndSts.OrgnlEndToEndId"))
            .withColumn("tenantid",   F.col("doc.TenantId"))
            .withColumn("credtm",     F.col("doc.FIToFIPmtSts.GrpHdr.CreDtTm"))
        )

        bronze = (
            pacs002_df
            .withColumnRenamed("tenantid",  "tenant_id")
            .withColumnRenamed("messageid", "message_id")
            .withColumnRenamed("endtoendid","end_to_end_id")
            .withColumnRenamed("credtm",    "credttm_raw")
            .withColumn("credttm_ts",       F.to_timestamp(F.col("credttm_raw")))
            .withColumn("event_date",       F.to_date(F.col("credttm_ts")))
            .withColumn("ingested_at_ts",   F.current_timestamp())
            .withColumn("source_file_path", F.lit(None).cast("string"))
        )

        bronze = bronze.withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("tenant_id"),    F.lit("")),
                    F.coalesce(F.col("message_id"),   F.lit("")),
                    F.coalesce(F.col("end_to_end_id"), F.lit("")),
                    F.coalesce(F.col("credttm_raw"),  F.lit("")),
                ),
                256,
            ),
        ).withColumn("_row_payload_json", F.to_json(F.struct("*")))

        self.write_hudi(
            bronze,
            self.bronze_path,
            self.hudi_opts("bronze_pacs002", "end_to_end_id", "ingested_at_ts"),
        )
        print(f"[Pacs002ETL] Bronze written → {self.bronze_path}")
        return self.bronze_path

    # ------------------------------------------------------------------
    # SILVER
    # ------------------------------------------------------------------

    def silver(self) -> str:
        bronze_df = self.spark.read.format("hudi").load(self.bronze_path)

        doc_schema = self.spark.read.json(
            bronze_df.select("document").where(F.col("document").isNotNull()).rdd.map(lambda r: r[0])
        ).schema

        s = bronze_df.withColumn("doc", F.from_json(F.col("document"), doc_schema))

        # DataCache fields
        s = (
            s
            .withColumn("dc_cdtr_id",     F.col("doc.DataCache.cdtrId").cast("string"))
            .withColumn("dc_dbtr_id",     F.col("doc.DataCache.dbtrId").cast("string"))
            .withColumn("dc_cre_dt_tm",   F.to_timestamp(F.col("doc.DataCache.creDtTm")))
            .withColumn("dc_instd_amt",   F.col("doc.DataCache.instdAmt.amt").cast("double"))
            .withColumn("dc_instd_ccy",   F.col("doc.DataCache.instdAmt.ccy").cast("string"))
            .withColumn("dc_xchg_rate",   F.col("doc.DataCache.xchgRate").cast("string"))
            .withColumn("dc_cdtr_acct_id", F.col("doc.DataCache.cdtrAcctId").cast("string"))
            .withColumn("dc_dbtr_acct_id", F.col("doc.DataCache.dbtrAcctId").cast("string"))
            .withColumn("dc_intrbk_amt",  F.col("doc.DataCache.intrBkSttlmAmt.amt").cast("double"))
            .withColumn("dc_intrbk_ccy",  F.col("doc.DataCache.intrBkSttlmAmt.ccy").cast("string"))
        )

        # Group header + transaction status
        s = (
            s
            .withColumn("grp_msg_id",        F.col("doc.FIToFIPmtSts.GrpHdr.MsgId").cast("string"))
            .withColumn("grp_cre_dt_tm",     F.to_timestamp(F.col("doc.FIToFIPmtSts.GrpHdr.CreDtTm")))
            .withColumn("tx_status",         F.col("doc.FIToFIPmtSts.TxInfAndSts.TxSts").cast("string"))
            .withColumn("accptnc_dt_tm",     F.to_timestamp(F.col("doc.FIToFIPmtSts.TxInfAndSts.AccptncDtTm")))
            .withColumn("orgnl_instr_id",    F.col("doc.FIToFIPmtSts.TxInfAndSts.OrgnlInstrId").cast("string"))
            .withColumn("orgnl_end_to_end_id", F.col("doc.FIToFIPmtSts.TxInfAndSts.OrgnlEndToEndId").cast("string"))
            .withColumn("status_reason_code", F.get_json_object(F.col("document"), "$.FIToFIPmtSts.TxInfAndSts.StsRsnInf.Rsn.Cd").cast("string"))
            .withColumn("instd_mmb_id",      F.col("doc.FIToFIPmtSts.TxInfAndSts.InstdAgt.FinInstnId.ClrSysMmbId.MmbId").cast("string"))
            .withColumn("instg_mmb_id",      F.col("doc.FIToFIPmtSts.TxInfAndSts.InstgAgt.FinInstnId.ClrSysMmbId.MmbId").cast("string"))
        )

        # Charges
        charges = F.col("doc.FIToFIPmtSts.TxInfAndSts.ChrgsInf")
        s = (
            s
            .withColumn("charge_count",         F.when(charges.isNotNull(), F.size(charges)).otherwise(F.lit(0)).cast("int"))
            .withColumn("charge_total_amount",  F.when(charges.isNotNull(), F.expr("aggregate(transform(doc.FIToFIPmtSts.TxInfAndSts.ChrgsInf, x -> cast(x.Amt.Amt as double)), cast(0.0 as double), (acc, v) -> acc + coalesce(v, 0.0))")).otherwise(F.lit(0.0)).cast("double"))
            .withColumn("charge_currency_count", F.when(charges.isNotNull(), F.expr("size(array_distinct(transform(doc.FIToFIPmtSts.TxInfAndSts.ChrgsInf, x -> x.Amt.Ccy)))")).otherwise(F.lit(0)).cast("int"))
            .withColumn("charge_currency_hint",  F.when(charges.isNotNull(), F.expr("element_at(array_distinct(transform(doc.FIToFIPmtSts.TxInfAndSts.ChrgsInf, x -> x.Amt.Ccy)), 1)")).otherwise(F.lit(None).cast("string")))
            .withColumn("event_ts",             F.coalesce(F.col("grp_cre_dt_tm"), F.col("credttm_ts"), F.col("dc_cre_dt_tm")))
            .withColumn("event_date_silver",    F.to_date(F.col("event_ts")))
        )

        if "tx_type" not in s.columns:
            s = s.withColumn(
                "tx_type",
                F.coalesce(
                    F.get_json_object(F.col("document").cast("string"), "$.TxTp"),
                    F.lit("pacs.002.001.12"),
                ),
            )

        w = Window.partitionBy("tenant_id", "message_id").orderBy(F.col("ingested_at_ts").desc_nulls_last())
        silver = s.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn", "doc", "_row_payload_json")

        # Drop the raw document to keep silver lean
        if "document" in silver.columns:
            silver = silver.drop("document")

        self.write_hudi(
            silver,
            self.silver_path,
            self.hudi_opts("silver_pacs002", "end_to_end_id", "ingested_at_ts"),
        )
        print(f"[Pacs002ETL] Silver written → {self.silver_path}")
        return self.silver_path

    # ------------------------------------------------------------------
    # GOLD
    # ------------------------------------------------------------------

    def gold(self) -> str:
        s = self.spark.read.format("hudi").load(self.silver_path)
        s = self.ensure_columns(s, {"tx_tenant_id": "string"})
        s = s.withColumn("tx_tenant_id", F.coalesce(F.col("tx_tenant_id"), F.col("tenant_id").cast("string")))

        tx_msg_id       = F.col("grp_msg_id").cast("string")
        tx_event_ts     = F.col("event_ts").cast("timestamp")
        tx_amount       = F.col("dc_instd_amt").cast("double")
        tx_ccy          = F.col("dc_instd_ccy").cast("string")
        event_to_ingest = ((F.col("ingested_at_ts").cast("double") - F.col("event_ts").cast("double")) * 1000.0)

        gold_pk = F.sha2(
            F.concat_ws(
                "||",
                F.lit("gold_pacs002"),
                F.coalesce(F.col("tenant_id"),    F.lit("")),
                F.coalesce(tx_msg_id,             F.lit("")),
                F.coalesce(F.col("end_to_end_id"), F.lit("")),
                F.coalesce(F.col("record_hash"),  F.lit("")),
            ),
            256,
        )

        gold = (
            s
            .withColumn("pk",                 gold_pk)
            .withColumn("tx_msg_id",          tx_msg_id)
            .withColumn("tx_event_ts",        tx_event_ts)
            .withColumn("tx_amount",          tx_amount)
            .withColumn("tx_ccy",             tx_ccy)
            .withColumn("event_to_ingest_ms", event_to_ingest.cast("long"))
            .withColumn("event_date",         F.to_date(F.col("event_ts")))
            .select(
                "pk", "tenant_id", "message_id", "end_to_end_id",
                "credttm_raw", "credttm_ts",
                "tx_type", "tx_msg_id", "tx_status",
                "tx_amount", "tx_ccy",
                "instg_mmb_id", "instd_mmb_id",
                "charge_count", "event_ts", "tx_event_ts",
                "event_date", "event_to_ingest_ms",
                "tx_tenant_id",
                "dc_cdtr_id", "dc_dbtr_id", "dc_cre_dt_tm",
                "dc_instd_amt", "dc_instd_ccy", "dc_xchg_rate",
                "dc_cdtr_acct_id", "dc_dbtr_acct_id",
                "dc_intrbk_amt", "dc_intrbk_ccy",
                "grp_msg_id", "grp_cre_dt_tm",
                "accptnc_dt_tm", "orgnl_instr_id", "orgnl_end_to_end_id",
                "status_reason_code",
                "charge_total_amount", "charge_currency_count", "charge_currency_hint",
                "record_hash", "ingested_at_ts",
            )
        )

        bad = [c for c, t in gold.dtypes if t.startswith(("array", "struct", "map"))]
        if bad:
            raise RuntimeError(f"[Pacs002ETL] GOLD has non-scalar columns: {bad}")

        self.write_hudi(
            gold,
            self.gold_path,
            self.hudi_opts("pacs002", "end_to_end_id", "ingested_at_ts"),
        )
        print(f"[Pacs002ETL] Gold written → {self.gold_path}")
        return self.gold_path

    # ------------------------------------------------------------------
    # ORCHESTRATOR
    # ------------------------------------------------------------------

    def run(self, source_path: str) -> str:
        print(f"[Pacs002ETL] Starting Bronze → Silver → Gold from {source_path}")
        self.bronze(source_path)
        self.silver()
        self.gold()
        print("[Pacs002ETL] ETL complete.")
        return self.gold_path