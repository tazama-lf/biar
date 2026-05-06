"""
transaction_detail_view.py
--------------------------
Builds the vw_transaction_detail view from bronze/transactions.
Parses pacs.008 + pacs.002 JSON and writes a denormalized Hudi view.

Carries forward the composite primary key transaction_pk
(TxTp || "||" || endToEndId) from TransactionsETL as the sole primary key.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .BaseETL import BaseETL


class TransactionDetailViewETL(BaseETL):
    """
    Transaction Detail View builder.

    Reads bronze/transactions, extracts embedded PACS JSON fields,
    and writes vw_transaction_detail as a Hudi view.

    Uses transaction_pk (TxTp||endToEndId) as the primary key.
    """

    def __init__(self, spark, warehouse_root: str) -> None:
        super().__init__(spark, warehouse_root)
        self.views_root = f"{self.warehouse_root}/views"
        self.view_path = f"{self.views_root}/vw_transaction_detail"
        self.transactions_bronze_path = f"{self.warehouse_root}/bronze/transactions"

    @property
    def bronze_path(self) -> str:
        return self.view_path

    @property
    def silver_path(self) -> str:
        return self.view_path

    @property
    def gold_path(self) -> str:
        return self.view_path

    # ------------------------------------------------------------------
    # INTERNAL HELPERS
    # ------------------------------------------------------------------

    def _resolve_json_column(self, df: DataFrame) -> DataFrame:
        """Auto-detect and normalize the raw JSON payload column."""
        candidates = [
            "transactionData",
            "transaction_data",
            "transaction",
            "payload",
            "raw_payload",
            "raw_json",
            "transaction_json",
        ]
        json_col = next((c for c in candidates if c in df.columns), None)
        if json_col is None:
            raise ValueError(
                f"No raw JSON column found in bronze/transactions. "
                f"Tried: {candidates}\nAvailable: {df.columns}"
            )
        print(f"[TransactionDetailViewETL] Using JSON column: {json_col} → transaction_data")
        return df.withColumn("transaction_data", F.col(json_col).cast("string"))

    def _extract_pacs_fields(self, df: DataFrame) -> DataFrame:
        """Add all PACS-derived columns from the JSON payload."""
        # --- core identifiers ------------------------------------------------
        tx_type = F.get_json_object("transaction_data", "$.TxTp")
        tx_msg_id = F.coalesce(
            F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.GrpHdr.MsgId"),
            F.get_json_object("transaction_data", "$.FIToFIPmtSts.GrpHdr.MsgId"),
        )
        tx_event_ts = F.to_timestamp(
            F.coalesce(
                F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.GrpHdr.CreDtTm"),
                F.get_json_object("transaction_data", "$.FIToFIPmtSts.GrpHdr.CreDtTm"),
            )
        )
        tx_event_date = F.to_date(tx_event_ts)
        tx_tenant = F.get_json_object("transaction_data", "$.TenantId")

        # --- parties ---------------------------------------------------------
        dbtr_name = F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.Dbtr.Nm")
        dbtr_id = F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.Dbtr.Id.PrvtId.Othr[0].Id")
        cdtr_name = F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.Cdtr.Nm")
        cdtr_id = F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.Cdtr.Id.PrvtId.Othr[0].Id")

        dbtr_acct_id = F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.DbtrAcct.Id.Othr[0].Id")
        cdtr_acct_id = F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAcct.Id.Othr[0].Id")

        # --- amounts ---------------------------------------------------------
        instd_amt = F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.InstdAmt.Amt.Amt").cast("double")
        instd_ccy = F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.InstdAmt.Amt.Ccy")

        intrbk_amt = F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.IntrBkSttlmAmt.Amt.Amt").cast("double")
        intrbk_ccy = F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.IntrBkSttlmAmt.Amt.Ccy")
        xchg_rate = F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.XchgRate").cast("double")

        # --- agents ----------------------------------------------------------
        instg_mmb_id = F.coalesce(
            F.get_json_object("transaction_data", "$.FIToFIPmtSts.TxInfAndSts.InstgAgt.FinInstnId.ClrSysMmbId.MmbId"),
            F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.DbtrAgt.FinInstnId.ClrSysMmbId.MmbId"),
        )
        instd_mmb_id = F.coalesce(
            F.get_json_object("transaction_data", "$.FIToFIPmtSts.TxInfAndSts.InstdAgt.FinInstnId.ClrSysMmbId.MmbId"),
            F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAgt.FinInstnId.ClrSysMmbId.MmbId"),
        )

        # --- charges (pacs.002 array vs pacs.008 struct) ---------------------
        charges_arr_json = F.get_json_object("transaction_data", "$.FIToFIPmtSts.TxInfAndSts.ChrgsInf")
        charges_obj_json = F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.ChrgsInf")

        charges_arr_schema = "array<struct<Agt:struct<FinInstnId:struct<ClrSysMmbId:struct<MmbId:string>>>,Amt:struct<Amt:double,Ccy:string>>>"
        charges_obj_schema = "struct<Agt:struct<FinInstnId:struct<ClrSysMmbId:struct<MmbId:string>>>,Amt:struct<Amt:double,Ccy:string>>"

        return (
            df
            .withColumn("tx_type", tx_type)
            .withColumn("tx_msg_id", tx_msg_id)
            .withColumn("tx_event_ts", tx_event_ts)
            .withColumn("tx_event_date", tx_event_date)
            .withColumn("tx_tenant_id", tx_tenant)
            .withColumn("debtor_name", dbtr_name)
            .withColumn("debtor_id", dbtr_id)
            .withColumn("creditor_name", cdtr_name)
            .withColumn("creditor_id", cdtr_id)
            .withColumn("debtor_account_id", dbtr_acct_id)
            .withColumn("creditor_account_id", cdtr_acct_id)
            .withColumn("instructed_amount", instd_amt)
            .withColumn("instructed_currency", instd_ccy)
            .withColumn("interbank_settlement_amount", intrbk_amt)
            .withColumn("interbank_settlement_currency", intrbk_ccy)
            .withColumn("exchange_rate", xchg_rate)
            .withColumn("instg_mmb_id", instg_mmb_id)
            .withColumn("instd_mmb_id", instd_mmb_id)
            .withColumn("charges_arr", F.from_json(charges_arr_json, charges_arr_schema))
            .withColumn("charges_obj", F.from_json(charges_obj_json, charges_obj_schema))
            .withColumn(
                "charge_count",
                F.when(F.col("charges_arr").isNotNull(), F.size("charges_arr"))
                .when(F.col("charges_obj").isNotNull(), F.lit(1))
                .otherwise(F.lit(0))
                .cast("int"),
            )
            .withColumn(
                "charge_total_amount",
                F.when(
                    F.col("charges_arr").isNotNull(),
                    F.expr(
                        "aggregate(transform(charges_arr, x -> coalesce(x.Amt.Amt, 0D)), "
                        "0D, (acc, x) -> acc + x)"
                    ),
                )
                .when(
                    F.col("charges_obj").isNotNull(),
                    F.coalesce(F.col("charges_obj.Amt.Amt").cast("double"), F.lit(0.0)),
                )
                .otherwise(F.lit(0.0))
                .cast("double"),
            )
            .withColumn(
                "charge_currency",
                F.when(
                    F.col("charges_arr").isNotNull(),
                    F.expr("element_at(transform(charges_arr, x -> x.Amt.Ccy), 1)"),
                )
                .when(F.col("charges_obj").isNotNull(), F.col("charges_obj.Amt.Ccy"))
                .otherwise(F.lit(None).cast("string")),
            )
            .drop("charges_arr", "charges_obj")
        )

    def _finalize_schema(self, df: DataFrame) -> DataFrame:
        """Cast and select the final view schema."""
        has_source_file = "source_file_path" in df.columns
        has_record_hash = "record_hash" in df.columns

        return df.select(
            F.col("transaction_pk").cast("string").alias("transaction_pk"),
            F.col("end_to_end_id").cast("string").alias("end_to_end_id"),
            F.col("tenant_id").cast("string").alias("tenant_id"),
            F.col("tx_tenant_id").cast("string").alias("tx_tenant_id"),
            F.col("tx_type").cast("string").alias("tx_type"),
            F.col("tx_msg_id").cast("string").alias("tx_msg_id"),
            F.col("tx_event_ts").cast("timestamp").alias("tx_event_ts"),
            F.col("tx_event_date").cast("date").alias("tx_event_date"),
            F.col("debtor_name").cast("string").alias("debtor_name"),
            F.col("debtor_id").cast("string").alias("debtor_id"),
            F.col("creditor_name").cast("string").alias("creditor_name"),
            F.col("creditor_id").cast("string").alias("creditor_id"),
            F.col("debtor_account_id").cast("string").alias("debtor_account_id"),
            F.col("creditor_account_id").cast("string").alias("creditor_account_id"),
            F.col("instructed_amount").cast("double").alias("instructed_amount"),
            F.col("instructed_currency").cast("string").alias("instructed_currency"),
            F.col("interbank_settlement_amount").cast("double").alias("interbank_settlement_amount"),
            F.col("interbank_settlement_currency").cast("string").alias("interbank_settlement_currency"),
            F.col("exchange_rate").cast("double").alias("exchange_rate"),
            F.col("instg_mmb_id").cast("string").alias("instg_mmb_id"),
            F.col("instd_mmb_id").cast("string").alias("instd_mmb_id"),
            F.col("charge_count").cast("int").alias("charge_count"),
            F.col("charge_total_amount").cast("double").alias("charge_total_amount"),
            F.col("charge_currency").cast("string").alias("charge_currency"),
            (
                F.col("source_file_path").cast("string")
                if has_source_file
                else F.lit(None).cast("string")
            ).alias("source_file_path"),
            (
                F.col("record_hash").cast("string")
                if has_record_hash
                else F.lit(None).cast("string")
            ).alias("record_hash"),
            F.current_timestamp().cast("timestamp").alias("ingested_at_ts"),
        )

    # ------------------------------------------------------------------
    # BRONZE  (main view build)
    # ------------------------------------------------------------------

    def bronze(self, source_path: str = "") -> str:
        """
        Build vw_transaction_detail from bronze/transactions.

        *source_path* is ignored (reads from warehouse bronze path).
        """
        print("[TransactionDetailViewETL] Creating Transaction Detail View...")

        # 1. Load bronze transactions
        tx = self.spark.read.format("hudi").load(self.transactions_bronze_path)

        # 2. Normalize column names
        rename_map = {
            "endToEndId": "end_to_end_id",
            "tenantId": "tenant_id",
            "transaction_pk": "transaction_pk",
        }
        for src, dst in rename_map.items():
            if src in tx.columns and dst not in tx.columns:
                tx = tx.withColumnRenamed(src, dst)

        # 3. Resolve JSON column
        tx = self._resolve_json_column(tx)

        # 4. Extract PACS fields
        tx = self._extract_pacs_fields(tx)

        # 5. Finalize schema
        tx_detail_view = self._finalize_schema(tx)

        # 6. Write Hudi view — transaction_pk is the primary key
        self.write_hudi(
            tx_detail_view,
            self.view_path,
            self.hudi_opts(
                "vw_transaction_detail",
                record_key="transaction_pk",
                precombine="ingested_at_ts",
            ),
        )
        print(f"[TransactionDetailViewETL] View written → {self.view_path}")
        return self.view_path

    # ------------------------------------------------------------------
    # SILVER / GOLD  (no-op for view builders)
    # ------------------------------------------------------------------

    def silver(self) -> str:
        return self.view_path

    def gold(self) -> str:
        return self.view_path

    # ------------------------------------------------------------------
    # ORCHESTRATOR
    # ------------------------------------------------------------------

    def run(self, source_path: str = "") -> str:
        print("[TransactionDetailViewETL] Starting view build")
        self.bronze(source_path)
        print("[TransactionDetailViewETL] View build complete.")
        return self.view_path
