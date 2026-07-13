"""
transaction_detail_view.py
--------------------------
Builds the vw_transaction_detail view from bronze/transactions.
Joins PACS gold enrichment and writes a denormalized Hudi view.

Carries forward the composite primary key transaction_id
(TxTp || "||" || endToEndId) from TransactionsETL as the sole primary key.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .BaseETL import BaseETL


class TransactionDetailViewETL(BaseETL):
    """
    Transaction Detail View builder.

    Reads bronze/transactions, joins PACS gold enrichment, and writes
    vw_transaction_detail as a Hudi view.

    Uses transaction_id (TxTp||endToEndId) as the primary key.
    """

    def __init__(self, spark, warehouse_root: str) -> None:
        super().__init__(spark, warehouse_root)
        self.views_root = f"{self.warehouse_root}/views"
        self.view_path = f"{self.views_root}/vw_transaction_detail"
        self.transactions_bronze_path = f"{self.warehouse_root}/bronze/transactions"
        self.pacs008_gold_path = f"{self.warehouse_root}/gold/pacs008"
        self.pacs002_gold_path = f"{self.warehouse_root}/gold/pacs002"

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

    def _safe_load(self, path: str, select_expr: list | None = None) -> DataFrame | None:
        """Attempt to load a Hudi table; return None if it does not exist."""
        try:
            df = self.spark.read.format("hudi").load(path)
            if select_expr:
                df = df.select(*select_expr)
            return df
        except Exception:
            return None

    @staticmethod
    def _event_ts(col_name: str) -> Column:
        """Parse the ISO timestamp emitted by Ozone event history."""
        return F.to_timestamp(F.regexp_replace(F.col(col_name).cast("string"), "Z$", ""))

    @staticmethod
    def _first_available(df: DataFrame, *names: str) -> Column:
        """Return the first available column from the transaction table."""
        cols = [F.col(name) for name in names if name in df.columns]
        if cols:
            return F.coalesce(*cols)
        return F.lit(None)

    def _normalize_transactions(self, tx: DataFrame) -> DataFrame:
        """Normalize Ozone transaction columns without changing view output names."""
        tx = (
            tx
            .withColumn("view_endtoendid", self._first_available(tx, "endtoendid", "raw_history_endtoendid", "endToEndId").cast("string"))
            .withColumn("view_tenantid", self._first_available(tx, "tenantid", "raw_history_tenantid", "tenantId").cast("string"))
            .withColumn("view_txtp", self._first_available(tx, "txtp", "raw_history_txtp", "tx_type").cast("string"))
            .withColumn("view_msgid", self._first_available(tx, "msgid", "raw_history_msgid", "tx_msg_id").cast("string"))
            .withColumn("view_credttm", self._first_available(tx, "credttm", "raw_history_credttm").cast("string"))
            .withColumn("view_amt", self._first_available(tx, "amt", "raw_history_amt").cast("double"))
            .withColumn("view_ccy", self._first_available(tx, "ccy", "raw_history_ccy").cast("string"))
        )
        return (
            tx
            .withColumn("end_to_end_id", F.col("view_endtoendid"))
            .withColumn("tenant_id", F.col("view_tenantid"))
            .withColumn("tx_tenant_id", F.col("view_tenantid"))
            .withColumn("tx_type", F.col("view_txtp"))
            .withColumn("tx_msg_id", F.col("view_msgid"))
            .withColumn("tx_event_ts", F.coalesce(self._first_available(tx, "event_ts").cast("timestamp"), self._event_ts("view_credttm")))
            .withColumn("tx_event_date", F.to_date("tx_event_ts"))
            .withColumn("tx_amount_from_event", F.col("view_amt"))
            .withColumn("tx_ccy_from_event", F.col("view_ccy"))
        )

    def _join_payment_enrichment(self, tx: DataFrame) -> DataFrame:
        """Join transactions to PACS gold tables while preserving view schema."""
        p8 = self._safe_load(
            self.pacs008_gold_path,
            select_expr=[
                F.col("end_to_end_id").cast("string").alias("p8_end_to_end_id"),
                F.col("tx_tenant_id").cast("string").alias("p8_tx_tenant_id"),
                F.col("dbtr_name").cast("string").alias("p8_dbtr_name"),
                F.col("dbtr_id").cast("string").alias("p8_dbtr_id"),
                F.col("cdtr_name").cast("string").alias("p8_cdtr_name"),
                F.col("cdtr_id").cast("string").alias("p8_cdtr_id"),
                F.coalesce(F.col("dbtr_acct_id"), F.col("dc_dbtr_acct_id"), F.col("debtor_account_id")).cast("string").alias("p8_dbtr_account_id"),
                F.coalesce(F.col("cdtr_acct_id"), F.col("dc_cdtr_acct_id"), F.col("creditor_account_id")).cast("string").alias("p8_cdtr_account_id"),
                F.coalesce(F.col("instd_amt"), F.col("dc_instd_amt")).cast("double").alias("p8_instructed_amount"),
                F.coalesce(F.col("instd_ccy"), F.col("dc_instd_ccy")).cast("string").alias("p8_instructed_currency"),
                F.coalesce(F.col("intrbk_amt"), F.col("dc_intrbk_amt")).cast("double").alias("p8_interbank_settlement_amount"),
                F.coalesce(F.col("intrbk_ccy"), F.col("dc_intrbk_ccy")).cast("string").alias("p8_interbank_settlement_currency"),
                F.col("xchg_rate").cast("double").alias("p8_exchange_rate"),
                F.col("dbtr_agt_mmb_id").cast("string").alias("p8_instg_mmb_id"),
                F.col("cdtr_agt_mmb_id").cast("string").alias("p8_instd_mmb_id"),
                F.col("charge_amt").cast("double").alias("p8_charge_total_amount"),
                F.col("charge_ccy").cast("string").alias("p8_charge_currency"),
            ],
        )
        p2 = self._safe_load(
            self.pacs002_gold_path,
            select_expr=[
                F.col("message_id").cast("string").alias("p2_message_id"),
                F.col("tx_tenant_id").cast("string").alias("p2_tx_tenant_id"),
                F.col("instg_mmb_id").cast("string").alias("p2_instg_mmb_id"),
                F.col("instd_mmb_id").cast("string").alias("p2_instd_mmb_id"),
                F.col("charge_count").cast("int").alias("p2_charge_count"),
                F.col("charge_total_amount").cast("double").alias("p2_charge_total_amount"),
                F.col("charge_currency_hint").cast("string").alias("p2_charge_currency"),
                F.col("ingested_at_ts").cast("timestamp").alias("p2_ingested_at_ts"),
            ],
        )

        joined = tx
        if p8 is not None:
            joined = joined.join(p8, joined.end_to_end_id == p8.p8_end_to_end_id, "left")
        if p2 is not None:
            w = Window.partitionBy("p2_tx_tenant_id", "p2_message_id").orderBy(F.col("p2_ingested_at_ts").desc_nulls_last())
            p2 = p2.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
            joined = joined.join(
                p2,
                (joined.tx_msg_id == p2.p2_message_id) & (joined.tx_tenant_id == p2.p2_tx_tenant_id),
                "left",
            )

        joined = self.ensure_columns(
            joined,
            {
                "p8_tx_tenant_id": "string",
                "p8_dbtr_name": "string",
                "p8_dbtr_id": "string",
                "p8_cdtr_name": "string",
                "p8_cdtr_id": "string",
                "p8_dbtr_account_id": "string",
                "p8_cdtr_account_id": "string",
                "p8_instructed_amount": "double",
                "p8_instructed_currency": "string",
                "p8_interbank_settlement_amount": "double",
                "p8_interbank_settlement_currency": "string",
                "p8_exchange_rate": "double",
                "p8_instg_mmb_id": "string",
                "p8_instd_mmb_id": "string",
                "p8_charge_total_amount": "double",
                "p8_charge_currency": "string",
                "p2_tx_tenant_id": "string",
                "p2_instg_mmb_id": "string",
                "p2_instd_mmb_id": "string",
                "p2_charge_count": "int",
                "p2_charge_total_amount": "double",
                "p2_charge_currency": "string",
                "p2_ingested_at_ts": "timestamp",
            },
        )

        return (
            joined
            .withColumn("tx_tenant_id", F.coalesce(F.col("tx_tenant_id"), F.col("p2_tx_tenant_id"), F.col("p8_tx_tenant_id")))
            .withColumn("debtor_name", F.col("p8_dbtr_name"))
            .withColumn("debtor_id", F.col("p8_dbtr_id"))
            .withColumn("creditor_name", F.col("p8_cdtr_name"))
            .withColumn("creditor_id", F.col("p8_cdtr_id"))
            .withColumn("debtor_account_id", F.col("p8_dbtr_account_id"))
            .withColumn("creditor_account_id", F.col("p8_cdtr_account_id"))
            .withColumn("instructed_amount", F.coalesce(F.col("p8_instructed_amount"), F.col("tx_amount_from_event")))
            .withColumn("instructed_currency", F.coalesce(F.col("p8_instructed_currency"), F.col("tx_ccy_from_event")))
            .withColumn("interbank_settlement_amount", F.col("p8_interbank_settlement_amount"))
            .withColumn("interbank_settlement_currency", F.col("p8_interbank_settlement_currency"))
            .withColumn("exchange_rate", F.col("p8_exchange_rate"))
            .withColumn("instg_mmb_id", F.coalesce(F.col("p2_instg_mmb_id"), F.col("p8_instg_mmb_id")))
            .withColumn("instd_mmb_id", F.coalesce(F.col("p2_instd_mmb_id"), F.col("p8_instd_mmb_id")))
            .withColumn("charge_count", F.coalesce(F.col("p2_charge_count"), F.when(F.col("p8_charge_total_amount").isNotNull(), F.lit(1)).otherwise(F.lit(0))).cast("int"))
            .withColumn("charge_total_amount", F.coalesce(F.col("p2_charge_total_amount"), F.col("p8_charge_total_amount"), F.lit(0.0)).cast("double"))
            .withColumn("charge_currency", F.coalesce(F.col("p2_charge_currency"), F.col("p8_charge_currency")))
        )

    def _finalize_schema(self, df: DataFrame) -> DataFrame:
        """Cast and select the final view schema."""
        has_source_file = "source_file_path" in df.columns
        has_record_hash = "record_hash" in df.columns

        return df.select(
            F.col("transaction_id").cast("string").alias("transaction_id"),
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

        # 2. Normalize Ozone transaction columns
        tx = self._normalize_transactions(tx)

        # 3. Join PACS enrichment without changing the output schema
        tx = self._join_payment_enrichment(tx)

        # 4. Finalize schema
        tx_detail_view = self._finalize_schema(tx)

        # 5. Write Hudi view — transaction_id is the primary key
        self.write_hudi(
            tx_detail_view,
            self.view_path,
            self.hudi_opts(
                "vw_transaction_detail",
                record_key="transaction_id",
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
