"""
transactions.py
---------------
Bronze → Silver → Gold ETL for the Transactions domain.

Transactions are derived from PACS bronze tables.  Two modes:
  - "from_pacs" : build from pacs008/pacs002 bronze (default when triggered by etl_pacs)
  - "join"      : join a raw transaction feed against the PACS bronze tables

Primary key
-----------
The record key is a composite of TxTp (message type) and EndToEndId, stored as
a single derived column `transaction_id`:

    transaction_id = TxTp + "||" + endToEndId   (plain string concatenation)

For example: "pacs.008.001.10||2024-ABC-123-XYZ"

This is human-readable, debuggable, and maps directly back to the source message.
It is used as the Hudi record key at every layer (bronze, silver, gold).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .BaseETL import BaseETL


class TransactionsETL(BaseETL):
    """Full Bronze → Silver → Gold pipeline for transactions."""

    @property
    def bronze_path(self) -> str:
        return f"{self.warehouse_root}/bronze/transactions"

    @property
    def silver_path(self) -> str:
        return f"{self.warehouse_root}/silver/transactions"

    @property
    def gold_path(self) -> str:
        return f"{self.warehouse_root}/gold/transactions"

    # ------------------------------------------------------------------
    # COMPOSITE KEY HELPER
    # ------------------------------------------------------------------

    @staticmethod
    def _make_pk(tx_type_col: str, e2e_col: str) -> "Column":
        """
        Build the plain composite primary key.

        transaction_id = TxTp + "||" + endToEndId

        For example: "pacs.008.001.10||2024-ABC-123-XYZ"
        Both inputs are coalesced to empty string so a null never breaks
        the concat or produces an ambiguous key.
        """
        return F.concat_ws(
            "||",
            F.coalesce(F.col(tx_type_col), F.lit("")),
            F.coalesce(F.col(e2e_col),      F.lit("")),
        )

    # ------------------------------------------------------------------
    # INTERNAL: build rows from PACS bronze
    # ------------------------------------------------------------------

    def _transactions_from_pacs(self, df_pacs: DataFrame, source_label: str) -> DataFrame:
        """
        Derive canonical transaction rows from a PACS bronze DataFrame.

        TxTp is extracted from the embedded JSON so the composite PK can be
        formed immediately — no synthetic integer needed.
        """
        created_ts    = F.coalesce(F.col("credttm_ts"), F.col("ingested_at_ts"), F.current_timestamp())
        created_at_ms = (created_ts.cast("long") * F.lit(1000)).cast("long")

        if "document_json" in df_pacs.columns:
            tx_data = F.coalesce(F.col("document_json"), F.col("document").cast("string")).cast("string")
        else:
            tx_data = F.col("document").cast("string")

        # Extract TxTp from the JSON payload so the PK is available at ingest time.
        tx_type_from_json = F.get_json_object(tx_data, "$.TxTp")

        return (
            df_pacs
            .filter(F.col("end_to_end_id").isNotNull() & F.col("tenant_id").isNotNull())
            .select(
                created_at_ms.alias("createdAt"),
                F.col("end_to_end_id").cast("string").alias("endToEndId"),
                F.col("tenant_id").cast("string").alias("tenantId"),
                tx_data.alias("transactionData"),
                tx_type_from_json.alias("tx_type_raw"),
            )
        )

    # ------------------------------------------------------------------
    # BRONZE
    # ------------------------------------------------------------------

    def bronze(self, source_path: str, mode: str = "from_pacs") -> str:
        pacs008_bronze_path = f"{self.warehouse_root}/bronze/pacs008"
        pacs002_bronze_path = f"{self.warehouse_root}/bronze/pacs002"

        df_pacs008 = self.spark.read.format("hudi").load(pacs008_bronze_path)
        df_pacs002 = self.spark.read.format("hudi").load(pacs002_bronze_path)

        if mode == "from_pacs":
            raw = (
                self._transactions_from_pacs(df_pacs008, "pacs008")
                .unionByName(self._transactions_from_pacs(df_pacs002, "pacs002"))
            )
        else:
            raw = self._join_mode(source_path, df_pacs008, df_pacs002)

        bronze = (
            raw
            .withColumn("createdAt",       F.col("createdAt").cast("long"))
            .withColumn("endToEndId",      F.col("endToEndId").cast("string"))
            .withColumn("tenantId",        F.col("tenantId").cast("string"))
            .withColumn("transactionData", F.col("transactionData").cast("string"))
            .withColumn("created_at_ts",   F.current_timestamp())
            .withColumn("source_file_path", F.lit(source_path))
            # Resolve tx_type from the pre-extracted column or re-parse the JSON
            .withColumn(
                "tx_type",
                F.coalesce(
                    F.col("tx_type_raw"),
                    F.get_json_object("transactionData", "$.TxTp"),
                ),
            )
            # Composite PK: TxTp || endToEndId (plain string, no hashing)
            .withColumn("transaction_id", self._make_pk("tx_type", "endToEndId"))
            .withColumn(
                "record_hash",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.col("endToEndId"),
                        F.col("tenantId"),
                        F.col("createdAt").cast("string"),
                        F.col("transactionData"),
                    ),
                    256,
                ),
            )
            .withColumn("_row_payload_json", F.to_json(F.struct("*")))
            .drop("tx_type_raw")
        )

        self.write_hudi(
            bronze,
            self.bronze_path,
            # Hudi record key is the composite PK column
            self.hudi_opts("transactions", "transaction_id", "created_at_ts"),
        )
        print(f"[TransactionsETL] Bronze written → {self.bronze_path}")
        return self.bronze_path

    # ------------------------------------------------------------------
    # JOIN MODE (legacy)
    # ------------------------------------------------------------------

    def _join_mode(self, source_path: str, df_pacs008: DataFrame, df_pacs002: DataFrame) -> DataFrame:
        """Legacy: join raw transaction feed against PACS bronze tables."""
        df_tx = self.spark.read.json(source_path)

        pacs008_rows = (
            df_tx.filter(F.col("txtp") == "pacs.008.001.10")
            .join(
                df_pacs008.select(F.col("end_to_end_id").alias("p8_id"), F.col("document")),
                df_tx.endtoendid == F.col("p8_id"),
                "inner",
            )
            .select(
                (F.unix_timestamp("credttm") * 1000).alias("createdAt"),
                F.col("endtoendid").alias("endToEndId"),
                F.col("tenantid").alias("tenantId"),
                F.col("document").alias("transactionData"),
                F.col("txtp").alias("tx_type_raw"),
            )
        )

        pacs002_rows = (
            df_tx.filter(F.col("txtp") == "pacs.002.001.12")
            .join(
                df_pacs002.select(F.col("end_to_end_id").alias("p2_id"), F.col("document")),
                df_tx.endtoendid == F.col("p2_id"),
                "inner",
            )
            .select(
                (F.unix_timestamp("credttm") * 1000).alias("createdAt"),
                F.col("endtoendid").alias("endToEndId"),
                F.col("tenantid").alias("tenantId"),
                F.col("document").alias("transactionData"),
                F.col("txtp").alias("tx_type_raw"),
            )
        )

        combined = pacs008_rows.unionByName(pacs002_rows)

        if combined.rdd.isEmpty():
            print("[TransactionsETL] Join produced 0 rows; falling back to from_pacs.")
            return (
                self._transactions_from_pacs(df_pacs008, "pacs008")
                .unionByName(self._transactions_from_pacs(df_pacs002, "pacs002"))
            )

        return combined

    # ------------------------------------------------------------------
    # SILVER
    # ------------------------------------------------------------------

    def silver(self) -> str:
        b = self.spark.read.format("hudi").load(self.bronze_path)

        s = (
            b
            # Re-resolve tx_type in case it was null at bronze time
            .withColumn(
                "tx_type",
                F.coalesce(
                    F.col("tx_type"),
                    F.get_json_object("transactionData", "$.TxTp"),
                ),
            )
            .withColumn("tx_tenant_id",  F.get_json_object("transactionData", "$.TenantId"))
            # pacs.008 paths
            .withColumn("msg_id_008",  F.get_json_object("transactionData", "$.FIToFICstmrCdtTrf.GrpHdr.MsgId"))
            .withColumn("created_008", F.get_json_object("transactionData", "$.FIToFICstmrCdtTrf.GrpHdr.CreDtTm"))
            # pacs.002 paths
            .withColumn("msg_id_002",  F.get_json_object("transactionData", "$.FIToFIPmtSts.GrpHdr.MsgId"))
            .withColumn("created_002", F.get_json_object("transactionData", "$.FIToFIPmtSts.GrpHdr.CreDtTm"))
            .withColumn("tx_msg_id",     F.coalesce("msg_id_008", "msg_id_002"))
            .withColumn("tx_created_ts", F.to_timestamp(F.coalesce("created_008", "created_002")))
            .withColumn("tx_status",     F.get_json_object("transactionData", "$.FIToFIPmtSts.TxInfAndSts.TxSts"))
            .withColumn("tx_accept_ts",  F.to_timestamp(F.get_json_object("transactionData", "$.FIToFIPmtSts.TxInfAndSts.AccptncDtTm")))
            .withColumn("event_ts",      F.col("tx_created_ts"))
            .withColumn("event_date",    F.to_date("event_ts"))
            .withColumn(
                "instg_mmb_id",
                F.coalesce(
                    F.get_json_object("transactionData", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.DbtrAgt.FinInstnId.ClrSysMmbId.MmbId"),
                    F.get_json_object("transactionData", "$.FIToFIPmtSts.TxInfAndSts.InstgAgt.FinInstnId.ClrSysMmbId.MmbId"),
                ),
            )
            .withColumn(
                "instd_mmb_id",
                F.coalesce(
                    F.get_json_object("transactionData", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAgt.FinInstnId.ClrSysMmbId.MmbId"),
                    F.get_json_object("transactionData", "$.FIToFIPmtSts.TxInfAndSts.InstdAgt.FinInstnId.ClrSysMmbId.MmbId"),
                ),
            )
            .withColumn("tx_amount", F.get_json_object("transactionData", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.InstdAmt.Amt.Amt").cast("double"))
            .withColumn("tx_ccy",    F.get_json_object("transactionData", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.InstdAmt.Amt.Ccy").cast("string"))
            .withColumn("charges_002_json", F.get_json_object("transactionData", "$.FIToFIPmtSts.TxInfAndSts.ChrgsInf"))
            .withColumn("charges_008_amt",  F.get_json_object("transactionData", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.ChrgsInf.Amt.Amt").cast("double"))
            .withColumn(
                "charge_count",
                F.when(
                    F.col("charges_002_json").isNotNull(),
                    F.size(F.from_json(
                        "charges_002_json",
                        "array<struct<Agt:struct<FinInstnId:struct<ClrSysMmbId:struct<MmbId:string>>>,Amt:struct<Amt:double,Ccy:string>>>",
                    )),
                ).otherwise(
                    F.when(F.col("charges_008_amt").isNotNull(), F.lit(1)).otherwise(F.lit(0))
                ),
            )
            # Recompute composite PK with the now-resolved tx_type (covers null-at-bronze cases)
            .withColumn("transaction_id", self._make_pk("tx_type", "endToEndId"))
        )

        # Dedup: keep latest per composite key
        w = Window.partitionBy("transaction_id").orderBy(F.col("created_at_ts").desc())
        silver = s.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

        self.write_hudi(
            silver,
            self.silver_path,
            self.hudi_opts("silver_transactions", "transaction_id", "created_at_ts"),
        )
        print(f"[TransactionsETL] Silver written → {self.silver_path}")
        return self.silver_path

    # ------------------------------------------------------------------
    # GOLD
    # ------------------------------------------------------------------

    def gold(self) -> str:
        s = self.spark.read.format("hudi").load(self.silver_path)

        gold = (
            s
            .withColumn(
                "event_to_ingest_ms",
                F.when(
                    F.col("event_ts").isNotNull(),
                    (F.col("created_at_ts").cast("long") - F.col("event_ts").cast("long")) * 1000,
                ).otherwise(F.lit(None).cast("long")),
            )
            .select(
                # Composite PK carried through to gold
                F.col("transaction_id").cast("string").alias("transaction_id"),
                F.col("endToEndId").cast("string").alias("end_to_end_id"),
                F.col("tenantId").cast("string").alias("tenant_id"),
                F.col("tx_type").cast("string").alias("tx_type"),
                F.col("tx_msg_id").cast("string").alias("tx_msg_id"),
                F.col("tx_status").cast("string").alias("tx_status"),
                F.col("tx_amount").cast("double").alias("tx_amount"),
                F.col("tx_ccy").cast("string").alias("tx_ccy"),
                F.col("instg_mmb_id").cast("string").alias("instg_mmb_id"),
                F.col("instd_mmb_id").cast("string").alias("instd_mmb_id"),
                F.col("charge_count").cast("int").alias("charge_count"),
                F.col("event_ts").cast("timestamp").alias("event_ts"),
                F.col("event_date").cast("date").alias("event_date"),
                F.col("created_at_ts").cast("timestamp").alias("ingested_at_ts"),
                F.col("event_to_ingest_ms").cast("long").alias("event_to_ingest_ms"),
                F.col("source_file_path").cast("string").alias("source_file_path"),
                F.col("record_hash").cast("string").alias("record_hash"),
            )
        )

        bad = [c for c, t in gold.dtypes if t.startswith(("array", "struct"))]
        if bad:
            raise RuntimeError(f"[TransactionsETL] Gold contains non-scalar cols: {bad}")

        gold_opts = {
            **self.hudi_opts("transactions", "transaction_id", "ingested_at_ts"),
            "hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
        }
        self.write_hudi(gold, self.gold_path, gold_opts)
        print(f"[TransactionsETL] Gold written → {self.gold_path}")
        return self.gold_path

    # ------------------------------------------------------------------
    # ORCHESTRATOR
    # ------------------------------------------------------------------

    def run(self, source_path: str, mode: str = "from_pacs") -> str:
        if mode not in {"join", "from_pacs"}:
            raise ValueError(f"mode must be 'join' or 'from_pacs' (got: {mode!r})")
        print(f"[TransactionsETL] Starting Bronze → Silver → Gold (mode={mode}) from {source_path}")
        self.bronze(source_path, mode=mode)
        self.silver()
        self.gold()
        print("[TransactionsETL] ETL complete.")
        return self.gold_path