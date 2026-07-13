"""
transactions.py
---------------
Bronze -> Silver -> Gold ETL for event-history transactions from Ozone.

The source dataframe is the raw event-history transactions table:

    amt, ccy, credttm, destination, endtoendid, msgid,
    source, tenantid, transaction, txsts, txtp

This ETL intentionally does not read or derive anything from payment message
tables. The original Ozone column names are preserved across the transaction
layers.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .BaseETL import BaseETL


class TransactionsETL(BaseETL):
    """Full Bronze -> Silver -> Gold pipeline for Ozone event-history transactions."""

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
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _make_pk(tx_type_col: str, e2e_col: str) -> Column:
        """Build transaction_id as TxTp || EndToEndId."""
        return F.concat_ws(
            "||",
            F.coalesce(F.col(tx_type_col), F.lit("")),
            F.coalesce(F.col(e2e_col), F.lit("")),
        )

    @staticmethod
    def _event_ts(col_name: str) -> Column:
        """Parse the ISO timestamp emitted by Ozone event history."""
        return F.to_timestamp(F.regexp_replace(F.col(col_name).cast("string"), "Z$", ""))

    # ------------------------------------------------------------------
    # Bronze
    # ------------------------------------------------------------------

    def bronze(self, source_path: str) -> str:
        raw = self.spark.read.json(source_path)

        bronze = (
            raw
            .withColumn("amt", F.col("amt").cast("double"))
            .withColumn("ccy", F.col("ccy").cast("string"))
            .withColumn("credttm", F.col("credttm").cast("string"))
            .withColumn("destination", F.col("destination").cast("string"))
            .withColumn("endtoendid", F.col("endtoendid").cast("string"))
            .withColumn("msgid", F.col("msgid").cast("string"))
            .withColumn("source", F.col("source").cast("string"))
            .withColumn("tenantid", F.col("tenantid").cast("string"))
            .withColumn("transaction", F.col("transaction").cast("string"))
            .withColumn("txsts", F.col("txsts").cast("string"))
            .withColumn("txtp", F.col("txtp").cast("string"))
            .withColumn("event_ts", self._event_ts("credttm"))
            .withColumn("event_date", F.to_date("event_ts"))
            .withColumn("created_at_epoch_ms", (F.col("event_ts").cast("long") * F.lit(1000)).cast("long"))
            .withColumn("transaction_id", self._make_pk("txtp", "endtoendid"))
            .withColumn("ingested_at_ts", F.current_timestamp())
            .withColumn("source_file_path", F.input_file_name())
            .withColumn(
                "record_hash",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.coalesce(F.col("amt").cast("string"), F.lit("")),
                        F.coalesce(F.col("ccy"), F.lit("")),
                        F.coalesce(F.col("credttm"), F.lit("")),
                        F.coalesce(F.col("destination"), F.lit("")),
                        F.coalesce(F.col("endtoendid"), F.lit("")),
                        F.coalesce(F.col("msgid"), F.lit("")),
                        F.coalesce(F.col("source"), F.lit("")),
                        F.coalesce(F.col("tenantid"), F.lit("")),
                        F.coalesce(F.col("txsts"), F.lit("")),
                        F.coalesce(F.col("txtp"), F.lit("")),
                    ),
                    256,
                ),
            )
            .withColumn("_row_payload_json", F.to_json(F.struct(*[F.col(c) for c in raw.columns])))
        )

        self.write_hudi(
            bronze,
            self.bronze_path,
            self.hudi_opts("bronze_transactions", "transaction_id", "ingested_at_ts"),
        )
        print(f"[TransactionsETL] Bronze written -> {self.bronze_path}")
        return self.bronze_path

    # ------------------------------------------------------------------
    # Silver
    # ------------------------------------------------------------------

    def silver(self) -> str:
        b = self.spark.read.format("hudi").load(self.bronze_path)

        s = (
            b
            .withColumn("amt", F.col("amt").cast("double"))
            .withColumn("ccy", F.col("ccy").cast("string"))
            .withColumn("credttm", F.col("credttm").cast("string"))
            .withColumn("destination", F.col("destination").cast("string"))
            .withColumn("endtoendid", F.col("endtoendid").cast("string"))
            .withColumn("msgid", F.col("msgid").cast("string"))
            .withColumn("source", F.col("source").cast("string"))
            .withColumn("tenantid", F.col("tenantid").cast("string"))
            .withColumn("transaction", F.col("transaction").cast("string"))
            .withColumn("txsts", F.col("txsts").cast("string"))
            .withColumn("txtp", F.col("txtp").cast("string"))
            .withColumn("event_ts", F.col("event_ts").cast("timestamp"))
            .withColumn("event_date", F.to_date("event_ts"))
            .withColumn("created_at_epoch_ms", F.col("created_at_epoch_ms").cast("long"))
            .withColumn("transaction_id", self._make_pk("txtp", "endtoendid"))
        )

        w = Window.partitionBy("transaction_id").orderBy(F.col("ingested_at_ts").desc())
        silver = s.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

        self.write_hudi(
            silver,
            self.silver_path,
            self.hudi_opts("silver_transactions", "transaction_id", "ingested_at_ts"),
        )
        print(f"[TransactionsETL] Silver written -> {self.silver_path}")
        return self.silver_path

    # ------------------------------------------------------------------
    # Gold
    # ------------------------------------------------------------------

    def gold(self) -> str:
        s = self.spark.read.format("hudi").load(self.silver_path)

        gold = (
            s
            .withColumn(
                "event_to_ingest_ms",
                F.when(
                    F.col("event_ts").isNotNull(),
                    (F.col("ingested_at_ts").cast("long") - F.col("event_ts").cast("long")) * 1000,
                ).otherwise(F.lit(None).cast("long")),
            )
            .select(
                F.col("transaction_id").cast("string").alias("transaction_id"),
                F.col("amt").cast("double").alias("amt"),
                F.col("ccy").cast("string").alias("ccy"),
                F.col("credttm").cast("string").alias("credttm"),
                F.col("destination").cast("string").alias("destination"),
                F.col("endtoendid").cast("string").alias("endtoendid"),
                F.col("msgid").cast("string").alias("msgid"),
                F.col("source").cast("string").alias("source"),
                F.col("tenantid").cast("string").alias("tenantid"),
                F.col("txsts").cast("string").alias("txsts"),
                F.col("txtp").cast("string").alias("txtp"),
                F.col("event_ts").cast("timestamp").alias("event_ts"),
                F.col("event_date").cast("date").alias("event_date"),
                F.col("created_at_epoch_ms").cast("long").alias("created_at_epoch_ms"),
                F.col("ingested_at_ts").cast("timestamp").alias("ingested_at_ts"),
                F.col("event_to_ingest_ms").cast("long").alias("event_to_ingest_ms"),
                F.col("source_file_path").cast("string").alias("source_file_path"),
                F.col("record_hash").cast("string").alias("record_hash"),
            )
        )

        bad = [c for c, t in gold.dtypes if t.startswith(("array", "struct", "map"))]
        if bad:
            raise RuntimeError(f"[TransactionsETL] Gold contains non-scalar cols: {bad}")

        self.write_hudi(
            gold,
            self.gold_path,
            self.hudi_opts(
                "transactions",
                "transaction_id",
                "ingested_at_ts",
                payload_class="org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
            ),
        )
        print(f"[TransactionsETL] Gold written -> {self.gold_path}")
        return self.gold_path

    # ------------------------------------------------------------------
    # Orchestrator
    # ------------------------------------------------------------------

    def run(self, source_path: str) -> str:
        print(f"[TransactionsETL] Starting Bronze -> Silver -> Gold from {source_path}")
        self.bronze(source_path)
        self.silver()
        self.gold()
        print("[TransactionsETL] ETL complete.")
        return self.gold_path
