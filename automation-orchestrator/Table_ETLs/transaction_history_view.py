from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .BaseETL import BaseETL


class TransactionHistoryViewETL(BaseETL):
    """
    Transaction History View builder.

    Reads gold/transactions (sourced from event_history.transaction), bridges
    party display names from gold/pacs008, joins optional alerts/cases/tasks,
    expands to entity level, and writes event + day/week/month/year
    aggregate rows to Hudi.

    Uses transaction_id (TxTp||endToEndId) as the primary key.
    """

    def __init__(self, spark, warehouse_root: str) -> None:
        super().__init__(spark, warehouse_root)
        self.views_root = f"{self.warehouse_root}/views"
        self.view_path = f"{self.views_root}/vw_transaction_history"
        self.transactions_gold_path = f"{self.warehouse_root}/gold/transactions"
        self.pacs008_gold_path = f"{self.warehouse_root}/gold/pacs008"

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

    def _extract_base_from_tables(self, tx: DataFrame) -> DataFrame:
        """Build the base transaction frame from gold/transactions.

        Account and entity ids, amount and currency come from the TMS
        record. Party display names are not stored by TMS, so they are
        bridged from gold/pacs008 on (end_to_end_id, tenant) until Gold-layer
        enrichment lands (#110 Req 6).
        """
        base_tx = (
            tx
            .withColumn("end_to_end_id", F.col("endtoendid").cast("string"))
            .withColumn("tenant_id", F.col("tenantid").cast("string"))
        )

        p8 = self._safe_load(
            self.pacs008_gold_path,
            select_expr=[
                F.col("end_to_end_id").cast("string").alias("p8_end_to_end_id"),
                F.col("tx_tenant_id").cast("string").alias("p8_tenant_id"),
                F.col("dbtr_name").cast("string").alias("p8_dbtr_name"),
                F.col("cdtr_name").cast("string").alias("p8_cdtr_name"),
            ],
        )

        joined = base_tx
        if p8 is not None:
            p8 = p8.dropDuplicates(["p8_tenant_id", "p8_end_to_end_id"])
            joined = joined.join(
                p8,
                (joined.end_to_end_id == p8.p8_end_to_end_id) & (joined.tenant_id == p8.p8_tenant_id),
                "left",
            )

        joined = self.ensure_columns(joined, {"p8_dbtr_name": "string", "p8_cdtr_name": "string"})

        return (
            joined
            .select(
                F.col("transaction_id").cast("string").alias("transaction_id"),
                F.col("end_to_end_id").cast("string").alias("end_to_end_id"),
                F.col("tenant_id").cast("string").alias("tenant_id"),
                F.col("txtp").cast("string").alias("tx_type"),
                F.col("msgid").cast("string").alias("tx_msg_id"),
                F.col("event_ts").cast("timestamp").alias("event_ts"),
                F.col("event_date").cast("date").alias("event_date"),
                F.col("amt").cast("double").alias("tx_amount"),
                F.col("ccy").cast("string").alias("tx_ccy"),
                F.col("p8_dbtr_name").cast("string").alias("dbtr_name"),
                F.col("debtor_entity_id").cast("string").alias("dbtr_id"),
                F.col("p8_cdtr_name").cast("string").alias("cdtr_name"),
                F.col("creditor_entity_id").cast("string").alias("cdtr_id"),
                F.col("debtor_account_id").cast("string").alias("dbtr_account_id"),
                F.col("creditor_account_id").cast("string").alias("cdtr_account_id"),
                F.col("source_file_path").cast("string").alias("source_file_path"),
                F.col("record_hash").cast("string").alias("record_hash"),
            )
            .filter(F.col("event_ts").isNotNull())
        )

    def _join_flags(self, base: DataFrame) -> DataFrame:
        """Join optional alerts, cases, and tasks to add is_alerted / is_investigated."""
        alerts_g = self._safe_load(
            f"{self.warehouse_root}/gold/alerts",
            select_expr=[
                F.col("tx_msg_id").cast("string").alias("tx_msg_id"),
                F.col("alert_id").cast("long").alias("alert_id"),
                F.col("case_id").cast("long").alias("case_id"),
            ],
        )
        if alerts_g is not None:
            alerts_g = alerts_g.dropna(subset=["tx_msg_id"]).dropDuplicates(["tx_msg_id"])

        # ------------------------------------------------------------------
        # CASES — status column is optional
        # ------------------------------------------------------------------
        cases_g = self._safe_load(f"{self.warehouse_root}/gold/cases")
        if cases_g is not None:
            if "status" in cases_g.columns:
                cases_g = cases_g.select(
                    F.col("case_id").cast("long").alias("case_id"),
                    F.col("status").cast("string").alias("case_status"),
                )
            else:
                cases_g = cases_g.select(
                    F.col("case_id").cast("long").alias("case_id"),
                    F.lit(None).cast("string").alias("case_status"),
                )
            cases_g = cases_g.dropDuplicates(["case_id"])

        tasks_g = self._safe_load(
            f"{self.warehouse_root}/gold/tasks",
            select_expr=[
                F.col("case_id").cast("long").alias("case_id"),
                F.col("is_completed").cast("int").alias("is_completed"),
            ],
        )

        flags = base
        if alerts_g is not None:
            flags = flags.join(alerts_g, "tx_msg_id", "left")
        if cases_g is not None:
            flags = flags.join(cases_g, "case_id", "left")
        if tasks_g is not None:
            tasks_agg = tasks_g.groupBy("case_id").agg(
                F.max("is_completed").alias("has_completed_task")
            )
            flags = flags.join(tasks_agg, "case_id", "left")

        # ------------------------------------------------------------------
        # Guard: ensure these columns exist even when the table was missing
        # ------------------------------------------------------------------
        if "alert_id" not in flags.columns:
            flags = flags.withColumn("alert_id", F.lit(None).cast("long"))
        if "case_status" not in flags.columns:
            flags = flags.withColumn("case_status", F.lit(None).cast("string"))
        if "has_completed_task" not in flags.columns:
            flags = flags.withColumn("has_completed_task", F.lit(0).cast("int"))

        return (
            flags.withColumn(
                "is_alerted",
                F.when(F.col("alert_id").isNotNull(), F.lit(1)).otherwise(F.lit(0)),
            )
            .withColumn(
                "is_investigated",
                F.when(
                    (F.col("case_status").isNotNull())
                    | (F.coalesce(F.col("has_completed_task"), F.lit(0)) == 1),
                    F.lit(1),
                ).otherwise(F.lit(0)),
            )
            .drop("has_completed_task", "alert_id", "case_status", "case_id")
        )

    def _expand_entities(self, df: DataFrame) -> DataFrame:
        """Explode each transaction into entity rows (accounts + counterparties)."""
        return (
            df.select(
                "*",
                F.array(
                    F.when(
                        F.col("dbtr_account_id").isNotNull(),
                        F.struct(
                            F.lit("ACCOUNT").alias("entity_type"),
                            F.lit("DEBTOR").alias("entity_role"),
                            F.col("dbtr_account_id").alias("entity_id"),
                            F.col("dbtr_name").alias("entity_name"),
                        ),
                    ),
                    F.when(
                        F.col("cdtr_account_id").isNotNull(),
                        F.struct(
                            F.lit("ACCOUNT").alias("entity_type"),
                            F.lit("CREDITOR").alias("entity_role"),
                            F.col("cdtr_account_id").alias("entity_id"),
                            F.col("cdtr_name").alias("entity_name"),
                        ),
                    ),
                    F.when(
                        F.col("dbtr_id").isNotNull(),
                        F.struct(
                            F.lit("COUNTERPARTY").alias("entity_type"),
                            F.lit("DEBTOR").alias("entity_role"),
                            F.col("dbtr_id").alias("entity_id"),
                            F.col("dbtr_name").alias("entity_name"),
                        ),
                    ),
                    F.when(
                        F.col("cdtr_id").isNotNull(),
                        F.struct(
                            F.lit("COUNTERPARTY").alias("entity_type"),
                            F.lit("CREDITOR").alias("entity_role"),
                            F.col("cdtr_id").alias("entity_id"),
                            F.col("cdtr_name").alias("entity_name"),
                        ),
                    ),
                ).alias("entities"),
            )
            .withColumn("entity", F.explode(F.expr("filter(entities, x -> x is not null)")))
            .drop("entities")
            .withColumn("entity_type", F.col("entity.entity_type"))
            .withColumn("entity_role", F.col("entity.entity_role"))
            .withColumn("entity_id", F.col("entity.entity_id"))
            .withColumn("entity_name", F.col("entity.entity_name"))
            .drop("entity")
        )

    def _build_events(self, df: DataFrame) -> DataFrame:
        """Add window calculations and mark as EVENT rows."""
        w_recent = Window.partitionBy("entity_type", "entity_role", "entity_id").orderBy(
            F.col("event_ts").desc()
        )
        w_cum = (
            Window.partitionBy("entity_type", "entity_role", "entity_id")
            .orderBy(F.col("event_ts").asc())
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )

        # Only count actual payment instructions (pacs.008 / pain.001) in cumulative totals.
        # pacs.002 is a status report for the same transaction, not a new transaction.
        is_payment_tx = F.col("tx_type").isin(["pacs.008.001.10", "pain.001.001.11"])

        return (
            df.withColumn("recent_rank_desc", F.row_number().over(w_recent))
            .withColumn(
                "cum_tx_count",
                F.sum(F.when(is_payment_tx, F.lit(1)).otherwise(F.lit(0))).over(w_cum),
            )
            .withColumn(
                "cum_tx_amount",
                F.sum(
                    F.when(
                        is_payment_tx,
                        F.coalesce(F.col("tx_amount"), F.lit(0.0)),
                    ).otherwise(F.lit(0.0))
                ).over(w_cum),
            )
            .withColumn("row_type", F.lit("EVENT"))
            .withColumn("bucket_granularity", F.lit(None).cast("string"))
            .withColumn("bucket_start", F.lit(None).cast("timestamp"))
            .withColumn("bucket_tx_count", F.lit(None).cast("long"))
            .withColumn("bucket_tx_amount", F.lit(None).cast("double"))
        )
    
    def _build_agg(self, df: DataFrame, granularity: str) -> DataFrame:
        """Roll up entity rows to a given time granularity with deterministic PK."""
        bucket_start = F.date_trunc(granularity, F.col("event_ts"))

        # Only aggregate actual payment instructions, not status reports
        is_payment_tx = F.col("tx_type").isin(["pacs.008.001.10", "pain.001.001.11"])

        return (
            df.withColumn("bucket_start", bucket_start)
            .filter(is_payment_tx)  # <-- EXCLUDE pacs.002 from aggregates
            .groupBy("entity_type", "entity_role", "entity_id", "bucket_start")
            .agg(
                F.count("*").cast("long").alias("bucket_tx_count"),
                F.sum(F.coalesce(F.col("tx_amount"), F.lit(0.0)))
                    .cast("double")
                    .alias("bucket_tx_amount"),
                F.max("event_date").alias("event_date"),
                F.max("tenant_id").alias("tenant_id"),
            )
            .withColumn("row_type", F.lit("AGG"))
            .withColumn("bucket_granularity", F.lit(granularity))

            .withColumn(
                "transaction_id",
                F.concat_ws(
                     "||",
                    F.lit("AGG"),
                    F.col("entity_type"),
                    F.col("entity_role"),
                    F.col("entity_id"),
                    F.col("bucket_granularity"),
                    F.col("bucket_start").cast("string"),
                ),
            )

            # Remaining columns (unchanged)
            .withColumn("recent_rank_desc", F.lit(None).cast("int"))
            .withColumn("cum_tx_count", F.lit(None).cast("long"))
            .withColumn("cum_tx_amount", F.lit(None).cast("double"))
            .withColumn("end_to_end_id", F.lit(None).cast("string"))
            .withColumn("tx_type", F.lit(None).cast("string"))
            .withColumn("tx_msg_id", F.lit(None).cast("string"))
            .withColumn("event_ts", F.lit(None).cast("timestamp"))
            .withColumn("tx_amount", F.lit(None).cast("double"))
            .withColumn("tx_ccy", F.lit(None).cast("string"))
            .withColumn("entity_name", F.lit(None).cast("string"))
            .withColumn("is_alerted", F.lit(None).cast("int"))
            .withColumn("is_investigated", F.lit(None).cast("int"))
            .withColumn("source_file_path", F.lit(None).cast("string"))
            .withColumn("record_hash", F.lit(None).cast("string"))
        )
    
    def _add_pk(self, df: DataFrame) -> DataFrame:
        """Add unique record key for Hudi + ingestion timestamp.

        transaction_id is kept as tx_type||end_to_end_id from bronze.
        _record_key ensures each entity row is unique for Hudi upserts.
        """
        return (
            df.withColumn(
                "_record_key",
                F.concat_ws(
                    "||",
                    F.col("transaction_id"),
                    F.col("entity_type"),
                    F.col("entity_role"),
                    F.col("entity_id"),
                ),
            )
            .withColumn("ingested_at_ts", F.current_timestamp())
        )

    # ------------------------------------------------------------------
    # BRONZE  (main view build)
    # ------------------------------------------------------------------

    def bronze(self, source_path: str = "") -> str:
        """
        Build vw_transaction_history from gold/transactions.

        *source_path* is ignored (reads from warehouse gold path).
        """
        print("[TransactionHistoryViewETL] Creating Transaction History View...")

        # 1. Load gold transactions
        tx = self.spark.read.format("hudi").load(self.transactions_gold_path)

        # 2. Build base frame from gold transactions plus pacs name bridge
        base = self._extract_base_from_tables(tx)

        # 5. Join flags
        flags = self._join_flags(base)

        # 6. Expand entities
        entity_rows = self._expand_entities(flags)

        # 7. Build events + aggregates
        events = self._build_events(entity_rows)
        agg_day = self._build_agg(entity_rows, "day")
        agg_week = self._build_agg(entity_rows, "week")
        agg_month = self._build_agg(entity_rows, "month")
        agg_year = self._build_agg(entity_rows, "year")

        # 8. Union all
        view_df = (
            events.select(
                "transaction_id",
                "entity_type",
                "entity_role",
                "entity_id",
                "entity_name",
                "end_to_end_id",
                "tenant_id",
                "tx_type",
                "tx_msg_id",
                "event_ts",
                "event_date",
                "tx_amount",
                "tx_ccy",
                "is_alerted",
                "is_investigated",
                "recent_rank_desc",
                "cum_tx_count",
                "cum_tx_amount",
                "row_type",
                "bucket_granularity",
                "bucket_start",
                "bucket_tx_count",
                "bucket_tx_amount",
                "source_file_path",
                "record_hash",
            )
            .unionByName(agg_day, allowMissingColumns=True)
            .unionByName(agg_week, allowMissingColumns=True)
            .unionByName(agg_month, allowMissingColumns=True)
            .unionByName(agg_year, allowMissingColumns=True)
        )

        # 9. Ingest timestamp
        view_df = self._add_pk(view_df)

        # 10. Write Hudi view — transaction_id is the primary key
        self.write_hudi(
            view_df,
            self.view_path,
            self.hudi_opts(
                "vw_transaction_history",
                record_key="_record_key",
                precombine="ingested_at_ts",
            ),
        )
        print(f"[TransactionHistoryViewETL] View written → {self.view_path}")
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
        print("[TransactionHistoryViewETL] Starting view build")
        self.bronze(source_path)
        print("[TransactionHistoryViewETL] View build complete.")
        return self.view_path
