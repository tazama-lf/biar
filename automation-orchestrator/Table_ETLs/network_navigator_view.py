from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .BaseETL import BaseETL


class NetworkNavigatorViewETL(BaseETL):
    """
    Network Navigator view builder.

    Reads gold/transactions (sourced from event_history.transaction), joins
    alerts/cases/tasks for flags, and produces time-bucketed edge tables for
    graph visualization. Account ids and debtor/creditor entity ids come from
    the TMS account hierarchy in gold/transactions; no pacs fields are needed.
    """

    BUCKET_GRANULARITIES = ("day", "week", "month", "year")

    def __init__(self, spark, warehouse_root: str) -> None:
        super().__init__(spark, warehouse_root)
        self.views_root = f"{self.warehouse_root}/views"
        self.accounts_edges_path = f"{self.views_root}/vw_tx_network_accounts_edges"
        self.counterparties_edges_path = f"{self.views_root}/vw_tx_network_counterparties_edges"
        self.holder_links_path = f"{self.views_root}/vw_counterparty_account_links"
        self.transactions_gold_path = f"{self.warehouse_root}/gold/transactions"

    @property
    def bronze_path(self) -> str:
        return self.views_root

    @property
    def silver_path(self) -> str:
        return self.views_root

    @property
    def gold_path(self) -> str:
        return self.views_root

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

    def _load_flags(self) -> DataFrame:
        """Load gold transactions + gold alerts/cases/tasks."""
        tx = self.spark.read.format("hudi").load(self.transactions_gold_path)

        # Only payment instructions form edges; pacs.002 is a status report
        # for the same payment and would double-count it.
        is_payment_tx = F.col("txtp").isin(["pacs.008.001.10", "pain.001.001.11"])
        base = (
            tx
            .filter(is_payment_tx)
            .select(
                F.col("transaction_id").cast("string").alias("transaction_id"),
                F.col("endtoendid").cast("string").alias("end_to_end_id"),
                F.col("tenantid").cast("string").alias("tenant_id"),
                F.col("txtp").cast("string").alias("tx_type"),
                F.col("msgid").cast("string").alias("tx_msg_id"),
                F.col("event_ts").cast("timestamp").alias("event_ts"),
                F.col("event_date").cast("date").alias("event_date"),
                F.col("amt").cast("double").alias("tx_amount"),
                F.col("ccy").cast("string").alias("tx_ccy"),
                F.col("debtor_entity_id").cast("string").alias("dbtr_id"),
                F.col("creditor_entity_id").cast("string").alias("cdtr_id"),
                F.col("debtor_account_id").cast("string").alias("dbtr_account_id"),
                F.col("creditor_account_id").cast("string").alias("cdtr_account_id"),
            )
            .filter(F.col("event_ts").isNotNull())
        )

        # ------------------------------------------------------------------
        # Join flags — optional upstream tables
        # ------------------------------------------------------------------
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
        else:
            # Ensure these columns exist for downstream joins & expressions
            flags = flags.withColumn("alert_id", F.lit(None).cast("long"))
            flags = flags.withColumn("case_id", F.lit(None).cast("long"))

        if cases_g is not None:
            flags = flags.join(cases_g, "case_id", "left")

        if tasks_g is not None:
            tasks_agg = tasks_g.groupBy("case_id").agg(
                F.max("is_completed").alias("has_completed_task")
            )
            flags = flags.join(tasks_agg, "case_id", "left")

        # ------------------------------------------------------------------
        # Guards: ensure columns exist even when upstream tables were missing
        # ------------------------------------------------------------------
        if "alert_id" not in flags.columns:
            flags = flags.withColumn("alert_id", F.lit(None).cast("long"))
        if "case_status" not in flags.columns:
            flags = flags.withColumn("case_status", F.lit(None).cast("string"))
        if "has_completed_task" not in flags.columns:
            flags = flags.withColumn("has_completed_task", F.lit(0).cast("int"))

        return (
            flags.withColumn(
                "is_alerted_tx",
                F.when(F.col("alert_id").isNotNull(), F.lit(1)).otherwise(F.lit(0)),
            )
            .withColumn(
                "is_investigated_tx",
                F.when(
                    (F.col("case_status").isNotNull())
                    | (F.coalesce(F.col("has_completed_task"), F.lit(0)) == 1),
                    F.lit(1),
                ).otherwise(F.lit(0)),
            )
            .drop("has_completed_task", "alert_id", "case_status", "case_id")
        )

    def _edge_bucket_agg(self, df: DataFrame, from_col: str, to_col: str) -> DataFrame:
        """Aggregate edges into day/week/month/year buckets."""
        df = df.filter(F.col(from_col).isNotNull() & F.col(to_col).isNotNull())

        def _agg(granularity: str) -> DataFrame:
            bstart = F.date_trunc(granularity, F.col("event_ts"))
            return (
                df.withColumn("bucket_granularity", F.lit(granularity))
                .withColumn("bucket_start", bstart)
                .groupBy(
                    "tenant_id",
                    "bucket_granularity",
                    "bucket_start",
                    F.col(from_col).alias("from_id"),
                    F.col(to_col).alias("to_id"),
                )
                .agg(
                    F.count("*").cast("long").alias("tx_count"),
                    F.sum(F.coalesce(F.col("tx_amount"), F.lit(0.0)))
                    .cast("double")
                    .alias("total_amount"),
                    F.max("tx_ccy").alias("currency_hint"),
                    F.min("event_ts").alias("first_event_ts"),
                    F.max("event_ts").alias("last_event_ts"),
                    F.max("is_alerted_tx").alias("is_alerted_edge"),
                    F.max("is_investigated_tx").alias("is_investigated_edge"),
                )
                .withColumn(
                    "active_window_sec",
                    (F.unix_timestamp("last_event_ts") - F.unix_timestamp("first_event_ts")).cast("long"),
                )
                .withColumn(
                    "tx_per_day",
                    F.when(
                        F.col("active_window_sec") > 0,
                        F.col("tx_count").cast("double")
                        / (F.col("active_window_sec").cast("double") / 86400.0),
                    ).otherwise(F.col("tx_count").cast("double")),
                )
            )

        result = _agg(self.BUCKET_GRANULARITIES[0])
        for gran in self.BUCKET_GRANULARITIES[1:]:
            result = result.unionByName(_agg(gran))
        return result

    def _add_pk_and_ingest(
        self,
        df: DataFrame,
        table_tag: str,
        from_col: str,
        to_col: str,
    ) -> DataFrame:
        """Add ingestion timestamp and deterministic PK."""
        return df.withColumn("ingested_at_ts", F.current_timestamp()).withColumn(
            "pk",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.lit(table_tag),
                    F.coalesce(F.col("tenant_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("bucket_granularity").cast("string"), F.lit("")),
                    F.coalesce(F.col("bucket_start").cast("string"), F.lit("")),
                    F.coalesce(F.col(from_col).cast("string"), F.lit("")),
                    F.coalesce(F.col(to_col).cast("string"), F.lit("")),
                ),
                256,
            ),
        )

    def _write_view(self, df: DataFrame, path: str, table_name: str) -> None:
        """Write a DataFrame as a Hudi view."""
        self.write_hudi(
            df,
            path,
            self.hudi_opts(table_name, record_key="pk", precombine="ingested_at_ts"),
        )
        print(f"[NetworkNavigatorViewETL] {table_name} written → {path}")

    # ------------------------------------------------------------------
    # BRONZE  (builds all 3 views)
    # ------------------------------------------------------------------

    def bronze(self, source_path: str = "") -> str:
        """Build all 3 Network Navigator views."""
        print("[NetworkNavigatorViewETL] Creating Network Navigator Views...")

        flags = self._load_flags()

        # 1. Account-to-Account edges
        accounts_edges = (
            self._edge_bucket_agg(flags, "dbtr_account_id", "cdtr_account_id")
            .withColumnRenamed("from_id", "from_account_id")
            .withColumnRenamed("to_id", "to_account_id")
        )
        accounts_edges = self._add_pk_and_ingest(
            accounts_edges,
            "vw_tx_network_accounts_edges",
            "from_account_id",
            "to_account_id",
        )
        self._write_view(
            accounts_edges,
            self.accounts_edges_path,
            "vw_tx_network_accounts_edges",
        )

        # 2. Counterparty-to-Counterparty edges
        counterparties_edges = (
            self._edge_bucket_agg(flags, "dbtr_id", "cdtr_id")
            .withColumnRenamed("from_id", "from_counterparty_id")
            .withColumnRenamed("to_id", "to_counterparty_id")
        )
        counterparties_edges = self._add_pk_and_ingest(
            counterparties_edges,
            "vw_tx_network_counterparties_edges",
            "from_counterparty_id",
            "to_counterparty_id",
        )
        self._write_view(
            counterparties_edges,
            self.counterparties_edges_path,
            "vw_tx_network_counterparties_edges",
        )

        # 3. Counterparty → Account holder links
        holder_debtor = (
            flags.filter(F.col("dbtr_id").isNotNull() & F.col("dbtr_account_id").isNotNull())
            .select(
                "tenant_id",
                "event_ts",
                "tx_amount",
                "tx_ccy",
                "is_alerted_tx",
                "is_investigated_tx",
                F.col("dbtr_id").alias("from_id"),
                F.col("dbtr_account_id").alias("to_id"),
            )
        )
        holder_creditor = (
            flags.filter(F.col("cdtr_id").isNotNull() & F.col("cdtr_account_id").isNotNull())
            .select(
                "tenant_id",
                "event_ts",
                "tx_amount",
                "tx_ccy",
                "is_alerted_tx",
                "is_investigated_tx",
                F.col("cdtr_id").alias("from_id"),
                F.col("cdtr_account_id").alias("to_id"),
            )
        )
        holder_edges = holder_debtor.unionByName(holder_creditor)

        holder_links = (
            self._edge_bucket_agg(holder_edges, "from_id", "to_id")
            .withColumnRenamed("from_id", "counterparty_id")
            .withColumnRenamed("to_id", "account_id")
        )
        holder_links = self._add_pk_and_ingest(
            holder_links,
            "vw_counterparty_account_links",
            "counterparty_id",
            "account_id",
        )
        self._write_view(
            holder_links,
            self.holder_links_path,
            "vw_counterparty_account_links",
        )

        print(f"[NetworkNavigatorViewETL] All 3 views created under: {self.views_root}")
        return self.views_root

    # ------------------------------------------------------------------
    # SILVER / GOLD  (no-op for view builders)
    # ------------------------------------------------------------------

    def silver(self) -> str:
        return self.views_root

    def gold(self) -> str:
        return self.views_root

    # ------------------------------------------------------------------
    # ORCHESTRATOR
    # ------------------------------------------------------------------

    def run(self, source_path: str = "") -> str:
        print("[NetworkNavigatorViewETL] Starting view build")
        self.bronze(source_path)
        print("[NetworkNavigatorViewETL] View build complete.")
        return self.views_root
