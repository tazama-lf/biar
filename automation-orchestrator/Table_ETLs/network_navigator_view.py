from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .BaseETL import BaseETL


class NetworkNavigatorViewETL(BaseETL):
    """
    Network Navigator view builder.

    Reads bronze/transactions, joins alerts/cases/tasks for flags,
    and produces time-bucketed edge tables for graph visualization.
    """

    BUCKET_GRANULARITIES = ("day", "week", "month", "year")

    def __init__(self, spark, warehouse_root: str) -> None:
        super().__init__(spark, warehouse_root)
        self.views_root = f"{self.warehouse_root}/views"
        self.accounts_edges_path = f"{self.views_root}/vw_tx_network_accounts_edges"
        self.counterparties_edges_path = f"{self.views_root}/vw_tx_network_counterparties_edges"
        self.holder_links_path = f"{self.views_root}/vw_counterparty_account_links"
        self.transactions_bronze_path = f"{self.warehouse_root}/bronze/transactions"

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
        """Load bronze transactions + gold alerts/cases/tasks, return flagged base frame."""
        tx = self.spark.read.format("hudi").load(self.transactions_bronze_path)

        # Normalize names
        rename_map = {
            "endToEndId": "end_to_end_id",
            "tenantId": "tenant_id",
            "transaction_pk": "transaction_pk",
        }
        for src, dst in rename_map.items():
            if src in tx.columns and dst not in tx.columns:
                tx = tx.withColumnRenamed(src, dst)

        # Resolve JSON column
        candidates = [
            "transactionData",
            "transaction_data",
            "transaction",
            "payload",
            "raw_payload",
        ]
        json_col = next((c for c in candidates if c in tx.columns), None)
        if json_col is None:
            raise ValueError(
                f"No raw JSON column found in bronze/transactions. "
                f"Tried: {candidates}\nAvailable: {tx.columns}"
            )
        tx = tx.withColumn("transaction_data", F.col(json_col).cast("string"))

        # Extract PACS fields
        tx_type = F.get_json_object("transaction_data", "$.TxTp")
        tx_msg_id = F.coalesce(
            F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.GrpHdr.MsgId"),
            F.get_json_object("transaction_data", "$.FIToFIPmtSts.GrpHdr.MsgId"),
        )
        event_ts = F.to_timestamp(
            F.coalesce(
                F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.GrpHdr.CreDtTm"),
                F.get_json_object("transaction_data", "$.FIToFIPmtSts.GrpHdr.CreDtTm"),
            )
        )
        event_date = F.to_date(event_ts)

        dbtr_id = F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.Dbtr.Id.PrvtId.Othr[0].Id")
        cdtr_id = F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.Cdtr.Id.PrvtId.Othr[0].Id")
        dbtr_acct = F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.DbtrAcct.Id.Othr[0].Id")
        cdtr_acct = F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAcct.Id.Othr[0].Id")
        tx_amount = F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.InstdAmt.Amt.Amt").cast("double")
        tx_ccy = F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.InstdAmt.Amt.Ccy")

        base = (
            tx.withColumn("tx_type", tx_type)
            .withColumn("tx_msg_id", tx_msg_id)
            .withColumn("event_ts", event_ts)
            .withColumn("event_date", event_date)
            .withColumn("tx_amount", tx_amount)
            .withColumn("tx_ccy", tx_ccy)
            .withColumn("dbtr_id", dbtr_id)
            .withColumn("cdtr_id", cdtr_id)
            .withColumn("dbtr_account_id", dbtr_acct)
            .withColumn("cdtr_account_id", cdtr_acct)
            .select(
                F.col("transaction_pk").cast("long").alias("transaction_pk"),
                F.col("end_to_end_id").cast("string").alias("end_to_end_id"),
                F.col("tenant_id").cast("string").alias("tenant_id"),
                F.col("tx_type").cast("string").alias("tx_type"),
                F.col("tx_msg_id").cast("string").alias("tx_msg_id"),
                F.col("event_ts").cast("timestamp").alias("event_ts"),
                F.col("event_date").cast("date").alias("event_date"),
                F.col("tx_amount").cast("double").alias("tx_amount"),
                F.col("tx_ccy").cast("string").alias("tx_ccy"),
                F.col("dbtr_id").cast("string").alias("dbtr_id"),
                F.col("cdtr_id").cast("string").alias("cdtr_id"),
                F.col("dbtr_account_id").cast("string").alias("dbtr_account_id"),
                F.col("cdtr_account_id").cast("string").alias("cdtr_account_id"),
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