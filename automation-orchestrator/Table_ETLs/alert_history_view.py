"""
alert_history_view.py
---------------------
Builds vw_alert_history from gold alerts, cases, tasks, and the transaction detail view.
Produces 5 granularities (day, week, month, year, all) at entity level (ACCOUNT + COUNTERPARTY).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .BaseETL import BaseETL


class AlertHistoryViewETL(BaseETL):
    """
    Alert History View builder.

    Reads gold alerts, cases, tasks, and vw_transaction_detail,
    expands to entity level, and aggregates into time-bucketed rows.
    """

    GRANULARITIES = ("day", "week", "month", "year", "all")

    def __init__(self, spark, warehouse_root: str) -> None:
        super().__init__(spark, warehouse_root)
        self.views_root = f"{self.warehouse_root}/views"
        self.view_path = f"{self.views_root}/alert_history"

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

    def _add_bucket(self, df: DataFrame, granularity: str) -> DataFrame:
        """Add bucket_start / bucket_end / bucket_granularity columns."""
        if granularity == "day":
            bs = F.date_trunc("day", F.col("event_ts"))
            be = bs + F.expr("INTERVAL 1 DAY")
        elif granularity == "week":
            bs = F.date_trunc("week", F.col("event_ts"))
            be = bs + F.expr("INTERVAL 7 DAYS")
        elif granularity == "month":
            bs = F.date_trunc("month", F.col("event_ts"))
            be = F.add_months(bs.cast("date"), 1).cast("timestamp")
        elif granularity == "year":
            bs = F.date_trunc("year", F.col("event_ts"))
            be = F.add_months(bs.cast("date"), 12).cast("timestamp")
        elif granularity == "all":
            bs = F.lit("1970-01-01 00:00:00").cast("timestamp")
            be = F.lit("2999-12-31 00:00:00").cast("timestamp")
        else:
            raise ValueError(
                f"granularity must be one of {self.GRANULARITIES} (got: {granularity!r})"
            )

        return (
            df.withColumn("bucket_granularity", F.lit(granularity))
            .withColumn("bucket_start", bs)
            .withColumn("bucket_end", be)
        )

    def _load_alerts_enriched(self) -> DataFrame:
        """Load and join alerts with transaction detail, cases, and tasks."""
        alerts = self.spark.read.format("hudi").load(f"{self.warehouse_root}/gold/alerts")
        cases = self.spark.read.format("hudi").load(f"{self.warehouse_root}/gold/cases")
        tasks = self.spark.read.format("hudi").load(f"{self.warehouse_root}/gold/tasks")
        txd = self.spark.read.format("hudi").load(f"{self.views_root}/vw_transaction_detail")

        case_status = cases.select(
            F.col("case_id").cast("long").alias("case_id"),
            F.col("status").cast("string").alias("case_status"),
        ).dropDuplicates(["case_id"])

        task_completed = (
            tasks.groupBy(F.col("case_id").cast("long").alias("case_id"))
            .agg(F.max(F.col("is_completed").cast("int")).alias("has_completed_task"))
        )

        a = alerts.select(
            F.col("alert_id").cast("long").alias("alert_id"),
            F.col("case_id").cast("long").alias("case_id"),
            F.col("tenant_id").cast("string").alias("alert_tenant_id"),
            F.col("tx_msg_id").cast("string").alias("tx_msg_id"),
            F.col("event_ts").cast("timestamp").alias("alert_event_ts"),
            F.col("tx_amount").cast("double").alias("alert_tx_amount"),
        ).dropDuplicates(["alert_id"])

        t = txd.select(
            F.col("tx_msg_id").cast("string").alias("tx_msg_id"),
            F.col("tenant_id").cast("string").alias("tx_tenant_id"),
            F.col("tx_event_ts").cast("timestamp").alias("tx_event_ts"),
            F.col("debtor_id").cast("string").alias("dbtr_id"),
            F.col("creditor_id").cast("string").alias("cdtr_id"),
            F.col("debtor_account_id").cast("string").alias("dbtr_account_id"),
            F.col("creditor_account_id").cast("string").alias("cdtr_account_id"),
            F.col("instructed_amount").cast("double").alias("instd_amount"),
            F.col("instructed_currency").cast("string").alias("instd_ccy"),
        ).dropDuplicates(["tx_msg_id"])

        enriched = (
            a.join(t, on="tx_msg_id", how="left")
            .withColumn(
                "tenant_id",
                F.coalesce(F.col("alert_tenant_id"), F.col("tx_tenant_id")),
            )
            .withColumn(
                "event_ts",
                F.coalesce(F.col("tx_event_ts"), F.col("alert_event_ts")),
            )
            .withColumn(
                "tx_amount",
                F.coalesce(F.col("alert_tx_amount"), F.col("instd_amount")),
            )
            .drop(
                "alert_tenant_id",
                "tx_tenant_id",
                "alert_event_ts",
                "tx_event_ts",
                "alert_tx_amount",
            )
        )

        return (
            enriched.join(case_status, on="case_id", how="left")
            .join(task_completed, on="case_id", how="left")
            .withColumn("alert_flag", F.lit(1).cast("int"))
            .withColumn(
                "investigation_flag",
                F.when(
                    (F.col("case_status").isNotNull())
                    | (F.coalesce(F.col("has_completed_task"), F.lit(0)) == 1),
                    F.lit(1),
                )
                .otherwise(F.lit(0))
                .cast("int"),
            )
        )

    def _build_entity_events(self, enriched: DataFrame) -> DataFrame:
        """Expand alerts to entity level (ACCOUNT + COUNTERPARTY)."""
        base_cols = [
            "tenant_id",
            "event_ts",
            "tx_amount",
            "alert_flag",
            "investigation_flag",
            "alert_id",
            "case_id",
            "tx_msg_id",
        ]

        acct_stream = (
            enriched.select(*base_cols, F.col("dbtr_account_id").alias("entity_id"))
            .where(F.col("entity_id").isNotNull())
            .withColumn("entity_type", F.lit("ACCOUNT"))
        ).unionByName(
            enriched.select(*base_cols, F.col("cdtr_account_id").alias("entity_id"))
            .where(F.col("entity_id").isNotNull())
            .withColumn("entity_type", F.lit("ACCOUNT")),
            allowMissingColumns=True,
        )

        cp_stream = (
            enriched.select(*base_cols, F.col("dbtr_id").alias("entity_id"))
            .where(F.col("entity_id").isNotNull())
            .withColumn("entity_type", F.lit("COUNTERPARTY"))
        ).unionByName(
            enriched.select(*base_cols, F.col("cdtr_id").alias("entity_id"))
            .where(F.col("entity_id").isNotNull())
            .withColumn("entity_type", F.lit("COUNTERPARTY")),
            allowMissingColumns=True,
        )

        return acct_stream.unionByName(cp_stream, allowMissingColumns=True)

    def _bucket_agg(self, df: DataFrame, granularity: str) -> DataFrame:
        """Aggregate entity events into a single time bucket."""
        d = self._add_bucket(df, granularity)
        return (
            d.groupBy(
                "tenant_id",
                "entity_type",
                "entity_id",
                "bucket_granularity",
                "bucket_start",
                "bucket_end",
            )
            .agg(
                F.countDistinct("alert_id").alias("alerts_count"),
                F.sum(F.coalesce("tx_amount", F.lit(0.0))).alias("alerts_value_sum"),
                F.sum("investigation_flag").cast("long").alias("investigations_count"),
                F.sum(
                    F.when(
                        F.col("investigation_flag") == 1,
                        F.coalesce(F.col("tx_amount"), F.lit(0.0)),
                    ).otherwise(F.lit(0.0))
                ).alias("investigations_value_sum"),
                F.lit(0).cast("long").alias("sar_str_count"),
                F.lit(0.0).cast("double").alias("sar_str_value_sum"),
                F.min("event_ts").alias("first_event_ts"),
                F.max("event_ts").alias("last_event_ts"),
            )
        )

    def _add_pk(self, df: DataFrame) -> DataFrame:
        """Add deterministic PK and ingestion timestamp."""
        return df.withColumn("ingested_at_ts", F.current_timestamp()).withColumn(
            "pk",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.lit("vw_alert_history"),
                    F.coalesce(F.col("tenant_id"), F.lit("")),
                    F.coalesce(F.col("entity_type"), F.lit("")),
                    F.coalesce(F.col("entity_id"), F.lit("")),
                    F.coalesce(F.col("bucket_granularity"), F.lit("")),
                    F.coalesce(F.col("bucket_start").cast("string"), F.lit("")),
                ),
                256,
            ),
        )

    # ------------------------------------------------------------------
    # BRONZE  (main view build)
    # ------------------------------------------------------------------

    def bronze(self, source_path: str = "") -> str:
        """Build vw_alert_history."""
        print("[AlertHistoryViewETL] Creating Alert History View...")

        # 1. Load and enrich alerts
        enriched = self._load_alerts_enriched()

        # 2. Expand to entity level
        entity_events = self._build_entity_events(enriched)

        # 3. Bucket aggregation across all granularities
        result = self._bucket_agg(entity_events, self.GRANULARITIES[0])
        for gran in self.GRANULARITIES[1:]:
            result = result.unionByName(self._bucket_agg(entity_events, gran))

        # 4. PK + ingest timestamp
        view_df = self._add_pk(result)

        # 5. Write Hudi view (partitioned by bucket_granularity)
        self.write_hudi(
            view_df,
            self.view_path,
            self.hudi_opts(
                "vw_alert_history",
                record_key="pk",
                precombine="ingested_at_ts",
                partition="bucket_granularity",
            ),
        )
        print(f"[AlertHistoryViewETL] View written → {self.view_path}")
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
        print("[AlertHistoryViewETL] Starting view build")
        self.bronze(source_path)
        print("[AlertHistoryViewETL] View build complete.")
        return self.view_path