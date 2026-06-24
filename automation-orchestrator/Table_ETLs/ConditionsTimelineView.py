"""
conditions_timeline_view.py
---------------------------
Builds vw_conditions_timeline from gold conditions and transactions,
with alerts, cases, and tasks enrichment.
Produces 5 granularities (day, week, month, year, all).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .BaseETL import BaseETL


class ConditionsTimelineViewETL(BaseETL):
    """
    Conditions Timeline View builder.

    Reads gold conditions (bucketized by inception), gold transactions
    (bucketized by event_ts, enriched with alerts/cases/tasks),
    joins them, and writes the timeline view.
    """

    GRANULARITIES = ("day", "week", "month", "year", "all")

    def __init__(self, spark, warehouse_root: str) -> None:
        super().__init__(spark, warehouse_root)
        self.views_root = f"{self.warehouse_root}/views"
        self.view_path = f"{self.views_root}/conditions_timeline"

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

    def _bucketize(self, df: DataFrame, ts_col: str) -> DataFrame:
        """Create 5 granularity buckets (day, week, month, year, all)."""
        base = df.withColumn("bucket_src_ts", F.col(ts_col).cast("timestamp"))
        out = None
        for g, trunc_unit in [
            ("day", "day"),
            ("week", "week"),
            ("month", "month"),
            ("year", "year"),
            ("all", None),
        ]:
            part = base.withColumn("bucket_granularity", F.lit(g))
            if trunc_unit:
                part = part.withColumn("bucket_start", F.date_trunc(trunc_unit, F.col("bucket_src_ts")))
            else:
                part = part.withColumn("bucket_start", F.lit("1900-01-01").cast("timestamp"))
            out = part if out is None else out.unionByName(part, allowMissingColumns=True)
        return out.drop("bucket_src_ts")

    def _load_conditions(self) -> DataFrame:
        """Load and select gold conditions fields."""
        c0 = self.spark.read.format("hudi").load(f"{self.warehouse_root}/gold/conditions").alias("c")

        return c0.select(
            F.col("c.pk").alias("condition_pk"),
            F.col("c.condition_id"),
            F.col("c.tenant_id").alias("condition_tenant_id"),
            F.col("c.created_by_user"),
            F.col("c.account_id"),
            F.col("c.account_scheme"),
            F.col("c.account_agent_mmb_id"),
            F.col("c.event_types_csv"),
            F.col("c.event_type_primary"),
            F.col("c.event_type_count"),
            F.col("c.condition_type"),
            F.col("c.perspective"),
            F.col("c.condition_reason"),
            F.col("c.force_create"),
            F.col("c.condition_created_ts"),
            F.col("c.condition_inception_ts"),
            F.col("c.condition_expiry_ts"),
            F.col("c.condition_created_date"),
            F.col("c.is_active"),
            F.col("c.is_expired"),
            F.col("c.ingested_at_ts").alias("condition_ingested_at_ts"),
        )

    def _load_transactions(self) -> DataFrame:
        """Load gold transactions with alerts, cases, tasks enrichment."""
        t0 = self.spark.read.format("hudi").load(f"{self.warehouse_root}/gold/transactions").alias("t")

        t = t0.select(
            F.col("t.transaction_id"),
            F.col("t.end_to_end_id"),
            F.col("t.tenant_id").alias("tx_tenant_id"),
            F.col("t.tx_type"),
            F.col("t.tx_msg_id"),
            F.col("t.tx_status"),
            F.col("t.tx_amount"),
            F.col("t.tx_ccy"),
            F.col("t.event_ts").alias("tx_event_ts"),
            F.col("t.event_date").alias("tx_event_date"),
        )

        # alerts join
        a0 = self.spark.read.format("hudi").load(f"{self.warehouse_root}/gold/alerts").alias("a")
        a = a0.select(
            F.col("a.alert_id"),
            F.col("a.case_id"),
            F.col("a.tx_msg_id").alias("a_tx_msg_id"),
        ).dropDuplicates(["a_tx_msg_id"])

        t = (
            t.join(a, t.tx_msg_id == a.a_tx_msg_id, "left")
            .withColumn("is_alerted_tx", F.when(F.col("alert_id").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
        )

        # cases join
        try:
            cases0 = self.spark.read.format("hudi").load(f"{self.warehouse_root}/gold/cases").alias("cs")
            cases = cases0.select(F.col("cs.case_id").alias("cs_case_id")).dropDuplicates(["cs_case_id"])
            t = t.join(cases, t.case_id == cases.cs_case_id, "left")
        except Exception:
            t = t.withColumn("cs_case_id", F.lit(None).cast("long"))

        # tasks join
        try:
            tasks0 = self.spark.read.format("hudi").load(f"{self.warehouse_root}/gold/tasks").alias("tk")
            tasks = tasks0.groupBy("case_id").agg(F.max("is_completed").alias("has_completed_task"))
            t = t.join(tasks, "case_id", "left")
        except Exception:
            t = t.withColumn("has_completed_task", F.lit(0).cast("int"))

        # tx flags
        t = t.withColumn(
            "is_investigated_tx",
            F.when(
                F.col("cs_case_id").isNotNull() | (F.coalesce(F.col("has_completed_task"), F.lit(0)) == 1),
                F.lit(1)
            ).otherwise(F.lit(0))
        )

        t = t.withColumn(
            "tx_block_override_status",
            F.when(F.upper(F.col("tx_status")).isin("BLOCKED", "REJECTED"), F.lit("BLOCKED"))
            .otherwise(F.lit("NONE"))
        )

        return t

    def _join_conditions_transactions(self, c_b: DataFrame, t_b: DataFrame) -> DataFrame:
        """Join bucketized conditions with bucketized transactions."""
        cond = c_b.alias("cond")
        tx = t_b.alias("tx")

        return cond.join(
            tx,
            (F.col("cond.condition_tenant_id") == F.col("tx.tx_tenant_id")) &
            (F.col("cond.bucket_granularity") == F.col("tx.bucket_granularity")) &
            (F.col("cond.bucket_start") == F.col("tx.bucket_start")) &
            (F.col("tx.tx_event_ts") >= F.col("cond.condition_inception_ts")) &
            (
                F.col("cond.condition_expiry_ts").isNull() |
                (F.col("tx.tx_event_ts") <= F.col("cond.condition_expiry_ts"))
            ) &
            (
                F.col("cond.event_types_csv").isNull() |
                F.col("tx.tx_type").isNull() |
                F.expr("instr(cond.event_types_csv, tx.tx_type) > 0")
            ),
            "left"
        )

    def _add_pk(self, df: DataFrame) -> DataFrame:
        """Add deterministic PK and ingestion timestamp."""
        return (
            df.withColumn("ingested_at_ts", F.current_timestamp())
            .withColumn(
                "pk",
                F.sha2(
                    F.concat_ws("||",
                        F.lit("vw_conditions_timeline"),
                        F.coalesce(F.col("cond.condition_id"), F.lit("")),
                        F.coalesce(F.col("cond.condition_tenant_id"), F.lit("")),
                        F.coalesce(F.col("cond.bucket_granularity"), F.lit("")),
                        F.coalesce(F.col("cond.bucket_start").cast("string"), F.lit("")),
                        F.coalesce(F.col("tx.transaction_id").cast("string"), F.lit("NO_TX"))
                    ),
                    256
                )
            )
        )

    # ------------------------------------------------------------------
    # BRONZE  (main view build)
    # ------------------------------------------------------------------

    def bronze(self, source_path: str = "") -> str:
        """Build vw_conditions_timeline."""
        print("[ConditionsTimelineViewETL] Creating Conditions Timeline View...")

        # 1. Load conditions, bucketize by inception
        c = self._load_conditions()
        c_b = self._bucketize(c, "condition_inception_ts")

        # 2. Load transactions, enrich, bucketize by event_ts
        t = self._load_transactions()
        t_b = self._bucketize(t, "tx_event_ts")

        # 3. Join conditions + transactions
        joined = self._join_conditions_transactions(c_b, t_b)

        # 4. PK + ingest timestamp
        view_df = self._add_pk(joined)

        # 5. Final select
        vw = view_df.select(
            "pk",
            F.col("cond.condition_id").alias("cond_condition_id"),
            F.col("cond.condition_pk").alias("cond_pk"),
            F.col("cond.condition_tenant_id").alias("cond_tenant_id"),
            F.col("cond.bucket_granularity"),
            F.col("cond.bucket_start"),
            F.col("cond.account_id").alias("cond_account_id"),
            F.col("cond.account_scheme").alias("cond_account_scheme"),
            F.col("cond.account_agent_mmb_id").alias("cond_account_agent_mmb_id"),
            F.col("cond.condition_type").alias("cond_type"),
            F.col("cond.perspective").alias("cond_perspective"),
            F.col("cond.condition_reason").alias("cond_reason"),
            F.col("cond.force_create").alias("cond_force_create"),
            F.col("cond.event_types_csv").alias("cond_event_types_csv"),
            F.col("cond.event_type_primary").alias("cond_event_type_primary"),
            F.col("cond.event_type_count").alias("cond_event_type_count"),
            F.col("cond.condition_created_ts").alias("cond_created_ts"),
            F.col("cond.condition_inception_ts").alias("cond_inception_ts"),
            F.col("cond.condition_expiry_ts").alias("cond_expiry_ts"),
            F.col("cond.is_active").alias("cond_is_active"),
            F.col("cond.is_expired").alias("cond_is_expired"),
            F.col("tx.transaction_id").alias("tx_transaction_id"),
            F.col("tx.end_to_end_id").alias("tx_end_to_end_id"),
            F.col("tx.tx_msg_id").alias("tx_msg_id"),
            F.col("tx.tx_type").alias("tx_type"),
            F.col("tx.tx_status").alias("tx_status"),
            F.col("tx.tx_amount").alias("tx_amount"),
            F.col("tx.tx_ccy").alias("tx_ccy"),
            F.col("tx.tx_event_ts").alias("tx_event_ts"),
            F.col("tx.is_alerted_tx").alias("tx_is_alerted"),
            F.col("tx.is_investigated_tx").alias("tx_is_investigated"),
            F.col("tx.tx_block_override_status").alias("tx_block_override_status"),
            F.col("cond.condition_ingested_at_ts").alias("cond_ingested_at_ts"),
            F.col("ingested_at_ts"),
        )

        # 6. Write Hudi view
        self.write_hudi(
            vw,
            self.view_path,
            self.hudi_opts(
                "vw_conditions_timeline",
                record_key="pk",
                precombine="ingested_at_ts",
            ),
        )
        print(f"[ConditionsTimelineViewETL] View written → {self.view_path}")
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
        print("[ConditionsTimelineViewETL] Starting view build")
        self.bronze(source_path)
        print("[ConditionsTimelineViewETL] View build complete.")
        return self.view_path