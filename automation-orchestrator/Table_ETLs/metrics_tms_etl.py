from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import List, Tuple

from .BaseETL import BaseETL


class MetricsTMSETL(BaseETL):
    """Pre-aggregate BIAR TMS metrics and persist to gold/metrics/tms as Hudi.

    Produces one combined metrics table partitioned by metric_year/metric_month/metric_date
    and keyed by metric_year,metric_month,metric_date,metric_hour,metric_quarter,metric_granularity.
    """

    def __init__(self, spark: SparkSession, warehouse_root: str) -> None:
        super().__init__(spark, warehouse_root)
        self.metrics_root = f"{self.warehouse_root}/gold/metrics/tms"

    @property
    def gold_path(self) -> str:
        return self.metrics_root

    def bronze(self, source_path: str) -> str:
        """No bronze stage — this ETL aggregates existing gold tables directly."""
        return source_path

    def silver(self) -> str:
        """No silver stage — this ETL aggregates existing gold tables directly."""
        return self.metrics_root

    def run(self, source_path: str) -> str:
        # source_path is a synthetic placeholder — this ETL has no source file,
        # it reads gold/transactions and gold/evaluation directly.
        print(
            f"[MetricsTMSETL] Aggregating from {self.warehouse_root}/gold/transactions "
            f"and {self.warehouse_root}/gold/evaluation"
        )
        result = self.gold()
        print("[MetricsTMSETL] ETL complete.")
        return result

    def _time_dims(self, ts_col: str) -> List[F.Column]:
        return [
            F.to_date(F.col(ts_col)).alias("metric_date"),
            F.hour(F.col(ts_col)).alias("metric_hour"),
            F.month(F.col(ts_col)).alias("metric_month"),
            F.quarter(F.col(ts_col)).alias("metric_quarter"),
            F.year(F.col(ts_col)).alias("metric_year"),
        ]

    def _aggregate_received(self, tx: DataFrame) -> DataFrame:
        # Filter pacs.008 and valid event_ts
        received = (
            tx.filter(F.col("tx_type") == "pacs.008.001.10")
            .filter(F.col("event_ts").isNotNull())
            .select("end_to_end_id", "event_ts")
        )

        agg = (
            received.withColumn("metric_date", F.to_date("event_ts"))
            .withColumn("metric_hour", F.hour("event_ts"))
            .withColumn("metric_month", F.month("event_ts"))
            .withColumn("metric_quarter", F.quarter("event_ts"))
            .withColumn("metric_year", F.year("event_ts"))
            .groupBy(
                "metric_year",
                "metric_month",
                "metric_date",
                "metric_hour",
                "metric_quarter",
            )
            .agg(F.countDistinct("end_to_end_id").alias("transactions_received"))
        )
        return agg

    def _aggregate_evaluated(
        self, eval_df: DataFrame
    ) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """Return (evaluated_counts_hourly, latency_hourly, latency_valid).

        - `evaluated_counts_hourly`: hourly counts of evaluated transactions (by tx_msg_id)
        - `latency_hourly`: hourly latency aggregates (avg/p95/count) — no evaluated-count
          column, so joining it against `evaluated_counts_hourly` in `_build_combined`
          doesn't produce a duplicate `transactions_evaluated` column.
        - `latency_valid`: raw per-evaluation rows with `e2e_eval_time_ms` (used for correct rollups)
        """
        # Hourly counts of evaluated transactions
        evaluated = (
            eval_df.filter(F.col("tx_msg_id").isNotNull())
            .filter(F.col("event_ts").isNotNull())
            .select("tx_msg_id", "event_ts")
            .withColumn("metric_date", F.to_date("event_ts"))
            .withColumn("metric_hour", F.hour("event_ts"))
            .withColumn("metric_month", F.month("event_ts"))
            .withColumn("metric_quarter", F.quarter("event_ts"))
            .withColumn("metric_year", F.year("event_ts"))
        )

        evaluated_counts_hourly = evaluated.groupBy(
            "metric_year",
            "metric_month",
            "metric_date",
            "metric_hour",
            "metric_quarter",
        ).agg(F.countDistinct("tx_msg_id").alias("transactions_evaluated"))

        # Raw per-eval latency rows (for correct percentile rollups)
        latency_valid = (
            eval_df.filter(
                F.col("event_ts").isNotNull() & F.col("dc_cre_dt_tm").isNotNull()
            )
            .withColumn(
                "e2e_eval_time_ms",
                (
                    F.col("event_ts").cast("double")
                    - F.col("dc_cre_dt_tm").cast("double")
                )
                * 1000.0,
            )
            .filter(F.col("e2e_eval_time_ms").isNotNull())
            .withColumn("metric_date", F.to_date("event_ts"))
            .withColumn("metric_hour", F.hour("event_ts"))
            .withColumn("metric_month", F.month("event_ts"))
            .withColumn("metric_quarter", F.quarter("event_ts"))
            .withColumn("metric_year", F.year("event_ts"))
            .select(
                "metric_year",
                "metric_month",
                "metric_date",
                "metric_hour",
                "metric_quarter",
                "e2e_eval_time_ms",
            )
        )

        # Hourly latency aggregates
        latency_hourly = latency_valid.groupBy(
            "metric_year",
            "metric_month",
            "metric_date",
            "metric_hour",
            "metric_quarter",
        ).agg(
            F.avg("e2e_eval_time_ms").alias("avg_evaluation_time_ms"),
            F.expr("percentile_approx(e2e_eval_time_ms, 0.95, 200)").alias(
                "p95_evaluation_time_ms"
            ),
            F.count("e2e_eval_time_ms").alias("evaluation_count"),
        )

        return evaluated_counts_hourly, latency_hourly, latency_valid

    @staticmethod
    def _with_rate(df: DataFrame) -> DataFrame:
        """Add received_vs_evaluated_rate_pct computed from this frame's own
        transactions_received/transactions_evaluated columns.

        Must be applied per-granularity on that granularity's own summed
        counts, not averaged from another granularity's rates: bucket A
        (100/50=200%) + bucket B (10/100=10%) must roll up to 110/150=73.3%,
        not avg(200, 10)=105%.
        """
        return df.withColumn(
            "received_vs_evaluated_rate_pct",
            F.when(
                F.col("transactions_evaluated") > 0,
                F.round(
                    (F.col("transactions_received") / F.col("transactions_evaluated"))
                    * 100,
                    2,
                ),
            ).otherwise(F.lit(None)),
        )

    def _build_combined(
        self,
        received_hourly: DataFrame,
        counts_hourly: DataFrame,
        latency_hourly: DataFrame,
        latency_valid: DataFrame,
    ) -> DataFrame:
        """Combine received counts, evaluated counts and latency aggregates into canonical metrics rows and produce rollups.

        Rollups (Daily/Monthly/Quarterly/Annually) are computed from raw `latency_valid` for percentiles and averages.
        """
        # Base hourly frame: join received + evaluated counts + latency aggregates
        hourly = (
            received_hourly.join(
                counts_hourly,
                on=[
                    "metric_year",
                    "metric_month",
                    "metric_date",
                    "metric_hour",
                    "metric_quarter",
                ],
                how="full",
            )
            .join(
                latency_hourly,
                on=[
                    "metric_year",
                    "metric_month",
                    "metric_date",
                    "metric_hour",
                    "metric_quarter",
                ],
                how="left",
            )
            .fillna({
                "transactions_received": 0,
                "transactions_evaluated": 0,
                "evaluation_count": 0,
            })
        )

        hourly = hourly.withColumn("metric_granularity", F.lit("Hourly"))
        # No DQ-exclusion source exists yet — placeholder so downstream rollups can sum it.
        hourly = hourly.withColumn("dq_excluded_count", F.lit(0).cast("long"))

        # Compute received_vs_evaluated_rate_pct
        hourly = self._with_rate(hourly)

        # Daily rollups: counts by summing hourly counts, latency from raw rows
        daily_counts = (
            hourly.groupBy(
                "metric_year", "metric_month", "metric_date", "metric_quarter"
            )
            .agg(
                F.sum("transactions_received").alias("transactions_received"),
                F.sum("transactions_evaluated").alias("transactions_evaluated"),
                F.sum("evaluation_count").alias("evaluation_count"),
                F.sum("dq_excluded_count").alias("dq_excluded_count"),
            )
            .withColumn("metric_hour", F.lit(-1))
            .withColumn("metric_granularity", F.lit("Daily"))
        )
        daily_counts = self._with_rate(daily_counts)

        daily_latency = (
            latency_valid.groupBy("metric_year", "metric_month", "metric_date")
            .agg(
                F.avg("e2e_eval_time_ms").alias("avg_evaluation_time_ms"),
                F.expr("percentile_approx(e2e_eval_time_ms, 0.95, 200)").alias(
                    "p95_evaluation_time_ms"
                ),
            )
        )

        daily = daily_counts.join(
            daily_latency, on=["metric_year", "metric_month", "metric_date"], how="left"
        ).withColumn("metric_quarter", F.quarter(F.col("metric_date")))

        # Monthly rollups
        monthly_counts = (
            hourly.groupBy("metric_year", "metric_month", "metric_quarter")
            .agg(
                F.sum("transactions_received").alias("transactions_received"),
                F.sum("transactions_evaluated").alias("transactions_evaluated"),
                F.sum("evaluation_count").alias("evaluation_count"),
                F.sum("dq_excluded_count").alias("dq_excluded_count"),
            )
            .withColumn("metric_date", F.lit(None).cast("date"))
            .withColumn("metric_hour", F.lit(-1))
            .withColumn("metric_granularity", F.lit("Monthly"))
        )
        monthly_counts = self._with_rate(monthly_counts)

        monthly_latency = (
            latency_valid.groupBy("metric_year", "metric_month")
            .agg(
                F.avg("e2e_eval_time_ms").alias("avg_evaluation_time_ms"),
                F.expr("percentile_approx(e2e_eval_time_ms, 0.95, 200)").alias(
                    "p95_evaluation_time_ms"
                ),
            )
        )

        monthly = monthly_counts.join(
            monthly_latency, on=["metric_year", "metric_month"], how="left"
        )

        # Quarterly rollups
        quarterly_counts = (
            hourly.groupBy("metric_year", "metric_quarter")
            .agg(
                F.sum("transactions_received").alias("transactions_received"),
                F.sum("transactions_evaluated").alias("transactions_evaluated"),
                F.sum("evaluation_count").alias("evaluation_count"),
                F.sum("dq_excluded_count").alias("dq_excluded_count"),
            )
            .withColumn("metric_month", F.lit(None).cast("int"))
            .withColumn("metric_date", F.lit(None).cast("date"))
            .withColumn("metric_hour", F.lit(-1))
            .withColumn("metric_granularity", F.lit("Quarterly"))
        )
        quarterly_counts = self._with_rate(quarterly_counts)

        quarterly_latency = (
            latency_valid.groupBy("metric_year", "metric_quarter")
            .agg(
                F.avg("e2e_eval_time_ms").alias("avg_evaluation_time_ms"),
                F.expr("percentile_approx(e2e_eval_time_ms, 0.95, 200)").alias(
                    "p95_evaluation_time_ms"
                ),
            )
        )

        quarterly = quarterly_counts.join(
            quarterly_latency, on=["metric_year", "metric_quarter"], how="left"
        )

        # Annual rollups
        annual_counts = (
            hourly.groupBy("metric_year")
            .agg(
                F.sum("transactions_received").alias("transactions_received"),
                F.sum("transactions_evaluated").alias("transactions_evaluated"),
                F.sum("evaluation_count").alias("evaluation_count"),
                F.sum("dq_excluded_count").alias("dq_excluded_count"),
            )
            .withColumn("metric_month", F.lit(None).cast("int"))
            .withColumn("metric_date", F.lit(None).cast("date"))
            .withColumn("metric_hour", F.lit(-1))
            .withColumn("metric_quarter", F.lit(None).cast("int"))
            .withColumn("metric_granularity", F.lit("Annually"))
        )
        annual_counts = self._with_rate(annual_counts)

        annual_latency = (
            latency_valid.groupBy("metric_year")
            .agg(
                F.avg("e2e_eval_time_ms").alias("avg_evaluation_time_ms"),
                F.expr("percentile_approx(e2e_eval_time_ms, 0.95, 200)").alias(
                    "p95_evaluation_time_ms"
                ),
            )
        )

        annual = annual_counts.join(annual_latency, on=["metric_year"], how="left")

        # Union all granularities
        all_frames = [hourly, daily, monthly, quarterly, annual]
        out = None
        for f in all_frames:
            if out is None:
                out = f
            else:
                out = out.unionByName(f, allowMissingColumns=True)

        # Ensure canonical column order and presence
        expected_cols = [
            "metric_year",
            "metric_month",
            "metric_date",
            "metric_quarter",
            "metric_hour",
            "metric_granularity",
            "transactions_received",
            "transactions_evaluated",
            "received_vs_evaluated_rate_pct",
            "avg_evaluation_time_ms",
            "p95_evaluation_time_ms",
            "evaluation_count",
            "dq_excluded_count",
        ]

        col_types = {
            "metric_year": "int",
            "metric_month": "int",
            "metric_date": "date",
            "metric_quarter": "int",
            "metric_hour": "int",
            "metric_granularity": "string",
            "transactions_received": "long",
            "transactions_evaluated": "long",
            "received_vs_evaluated_rate_pct": "double",
            "avg_evaluation_time_ms": "double",
            "p95_evaluation_time_ms": "double",
            "evaluation_count": "long",
            "dq_excluded_count": "long",
        }

        out = self.ensure_columns(out, col_types)
        return out.select(*expected_cols)

    def _normalize_transactions(self, tx: DataFrame) -> DataFrame:
        """gold/transactions can be built in either the PACS-derived schema
        (tx_type, end_to_end_id) or the raw Ozone transaction-feed schema
        (txtp, endtoendid) — support both, same as the dashboard notebook's
        normalize_transactions_for_dashboard()."""

        def pick(*names, cast_type="string"):
            for name in names:
                if name in tx.columns:
                    return F.col(name).cast(cast_type)
            return F.lit(None).cast(cast_type)

        return tx.select(
            pick("tx_type", "txtp").alias("tx_type"),
            pick("end_to_end_id", "endtoendid").alias("end_to_end_id"),
            pick("event_ts", cast_type="timestamp").alias("event_ts"),
        )

    def gold(self) -> str:
        # Read source gold tables
        tx_path = f"{self.warehouse_root}/gold/transactions"
        eval_path = f"{self.warehouse_root}/gold/evaluation"

        tx = self._normalize_transactions(self.read_latest_hudi(tx_path))
        evaluation = self.read_latest_hudi(eval_path)

        # Hourly aggregates
        received_hourly = self._aggregate_received(tx)
        counts_hourly, latency_hourly, latency_valid = self._aggregate_evaluated(
            evaluation
        )

        # Build combined table with correct rollups (hourly + daily/monthly/quarterly/annually)
        combined = self._build_combined(
            received_hourly, counts_hourly, latency_hourly, latency_valid
        )

        # Add write timestamp for precombine ordering
        rolled = combined.withColumn("written_at_ts", F.current_timestamp())

        # Hudi writer options
        # Use a composite record key that includes quarter and granularity to avoid collisions
        record_key = "metric_year,metric_month,metric_date,metric_hour,metric_quarter,metric_granularity"
        partition = "metric_year,metric_month,metric_date"
        opts = self.hudi_opts(
            table_name="metrics_tms",
            record_key=record_key,
            precombine="written_at_ts",
            partition=partition,
        )
        # hudi_opts() defaults to SimpleKeyGenerator when a partition is set, but
        # SimpleKeyGenerator only supports a single record-key field. record_key here
        # is a 6-field composite, so it needs ComplexKeyGenerator instead.
        opts["hoodie.datasource.write.keygenerator.class"] = (
            "org.apache.hudi.keygen.ComplexKeyGenerator"
        )

        # Ensure the written_at_ts column exists as timestamp for Hudi precombine
        rolled = self.ensure_columns(rolled, {"written_at_ts": "timestamp"})

        # Use the BaseETL writer
        self.write_hudi(rolled, self.metrics_root, opts)
        return self.metrics_root
