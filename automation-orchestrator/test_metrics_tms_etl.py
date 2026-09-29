"""Regression tests for MetricsTMSETL's DQ-exclusion handling.

Covers the case where a valid-latency row and a negative-latency
(DQ-excluded) row land in the *same* hourly bucket, which previously
caused `evaluation_count` to silently drop the excluded row, and the case
where a valid-latency row with no `tx_msg_id` had its avg/p95 latency
silently dropped by an intermediate left join (see PR #182 review
discussion).

Also covers tenant scoping (#185 / PR #186): every aggregate and rollup row
is keyed by `tenant_id`, so two tenants sharing a time bucket must produce
two separate rows rather than one blended row.
"""

from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from Table_ETLs.metrics_tms_etl import MetricsTMSETL

# Explicit schemas (rather than inference) so that all-null columns — e.g. rows
# with a null tx_msg_id — still produce a typed frame on every Spark version.
_EVAL_SCHEMA = StructType(
    [
        StructField("tx_msg_id", StringType()),
        StructField("event_ts", TimestampType()),
        StructField("dc_cre_dt_tm", TimestampType()),
        StructField("tenant_id", StringType()),
    ]
)

_RECEIVED_SCHEMA = StructType(
    [
        StructField("end_to_end_id", StringType()),
        StructField("event_ts", TimestampType()),
        StructField("tx_type", StringType()),
        StructField("tenant_id", StringType()),
    ]
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("metrics-tms-etl-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture()
def etl(spark):
    return MetricsTMSETL(spark, warehouse_root="/tmp/unused")


def _empty_received_hourly(spark):
    schema = StructType(
        [
            StructField("metric_year", IntegerType()),
            StructField("metric_month", IntegerType()),
            StructField("metric_date", StringType()),
            StructField("metric_hour", IntegerType()),
            StructField("metric_quarter", IntegerType()),
            StructField("tenant_id", StringType()),
            StructField("transactions_received", LongType()),
        ]
    )
    return spark.createDataFrame([], schema)


def _eval_df(spark, rows):
    """Build the gold/evaluation frame `_aggregate_evaluated()` expects."""
    return spark.createDataFrame(rows, _EVAL_SCHEMA)


def _received_df(spark, rows):
    """Build the gold/transactions frame `_aggregate_received()` expects."""
    return spark.createDataFrame(rows, _RECEIVED_SCHEMA)


def test_evaluation_count_includes_dq_excluded_rows_in_shared_bucket(etl, spark):
    # One valid-latency row and one negative-latency (DQ-excluded) row, both
    # landing in the same hour bucket (2026-01-01 10:00) for one tenant.
    rows = [
        ("msg1", datetime(2026, 1, 1, 10, 5, 0), datetime(2026, 1, 1, 10, 0, 0), "tenantA"),
        ("msg2", datetime(2026, 1, 1, 10, 10, 0), datetime(2026, 1, 1, 10, 20, 0), "tenantA"),
    ]
    eval_df = _eval_df(spark, rows)

    _, latency_hourly, latency_valid, dq_excluded_hourly, eval_count_hourly = (
        etl._aggregate_evaluated(eval_df)
    )

    assert dq_excluded_hourly.count() == 1
    assert dq_excluded_hourly.collect()[0]["dq_excluded_count"] == 1

    assert eval_count_hourly.count() == 1
    assert eval_count_hourly.collect()[0]["evaluation_count"] == 2

    # Latency aggregates still only reflect the valid row.
    assert latency_valid.count() == 1
    assert latency_hourly.collect()[0]["avg_evaluation_time_ms"] == pytest.approx(
        300000.0
    )


def test_build_combined_reconciles_evaluation_and_dq_excluded_counts(etl, spark):
    rows = [
        ("msg1", datetime(2026, 1, 1, 10, 5, 0), datetime(2026, 1, 1, 10, 0, 0), "tenantA"),
        ("msg2", datetime(2026, 1, 1, 10, 10, 0), datetime(2026, 1, 1, 10, 20, 0), "tenantA"),
    ]
    eval_df = _eval_df(spark, rows)

    (
        counts_hourly,
        latency_hourly,
        latency_valid,
        dq_excluded_hourly,
        eval_count_hourly,
    ) = etl._aggregate_evaluated(eval_df)

    combined = etl._build_combined(
        _empty_received_hourly(spark),
        counts_hourly,
        latency_hourly,
        latency_valid,
        dq_excluded_hourly,
        eval_count_hourly,
    )

    hourly_row = combined.filter(
        combined.metric_granularity == "Hourly"
    ).collect()[0]
    assert hourly_row["evaluation_count"] == 2
    assert hourly_row["dq_excluded_count"] == 1

    daily_row = combined.filter(combined.metric_granularity == "Daily").collect()[0]
    assert daily_row["evaluation_count"] == 2
    assert daily_row["dq_excluded_count"] == 1


def test_build_combined_preserves_latency_for_null_tx_msg_id_bucket(etl, spark):
    # A single valid-latency row with no tx_msg_id (so absent from
    # counts_hourly) and no received row in its bucket. Before the
    # latency_hourly join was changed from "left" to "full", this bucket's
    # avg/p95 latency was dropped at the latency_hourly join step and only
    # resurrected (with nulled-out latency) by the later eval_count_hourly
    # full join (see PR #182 review discussion).
    rows = [
        (None, datetime(2026, 1, 1, 10, 5, 0), datetime(2026, 1, 1, 10, 0, 0), "tenantA"),
    ]
    eval_df = _eval_df(spark, rows)

    (
        counts_hourly,
        latency_hourly,
        latency_valid,
        dq_excluded_hourly,
        eval_count_hourly,
    ) = etl._aggregate_evaluated(eval_df)

    assert counts_hourly.count() == 0
    assert dq_excluded_hourly.count() == 0
    assert latency_valid.count() == 1

    combined = etl._build_combined(
        _empty_received_hourly(spark),
        counts_hourly,
        latency_hourly,
        latency_valid,
        dq_excluded_hourly,
        eval_count_hourly,
    )

    hourly_row = combined.filter(
        combined.metric_granularity == "Hourly"
    ).collect()[0]
    assert hourly_row["evaluation_count"] == 1
    assert hourly_row["dq_excluded_count"] == 0
    assert hourly_row["transactions_evaluated"] == 0
    assert hourly_row["avg_evaluation_time_ms"] == pytest.approx(300000.0)
    assert hourly_row["p95_evaluation_time_ms"] == pytest.approx(300000.0)


def test_aggregate_evaluated_scopes_counts_per_tenant(etl, spark):
    # Two tenants evaluating in the same hour bucket: counts must stay split
    # by tenant instead of being blended into one cross-tenant row.
    eval_df = _eval_df(
        spark,
        [
            ("msgA", datetime(2026, 1, 1, 10, 5, 0), datetime(2026, 1, 1, 10, 0, 0), "tenantA"),
            ("msgB", datetime(2026, 1, 1, 10, 6, 0), datetime(2026, 1, 1, 10, 0, 0), "tenantB"),
            ("msgB2", datetime(2026, 1, 1, 10, 7, 0), datetime(2026, 1, 1, 10, 0, 0), "tenantB"),
        ],
    )

    (
        counts_hourly,
        _latency_hourly,
        latency_valid,
        _dq_excluded_hourly,
        eval_count_hourly,
    ) = etl._aggregate_evaluated(eval_df)

    assert counts_hourly.count() == 2
    assert {
        r["tenant_id"]: r["transactions_evaluated"] for r in counts_hourly.collect()
    } == {"tenantA": 1, "tenantB": 2}
    assert latency_valid.count() == 3
    assert {
        r["tenant_id"]: r["evaluation_count"] for r in eval_count_hourly.collect()
    } == {"tenantA": 1, "tenantB": 2}


def test_build_combined_keeps_tenants_separate_in_same_bucket(etl, spark):
    # The core #185 regression guard: two tenants' transactions in the same
    # time bucket must produce two separate rows, not one blended row.
    received_hourly = etl._aggregate_received(
        _received_df(
            spark,
            [
                ("e2eA", datetime(2026, 1, 1, 10, 5, 0), "pacs.008.001.10", "tenantA"),
                ("e2eB", datetime(2026, 1, 1, 10, 6, 0), "pacs.008.001.10", "tenantB"),
                ("e2eB2", datetime(2026, 1, 1, 10, 7, 0), "pacs.008.001.10", "tenantB"),
            ],
        )
    )
    (
        counts_hourly,
        latency_hourly,
        latency_valid,
        dq_excluded_hourly,
        eval_count_hourly,
    ) = etl._aggregate_evaluated(
        _eval_df(
            spark,
            [
                ("msgA", datetime(2026, 1, 1, 10, 5, 0), datetime(2026, 1, 1, 10, 0, 0), "tenantA"),
                ("msgB", datetime(2026, 1, 1, 10, 6, 0), datetime(2026, 1, 1, 10, 0, 0), "tenantB"),
            ],
        )
    )

    combined = etl._build_combined(
        received_hourly,
        counts_hourly,
        latency_hourly,
        latency_valid,
        dq_excluded_hourly,
        eval_count_hourly,
    )

    hourly = {
        r["tenant_id"]: r
        for r in combined.filter(combined.metric_granularity == "Hourly").collect()
    }
    assert set(hourly) == {"tenantA", "tenantB"}
    assert hourly["tenantA"]["transactions_received"] == 1
    assert hourly["tenantB"]["transactions_received"] == 2
    assert hourly["tenantA"]["transactions_evaluated"] == 1
    assert hourly["tenantB"]["transactions_evaluated"] == 1
    # Counts must not be blended across the two tenants in the shared bucket.
    assert hourly["tenantA"]["evaluation_count"] == 1
    assert hourly["tenantB"]["evaluation_count"] == 1


def test_rebuild_promotes_staged_table_and_keeps_live_table_on_failure(
    spark, tmp_path, monkeypatch
):
    # Hudi does not migrate _hoodie_record_key for rows already written, so
    # rebuild() must regenerate the table. The replacement is built at a
    # staging path and only promoted after the build succeeds — a failed
    # rebuild must leave the live table untouched (CodeRabbit, PR #186).
    etl = MetricsTMSETL(spark, warehouse_root=tmp_path.as_uri())
    live_dir = tmp_path / "gold" / "metrics" / "tms"
    staging_dir = tmp_path / "gold" / "metrics" / "tms__rebuild_staging"
    live_dir.mkdir(parents=True)
    (live_dir / "stale_old_key_row.parquet").write_text(
        "row written under the old 6-field record key"
    )

    # --- Failure case: the aggregation raises mid-rebuild. ---
    def failing_gold():
        raise RuntimeError("aggregation exploded")

    monkeypatch.setattr(etl, "gold", failing_gold)
    with pytest.raises(RuntimeError):
        etl.rebuild("ignored-source-path")

    assert (live_dir / "stale_old_key_row.parquet").exists()  # live table intact
    assert not staging_dir.exists()  # partial staging table cleaned up
    assert etl.metrics_root == live_dir.as_uri()  # metrics_root restored

    # --- Success case: staged build is promoted onto the live path. ---
    def working_gold():
        staging_dir.mkdir(parents=True, exist_ok=True)
        (staging_dir / "new_key_row.parquet").write_text(
            "row written under the new 7-field record key"
        )
        return etl.metrics_root

    monkeypatch.setattr(etl, "gold", working_gold)
    etl.rebuild("ignored-source-path")

    assert not staging_dir.exists()  # staging path consumed by the promotion
    assert (live_dir / "new_key_row.parquet").exists()  # new table is live
    assert not (live_dir / "stale_old_key_row.parquet").exists()  # old table gone
    assert etl.metrics_root == live_dir.as_uri()


def test_drop_existing_table_is_a_noop_when_table_missing(spark, tmp_path):
    # rebuild() must be safe to re-run once the table has already been dropped.
    etl = MetricsTMSETL(spark, warehouse_root=tmp_path.as_uri())
    etl._drop_existing_table()  # must not raise when gold/metrics/tms is absent
