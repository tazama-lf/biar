"""Regression tests for MetricsTMSETL's DQ-exclusion handling.

Covers the case where a valid-latency row and a negative-latency
(DQ-excluded) row land in the *same* hourly bucket, which previously
caused `evaluation_count` to silently drop the excluded row, and the case
where a valid-latency row with no `tx_msg_id` had its avg/p95 latency
silently dropped by an intermediate left join (see PR #182 review
discussion).
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
)

from Table_ETLs.metrics_tms_etl import MetricsTMSETL


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
            StructField("transactions_received", LongType()),
        ]
    )
    return spark.createDataFrame([], schema)


def test_evaluation_count_includes_dq_excluded_rows_in_shared_bucket(etl, spark):
    # One valid-latency row and one negative-latency (DQ-excluded) row, both
    # landing in the same hour bucket (2026-01-01 10:00).
    rows = [
        ("msg1", datetime(2026, 1, 1, 10, 5, 0), datetime(2026, 1, 1, 10, 0, 0)),
        ("msg2", datetime(2026, 1, 1, 10, 10, 0), datetime(2026, 1, 1, 10, 20, 0)),
    ]
    eval_df = spark.createDataFrame(
        rows, ["tx_msg_id", "event_ts", "dc_cre_dt_tm"]
    )

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
        ("msg1", datetime(2026, 1, 1, 10, 5, 0), datetime(2026, 1, 1, 10, 0, 0)),
        ("msg2", datetime(2026, 1, 1, 10, 10, 0), datetime(2026, 1, 1, 10, 20, 0)),
    ]
    eval_df = spark.createDataFrame(
        rows, ["tx_msg_id", "event_ts", "dc_cre_dt_tm"]
    )

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
        (None, datetime(2026, 1, 1, 10, 5, 0), datetime(2026, 1, 1, 10, 0, 0)),
    ]
    eval_df = spark.createDataFrame(
        rows, ["tx_msg_id", "event_ts", "dc_cre_dt_tm"]
    )

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
