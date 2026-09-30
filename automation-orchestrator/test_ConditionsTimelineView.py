"""Regression tests for ConditionsTimelineViewETL's EFRuP subruleref sourcing.

Covers PR #189 (issue #178): ``vw_conditions_timeline`` no longer derives a
block/override status from the payment-network ``tx_status`` (ISO 20022
pacs.002 ``TxSts``). It instead carries ``gold/alerts.efrup_subruleref`` -- the
canonical EFRuP ``subRuleRef`` datum -- through the existing alerts left-join,
exposing the raw ``block`` / ``override`` / ``none`` values and staying NULL
when no EFRuP result exists for the transaction.

The production pipeline is exercised end-to-end at the ``bronze()`` level: the
Hudi-backed reads are replaced with in-memory tables and the Hudi write is
captured, so the real alerts join, bucketizing, window filtering, PK
derivation and final projection all run for real.

Follows the same local-Spark + pytest pattern as ``test_metrics_tms_etl.py``.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from Table_ETLs.ConditionsTimelineView import ConditionsTimelineViewETL

# ---------------------------------------------------------------------------
# Source-table schemas (faithful subset of the gold tables the view reads)
# ---------------------------------------------------------------------------

CONDITION_SCHEMA = StructType(
    [
        StructField("pk", StringType()),
        StructField("condition_id", StringType()),
        StructField("tenant_id", StringType()),
        StructField("created_by_user", StringType()),
        StructField("account_id", StringType()),
        StructField("account_scheme", StringType()),
        StructField("account_agent_mmb_id", StringType()),
        StructField("event_types_csv", StringType()),
        StructField("event_type_primary", StringType()),
        StructField("event_type_count", IntegerType()),
        StructField("condition_type", StringType()),
        StructField("perspective", StringType()),
        StructField("condition_reason", StringType()),
        StructField("force_create", BooleanType()),
        StructField("condition_created_ts", TimestampType()),
        StructField("condition_inception_ts", TimestampType()),
        StructField("condition_expiry_ts", TimestampType()),
        StructField("condition_created_date", StringType()),
        StructField("is_active", BooleanType()),
        StructField("is_expired", BooleanType()),
        StructField("ingested_at_ts", TimestampType()),
    ]
)

TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType()),
        StructField("endtoendid", StringType()),
        StructField("tenantid", StringType()),
        StructField("txtp", StringType()),
        StructField("msgid", StringType()),
        StructField("txsts", StringType()),
        StructField("amt", DoubleType()),
        StructField("ccy", StringType()),
        StructField("event_ts", TimestampType()),
        StructField("event_date", DateType()),
    ]
)

ALERT_SCHEMA = StructType(
    [
        StructField("alert_id", LongType()),
        StructField("case_id", LongType()),
        StructField("tx_msg_id", StringType()),
        StructField("efrup_subruleref", StringType()),
    ]
)

CASE_SCHEMA = StructType([StructField("case_id", LongType())])

TASK_SCHEMA = StructType(
    [
        StructField("case_id", LongType()),
        StructField("is_completed", IntegerType()),
    ]
)

# ---------------------------------------------------------------------------
# Row builders (defaults describe a single condition/transaction/alert set
# that shares one bucket in every granularity)
# ---------------------------------------------------------------------------

_DEFAULT_INCEPTION = datetime(2026, 1, 1, 0, 0, 0)
_DEFAULT_EVENT_TS = datetime(2026, 1, 1, 10, 0, 0)


def _condition_row(**overrides) -> dict:
    row = {
        "pk": "cond-pk-1",
        "condition_id": "C1",
        "tenant_id": "T1",
        "created_by_user": "user-1",
        "account_id": "ACC-1",
        "account_scheme": "IBAN",
        "account_agent_mmb_id": "MMB-1",
        "event_types_csv": "PACS008",
        "event_type_primary": "PACS008",
        "event_type_count": 1,
        "condition_type": "account",
        "perspective": "payer",
        "condition_reason": "review",
        "force_create": False,
        "condition_created_ts": _DEFAULT_INCEPTION,
        "condition_inception_ts": _DEFAULT_INCEPTION,
        "condition_expiry_ts": None,
        "condition_created_date": "2026-01-01",
        "is_active": True,
        "is_expired": False,
        "ingested_at_ts": _DEFAULT_INCEPTION,
    }
    row.update(overrides)
    return row


def _transaction_row(**overrides) -> dict:
    row = {
        "transaction_id": "TX-1",
        "endtoendid": "E2E-1",
        "tenantid": "T1",
        "txtp": "PACS008",
        "msgid": "TXMSG-1",
        "txsts": "ACSC",
        "amt": 100.0,
        "ccy": "USD",
        "event_ts": _DEFAULT_EVENT_TS,
        "event_date": date(2026, 1, 1),
    }
    row.update(overrides)
    return row


def _alert_row(**overrides) -> dict:
    row = {
        "alert_id": 1,
        "case_id": 100,
        "tx_msg_id": "TXMSG-1",
        "efrup_subruleref": "block",
    }
    row.update(overrides)
    return row


def _df(spark, rows, schema):
    return spark.createDataFrame(rows, schema)


def _registry(spark, conditions=None, transactions=None, alerts=None, **extra):
    """Build a path -> DataFrame registry for the stubbed SparkSession read."""
    registry = {
        "gold/condition": _df(
            spark,
            conditions if conditions is not None else [_condition_row()],
            CONDITION_SCHEMA,
        ),
        "gold/transactions": _df(
            spark,
            transactions if transactions is not None else [_transaction_row()],
            TRANSACTION_SCHEMA,
        ),
        "gold/alerts": _df(
            spark, alerts if alerts is not None else [_alert_row()], ALERT_SCHEMA
        ),
    }
    registry.update(extra)
    return registry


# ---------------------------------------------------------------------------
# Stubs: serve Hudi reads from in-memory tables and capture the Hudi write
# ---------------------------------------------------------------------------


class _StubReader:
    def __init__(self, registry):
        self._registry = registry

    def format(self, source):
        assert source == "hudi"
        return self

    def load(self, path):
        key = "/".join(str(path).replace("\\", "/").rstrip("/").split("/")[-2:])
        if key not in self._registry:
            raise FileNotFoundError(f"no stub table registered for {key}")
        return self._registry[key]


class _StubSpark:
    """Delegates to a real SparkSession but serves ``.read`` from a registry."""

    def __init__(self, real_spark, registry):
        self._real = real_spark
        self._registry = registry

    @property
    def read(self):
        return _StubReader(self._registry)

    def __getattr__(self, item):
        return getattr(self._real, item)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("conditions-timeline-view-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture()
def build_view(spark):
    """Run the real ``bronze()`` with stubbed reads; return the captured state."""

    def _build(registry, warehouse_root="/tmp/unused"):
        etl = ConditionsTimelineViewETL(spark, warehouse_root)
        etl.spark = _StubSpark(spark, registry)
        captured = {}

        def _fake_write_hudi(df, path, opts):
            captured["df"] = df
            captured["path"] = path
            captured["opts"] = opts

        etl.write_hudi = _fake_write_hudi
        captured["built_path"] = etl.bronze()
        captured["etl"] = etl
        return captured

    return _build


def _collect(df):
    """Collect rows with timestamp columns rendered as strings.

    Casting avoids a Windows-only pyspark limitation where collecting
    pre-epoch timestamps (the 1900-01-01 sentinel used for the "all"
    bucket) raises ``OSError: [Errno 22] Invalid argument``. The view
    itself is unaffected (it runs on Linux in Docker); this is purely a
    local test-runner constraint.
    """
    ts_cols = {
        f.name for f in df.schema.fields if isinstance(f.dataType, TimestampType)
    }
    return df.select(
        *[
            (
                F.date_format(F.col(c), "yyyy-MM-dd HH:mm:ss").alias(c)
                if c in ts_cols
                else F.col(c)
            )
            for c in df.columns
        ]
    ).collect()


def _row_for(df, granularity="day"):
    """Return the single projected row for a granularity."""
    rows = [r for r in _collect(df) if r["bucket_granularity"] == granularity]
    assert len(rows) == 1, f"expected exactly one {granularity} row, got {len(rows)}"
    return rows[0]


# ---------------------------------------------------------------------------
# PR #189 core: efrup_subruleref replaces the tx_status-derived block status
# ---------------------------------------------------------------------------

LEGACY_COLUMN = "tx_block_override_status"


def test_view_exposes_efrup_subruleref_column(build_view, spark):
    captured = build_view(_registry(spark))
    assert "efrup_subruleref" in captured["df"].columns


def test_view_does_not_expose_legacy_tx_block_override_status(build_view, spark):
    captured = build_view(_registry(spark))
    assert LEGACY_COLUMN not in captured["df"].columns


def test_view_retains_tx_status_as_benign_carriage(build_view, spark):
    # tx_status is kept for display but must not drive any block/override value.
    captured = build_view(_registry(spark))
    assert "tx_status" in captured["df"].columns
    assert _row_for(captured["df"])["tx_status"] == "ACSC"


@pytest.mark.parametrize(
    "efrup_value",
    [
        "block",
        "override",
        "none",
        "BLOCK",
        "NONE",
        "Override",
        "block ",
        " blocked",
        "",
        None,
    ],
)
def test_efrup_subruleref_is_passed_through_verbatim(build_view, spark, efrup_value):
    captured = build_view(
        _registry(spark, alerts=[_alert_row(efrup_subruleref=efrup_value)])
    )
    assert _row_for(captured["df"])["efrup_subruleref"] == efrup_value


PACS002_TXSTS = [
    "ACCP",
    "ACSC",
    "ACSP",
    "ACTC",
    "ACWP",
    "CANC",
    "PART",
    "PDNG",
    "RJCT",
    "BLOCKED",
    "REJECTED",
]


@pytest.mark.parametrize("tx_status", PACS002_TXSTS)
def test_tx_status_does_not_influence_efrup_subruleref(build_view, spark, tx_status):
    # Whatever the payment-network TxSts value, efrup_subruleref comes from the
    # alert and is never derived from tx_status.
    captured = build_view(
        _registry(
            spark,
            transactions=[_transaction_row(txsts=tx_status)],
            alerts=[_alert_row(efrup_subruleref="override")],
        )
    )
    row = _row_for(captured["df"])
    assert row["tx_status"] == tx_status
    assert row["efrup_subruleref"] == "override"


@pytest.mark.parametrize("tx_status", ["BLOCKED", "REJECTED"])
def test_bogus_blocked_rejected_status_never_produces_blocked_override(
    build_view, spark, tx_status
):
    # The removed derivation mapped BLOCKED/REJECTED -> "BLOCKED". Even when the
    # alert returned "none", the view must not promote it to a block status.
    captured = build_view(
        _registry(
            spark,
            transactions=[_transaction_row(txsts=tx_status)],
            alerts=[_alert_row(efrup_subruleref="none")],
        )
    )
    row = _row_for(captured["df"])
    assert row["efrup_subruleref"] == "none"
    assert row["efrup_subruleref"] != "BLOCKED"


@pytest.mark.parametrize("granularity", ("day", "week", "month", "year", "all"))
def test_efrup_subruleref_sourced_for_every_granularity(build_view, spark, granularity):
    captured = build_view(_registry(spark))
    assert _row_for(captured["df"], granularity)["efrup_subruleref"] == "block"


def test_efrup_subruleref_is_null_when_no_alert_matches(build_view, spark):
    # No alert row at all -> left-join produces NULL, not a "none" default.
    captured = build_view(_registry(spark, alerts=[]))
    row = _row_for(captured["df"])
    assert row["efrup_subruleref"] is None
    assert row["tx_is_alerted"] == 0


def test_efrup_subruleref_is_null_when_alert_has_unrelated_tx_msg_id(build_view, spark):
    captured = build_view(
        _registry(spark, alerts=[_alert_row(tx_msg_id="TXMSG-OTHER")])
    )
    row = _row_for(captured["df"])
    assert row["efrup_subruleref"] is None
    assert row["tx_is_alerted"] == 0


def test_efrup_subruleref_is_null_when_alert_tx_msg_id_is_null(build_view, spark):
    captured = build_view(_registry(spark, alerts=[_alert_row(tx_msg_id=None)]))
    row = _row_for(captured["df"])
    assert row["efrup_subruleref"] is None
    assert row["tx_is_alerted"] == 0


def test_efrup_subruleref_is_null_when_alert_value_is_null(build_view, spark):
    # Alert exists (tx is alerted) but EFRuP produced no subRuleRef -> NULL,
    # and it must NOT be defaulted to "none".
    captured = build_view(_registry(spark, alerts=[_alert_row(efrup_subruleref=None)]))
    row = _row_for(captured["df"])
    assert row["tx_is_alerted"] == 1
    assert row["efrup_subruleref"] is None


def test_duplicate_alerts_for_same_tx_msg_id_do_not_fan_out(build_view, spark):
    captured = build_view(
        _registry(
            spark,
            alerts=[
                _alert_row(alert_id=1, efrup_subruleref="block"),
                _alert_row(alert_id=2, efrup_subruleref="override"),
            ],
        )
    )
    day_rows = [r for r in _collect(captured["df"]) if r["bucket_granularity"] == "day"]
    assert len(day_rows) == 1


def test_efrup_subruleref_not_confused_with_tx_status_value(build_view, spark):
    # tx_status "none"-like vs efrup "block": the two columns stay independent.
    captured = build_view(
        _registry(
            spark,
            transactions=[_transaction_row(txsts="PDNG")],
            alerts=[_alert_row(efrup_subruleref="block")],
        )
    )
    row = _row_for(captured["df"])
    assert row["tx_status"] == "PDNG"
    assert row["efrup_subruleref"] == "block"


def test_multiple_transactions_carry_their_own_efrup_value(build_view, spark):
    captured = build_view(
        _registry(
            spark,
            transactions=[
                _transaction_row(transaction_id="TX-1", msgid="TXMSG-1"),
                _transaction_row(transaction_id="TX-2", msgid="TXMSG-2"),
            ],
            alerts=[
                _alert_row(alert_id=1, tx_msg_id="TXMSG-1", efrup_subruleref="block"),
                _alert_row(alert_id=2, tx_msg_id="TXMSG-2", efrup_subruleref="none"),
            ],
        )
    )
    by_tx = {
        r["tx_transaction_id"]: r["efrup_subruleref"]
        for r in _collect(captured["df"])
        if r["bucket_granularity"] == "day"
    }
    assert by_tx == {"TX-1": "block", "TX-2": "none"}


def test_efrup_subruleref_defaults_to_null_when_cases_and_tasks_missing(
    build_view, spark
):
    # No gold/cases or gold/tasks registered -> graceful except branches; the
    # EFRuP value still flows through the alerts join.
    captured = build_view(_registry(spark))
    row = _row_for(captured["df"])
    assert row["efrup_subruleref"] == "block"
    assert row["tx_is_investigated"] == 0


# ---------------------------------------------------------------------------
# View shape, granularities and primary key
# ---------------------------------------------------------------------------


def test_view_emits_all_five_granularities(build_view, spark):
    captured = build_view(_registry(spark))
    got = {r["bucket_granularity"] for r in _collect(captured["df"])}
    assert got == {"day", "week", "month", "year", "all"}


def test_all_granularity_bucket_start_is_sentinel(build_view, spark):
    captured = build_view(_registry(spark))
    assert _row_for(captured["df"], "all")["bucket_start"] == "1900-01-01 00:00:00"


def test_day_bucket_start_truncates_to_midnight(build_view, spark):
    captured = build_view(_registry(spark))
    assert _row_for(captured["df"], "day")["bucket_start"] == "2026-01-01 00:00:00"


def test_month_bucket_start_truncates_to_first_of_month(build_view, spark):
    captured = build_view(_registry(spark))
    assert _row_for(captured["df"], "month")["bucket_start"] == "2026-01-01 00:00:00"


def test_year_bucket_start_truncates_to_first_of_year(build_view, spark):
    captured = build_view(_registry(spark))
    assert _row_for(captured["df"], "year")["bucket_start"] == "2026-01-01 00:00:00"


def test_week_bucket_start_is_monday_of_week(build_view, spark):
    # 2026-01-01 is a Thursday; its ISO week starts Monday 2025-12-29.
    captured = build_view(_registry(spark))
    assert _row_for(captured["df"], "week")["bucket_start"] == "2025-12-29 00:00:00"


def test_view_has_primary_key_column(build_view, spark):
    captured = build_view(_registry(spark))
    assert "pk" in captured["df"].columns


def test_primary_keys_are_unique(build_view, spark):
    rows = _collect(build_view(_registry(spark))["df"])
    pks = [r["pk"] for r in rows]
    assert len(pks) == len(set(pks))


def test_primary_key_is_deterministic_across_rebuilds(build_view, spark):
    first = {r["pk"] for r in _collect(build_view(_registry(spark))["df"])}
    second = {r["pk"] for r in _collect(build_view(_registry(spark))["df"])}
    assert first == second


def test_primary_key_is_sha256_hex(build_view, spark):
    captured = build_view(_registry(spark))
    pk = _row_for(captured["df"])["pk"]
    assert len(pk) == 64
    int(pk, 16)  # raises if not valid hex


def test_ingested_at_ts_is_populated(build_view, spark):
    captured = build_view(_registry(spark))
    assert _row_for(captured["df"])["ingested_at_ts"] is not None


# ---------------------------------------------------------------------------
# Alert / case / task derived flags
# ---------------------------------------------------------------------------


def test_is_alerted_flag_set_when_alert_matches(build_view, spark):
    captured = build_view(_registry(spark))
    assert _row_for(captured["df"])["tx_is_alerted"] == 1


def test_is_investigated_flag_from_matching_case(build_view, spark):
    captured = build_view(
        _registry(spark, **{"gold/cases": _df(spark, [(100,)], CASE_SCHEMA)})
    )
    assert _row_for(captured["df"])["tx_is_investigated"] == 1


def test_is_investigated_flag_from_completed_task(build_view, spark):
    captured = build_view(
        _registry(
            spark,
            **{"gold/tasks": _df(spark, [(100, 1)], TASK_SCHEMA)},
        )
    )
    assert _row_for(captured["df"])["tx_is_investigated"] == 1


def test_is_investigated_flag_zero_when_task_incomplete(build_view, spark):
    captured = build_view(
        _registry(
            spark,
            **{"gold/tasks": _df(spark, [(100, 0)], TASK_SCHEMA)},
        )
    )
    assert _row_for(captured["df"])["tx_is_investigated"] == 0


def test_is_investigated_flag_zero_when_case_unrelated(build_view, spark):
    captured = build_view(
        _registry(spark, **{"gold/cases": _df(spark, [(999,)], CASE_SCHEMA)})
    )
    assert _row_for(captured["df"])["tx_is_investigated"] == 0


# ---------------------------------------------------------------------------
# Window / event-type join filters
# ---------------------------------------------------------------------------


def test_transaction_after_expiry_does_not_join(build_view, spark):
    captured = build_view(
        _registry(
            spark,
            conditions=[
                _condition_row(condition_expiry_ts=datetime(2026, 1, 1, 9, 0, 0))
            ],
        )
    )
    row = _row_for(captured["df"])
    assert row["tx_transaction_id"] is None


def test_transaction_before_inception_does_not_join(build_view, spark):
    captured = build_view(
        _registry(
            spark,
            conditions=[
                _condition_row(condition_inception_ts=datetime(2026, 6, 1, 0, 0, 0))
            ],
        )
    )
    row = _row_for(captured["df"], "all")
    assert row["tx_transaction_id"] is None


def test_transaction_within_expiry_window_joins(build_view, spark):
    captured = build_view(
        _registry(
            spark,
            conditions=[
                _condition_row(condition_expiry_ts=datetime(2026, 1, 2, 0, 0, 0))
            ],
        )
    )
    assert _row_for(captured["df"])["tx_transaction_id"] == "TX-1"


def test_event_type_mismatch_does_not_join(build_view, spark):
    captured = build_view(
        _registry(
            spark,
            conditions=[_condition_row(event_types_csv="PACS002")],
        )
    )
    assert _row_for(captured["df"])["tx_transaction_id"] is None


def test_event_type_match_joins(build_view, spark):
    captured = build_view(
        _registry(
            spark,
            conditions=[_condition_row(event_types_csv="PACS008,PACS002")],
        )
    )
    assert _row_for(captured["df"])["tx_transaction_id"] == "TX-1"


def test_null_event_types_csv_matches_any_transaction(build_view, spark):
    captured = build_view(
        _registry(spark, conditions=[_condition_row(event_types_csv=None)])
    )
    assert _row_for(captured["df"])["tx_transaction_id"] == "TX-1"


def test_null_tx_type_matches_any_condition(build_view, spark):
    captured = build_view(
        _registry(
            spark,
            transactions=[_transaction_row(txtp=None)],
            conditions=[_condition_row(event_types_csv="PACS008")],
        )
    )
    assert _row_for(captured["df"])["tx_transaction_id"] == "TX-1"


def test_tenant_mismatch_keeps_condition_with_null_transaction(build_view, spark):
    captured = build_view(
        _registry(spark, transactions=[_transaction_row(tenantid="T2")])
    )
    row = _row_for(captured["df"])
    assert row["cond_condition_id"] == "C1"
    assert row["tx_transaction_id"] is None


def test_condition_without_transaction_is_retained(build_view, spark):
    captured = build_view(_registry(spark, transactions=[]))
    rows = [r for r in _collect(captured["df"]) if r["bucket_granularity"] == "day"]
    assert len(rows) == 1
    assert rows[0]["tx_transaction_id"] is None


# ---------------------------------------------------------------------------
# Carried / aliased columns
# ---------------------------------------------------------------------------


def test_transaction_fields_are_carried(build_view, spark):
    captured = build_view(_registry(spark))
    row = _row_for(captured["df"])
    assert row["tx_end_to_end_id"] == "E2E-1"
    assert row["tx_msg_id"] == "TXMSG-1"
    assert row["tx_type"] == "PACS008"
    assert row["tx_amount"] == pytest.approx(100.0)
    assert row["tx_ccy"] == "USD"


def test_condition_fields_are_aliased_with_cond_prefix(build_view, spark):
    captured = build_view(_registry(spark))
    row = _row_for(captured["df"])
    assert row["cond_condition_id"] == "C1"
    assert row["cond_pk"] == "cond-pk-1"
    assert row["cond_tenant_id"] == "T1"
    assert row["cond_account_id"] == "ACC-1"
    assert row["cond_type"] == "account"
    assert row["cond_perspective"] == "payer"


def test_multiple_conditions_produce_multiple_rows(build_view, spark):
    captured = build_view(
        _registry(
            spark,
            conditions=[
                _condition_row(condition_id="C1", pk="pk-1"),
                _condition_row(condition_id="C2", pk="pk-2"),
            ],
        )
    )
    day = [r for r in _collect(captured["df"]) if r["bucket_granularity"] == "day"]
    assert len(day) == 2


def test_no_conditions_produces_empty_view(build_view, spark):
    captured = build_view(_registry(spark, conditions=[]))
    assert len(_collect(captured["df"])) == 0


# ---------------------------------------------------------------------------
# Write target / options and path plumbing
# ---------------------------------------------------------------------------


def test_bronze_writes_to_views_conditions_timeline(build_view, spark):
    captured = build_view(_registry(spark))
    assert captured["path"] == "/tmp/unused/views/conditions_timeline"
    assert captured["built_path"] == captured["path"]


def test_write_uses_pk_record_key_and_ingested_at_precombine(build_view, spark):
    captured = build_view(_registry(spark))
    opts = captured["opts"]
    assert opts["hoodie.datasource.write.recordkey.field"] == "pk"
    assert opts["hoodie.datasource.write.precombine.field"] == "ingested_at_ts"
    assert opts["hoodie.table.name"] == "vw_conditions_timeline"


def test_custom_warehouse_root_is_honoured(build_view, spark):
    captured = build_view(_registry(spark), warehouse_root="/custom/root")
    assert captured["path"] == "/custom/root/views/conditions_timeline"


def test_silver_and_gold_return_view_path(spark):
    etl = ConditionsTimelineViewETL(spark, "/tmp/unused")
    assert etl.silver() == "/tmp/unused/views/conditions_timeline"
    assert etl.gold() == "/tmp/unused/views/conditions_timeline"


def test_run_builds_and_returns_view_path(spark):
    etl = ConditionsTimelineViewETL(spark, "/tmp/unused")
    etl.spark = _StubSpark(spark, _registry(spark))
    written = {}
    etl.write_hudi = lambda df, path, opts: written.update(path=path)
    result = etl.run()
    assert result == "/tmp/unused/views/conditions_timeline"
    assert written["path"] == result


# ---------------------------------------------------------------------------
# Exhaustive: canonical EFRuP values are preserved in every granularity
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("granularity", ("day", "week", "month", "year", "all"))
@pytest.mark.parametrize("efrup_value", ("block", "override", "none"))
def test_efrup_value_preserved_per_granularity(
    build_view, spark, efrup_value, granularity
):
    captured = build_view(
        _registry(spark, alerts=[_alert_row(efrup_subruleref=efrup_value)])
    )
    assert _row_for(captured["df"], granularity)["efrup_subruleref"] == efrup_value


# ---------------------------------------------------------------------------
# Condition column aliasing (cond_* projection)
# ---------------------------------------------------------------------------

CONDITION_ALIASES = [
    ("cond_condition_id", "C1"),
    ("cond_pk", "cond-pk-1"),
    ("cond_tenant_id", "T1"),
    ("cond_account_id", "ACC-1"),
    ("cond_account_scheme", "IBAN"),
    ("cond_account_agent_mmb_id", "MMB-1"),
    ("cond_type", "account"),
    ("cond_perspective", "payer"),
    ("cond_reason", "review"),
    ("cond_event_types_csv", "PACS008"),
    ("cond_event_type_primary", "PACS008"),
    ("cond_event_type_count", 1),
]


@pytest.mark.parametrize("column,expected", CONDITION_ALIASES)
def test_condition_columns_are_projected_under_expected_alias(
    build_view, spark, column, expected
):
    captured = build_view(_registry(spark))
    assert column in captured["df"].columns
    assert _row_for(captured["df"])[column] == expected
