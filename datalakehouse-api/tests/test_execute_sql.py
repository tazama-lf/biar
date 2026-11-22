# SPDX-License-Identifier: Apache-2.0
"""Tests for the per-session Hudi view registration in lakehouse_query_api.py.

Spark and findspark are replaced with in-memory fakes, so no JVM is needed.
The fakes model just enough of the Spark catalog (temp views, tenant filter,
SQL referencing views) to check the registration/rewrite/tenant behaviour.

Run with:
    cd datalakehouse-api
    pytest tests/test_execute_sql.py -v
"""
import importlib
import logging
import re
import sys
import threading
import types
from unittest.mock import MagicMock

import pytest


class FakeCol:
    def __init__(self, name):
        self.name = name

    def __eq__(self, other):
        return ("eq", self.name, other)

    __hash__ = None


class FakeDF:
    def __init__(self, rows, columns, session=None):
        self.rows = rows
        self.columns = columns
        self.session = session

    def filter(self, cond):
        _, col, value = cond
        return FakeDF([r for r in self.rows if r.get(col) == value], self.columns, self.session)

    def limit(self, n):
        return FakeDF(self.rows[:n], self.columns, self.session)

    def collect(self):
        return [types.SimpleNamespace(asDict=lambda recursive=True, r=r: dict(r)) for r in self.rows]

    def createOrReplaceTempView(self, name):
        self.session.views[name.lower()] = self


class FakeReader:
    def __init__(self, session):
        self.session = session

    def format(self, _fmt):
        return self

    def load(self, path):
        self.session.loads.append(path)
        rows = self.session.data[path]
        return FakeDF(rows, list(rows[0].keys()) if rows else [], self.session)


class FakeCatalog:
    def __init__(self, session):
        self.session = session

    def refreshTable(self, name):
        self.session.refreshed.append(name.lower())
        if self.session.refresh_error:
            raise RuntimeError("refresh boom")


class FakeSpark:
    """Fake session. ``data`` maps a table path to its rows."""

    def __init__(self, data):
        self.data = data
        self.views = {}
        self.loads = []
        self.queries = []
        self.refreshed = []
        self.refresh_error = False
        self.read = FakeReader(self)
        self.catalog = FakeCatalog(self)

    def sql(self, query):
        self.queries.append(query)
        # Resolve like Spark: every referenced view must exist.
        names = re.findall(r"(?:FROM|JOIN)\s+`?(\w+)`?", query, flags=re.I)
        for n in names:
            if n.lower() not in self.views:
                raise RuntimeError(f"[TABLE_OR_VIEW_NOT_FOUND] {n}")
        return FakeDF(self.views[names[0].lower()].rows, [], self)


@pytest.fixture()
def api(monkeypatch, tmp_path):
    """Import lakehouse_query_api with Spark/findspark faked out."""
    fake_pyspark = types.ModuleType("pyspark")
    fake_sql = types.ModuleType("pyspark.sql")
    fake_sql.SparkSession = MagicMock()
    fake_sql.functions = types.SimpleNamespace(col=FakeCol)
    fake_sql.types = MagicMock()
    fake_pyspark.sql = fake_sql
    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", fake_sql.functions)
    monkeypatch.setitem(sys.modules, "pyspark.sql.types", fake_sql.types)
    monkeypatch.setitem(sys.modules, "findspark", MagicMock())
    monkeypatch.setenv("SPARK_HOME", str(tmp_path))
    sys.modules.pop("lakehouse_query_api", None)
    module = importlib.import_module("lakehouse_query_api")
    yield module
    sys.modules.pop("lakehouse_query_api", None)


@pytest.fixture()
def warehouse(api, tmp_path, monkeypatch):
    """Two Gold tables on disk (alerts, conditions) and everything else missing."""
    paths = {}
    data = {}
    for name in ("alerts", "conditions"):
        p = tmp_path / name
        p.mkdir()
        paths[name] = str(p)
    paths["typologies"] = str(tmp_path / "does-not-exist")
    col = api.TENANT_ID_COLUMN
    data[paths["alerts"]] = [{col: "A", "id": 1}, {col: "B", "id": 2}]
    data[paths["conditions"]] = [{col: "A", "id": 1}, {col: "A", "id": 2}, {col: "B", "id": 3}]
    monkeypatch.setattr(api, "GOLD_PATHS", paths)
    return paths, data


def use_session(api, monkeypatch, data):
    spark = FakeSpark(data)
    monkeypatch.setattr(api, "get_spark", lambda: spark)
    return spark


def test_registers_once_across_many_calls(api, warehouse, monkeypatch):
    paths, data = warehouse
    spark = use_session(api, monkeypatch, data)

    for _ in range(100):
        api._execute_sql_sync("SELECT * FROM alerts", "A")

    assert sorted(spark.loads) == sorted([paths["alerts"], paths["conditions"]])


def test_reregisters_when_session_is_recreated(api, warehouse, monkeypatch):
    _, data = warehouse
    first = use_session(api, monkeypatch, data)
    api._execute_sql_sync("SELECT * FROM alerts", "A")

    second = use_session(api, monkeypatch, data)  # simulates get_spark() recreating the session
    rows = api._execute_sql_sync("SELECT * FROM alerts", "A")

    assert len(first.loads) == 2
    assert len(second.loads) == 2
    assert len(rows) == 1


def test_missing_path_warns_once_per_session(api, warehouse, monkeypatch, caplog):
    _, data = warehouse
    use_session(api, monkeypatch, data)
    with caplog.at_level(logging.WARNING, logger="pipeline"):
        for _ in range(20):
            api._execute_sql_sync("SELECT * FROM alerts", "A")

    warnings = [r for r in caplog.records if "typologies" in r.getMessage()]
    assert len(warnings) == 1


def test_unfiltered_data_is_never_registered_in_the_catalog(api, warehouse, monkeypatch):
    _, data = warehouse
    spark = use_session(api, monkeypatch, data)

    api._ensure_registered(spark)

    assert spark.views == {}


def test_every_registered_view_is_tenant_filtered(api, warehouse, monkeypatch):
    _, data = warehouse
    spark = use_session(api, monkeypatch, data)

    api._execute_sql_sync("SELECT * FROM conditions", "A")

    assert spark.views
    for view in spark.views.values():
        assert {r[api.TENANT_ID_COLUMN] for r in view.rows} == {"A"}


def test_tenant_filter_applied(api, warehouse, monkeypatch):
    _, data = warehouse
    use_session(api, monkeypatch, data)

    assert len(api._execute_sql_sync("SELECT * FROM conditions", "A")) == 2
    assert len(api._execute_sql_sync("SELECT * FROM conditions", "B")) == 1


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM CONDITIONS",
        "SELECT * FROM Conditions",
        "select * from cOnDiTiOnS",
        "SELECT * FROM `conditions`",
        "SELECT * FROM `CONDITIONS`",
        "SELECT * FROM (SELECT * FROM CONDITIONS) t",
    ],
)
def test_identifier_casing_and_quoting_cannot_bypass_tenant_filter(api, warehouse, monkeypatch, query):
    _, data = warehouse
    use_session(api, monkeypatch, data)

    rows = api._execute_sql_sync(query, "A")

    assert rows
    assert {r[api.TENANT_ID_COLUMN] for r in rows} == {"A"}


def test_concurrent_tenants_do_not_leak(api, warehouse, monkeypatch):
    _, data = warehouse
    use_session(api, monkeypatch, data)
    results = {}

    def run(tenant, i):
        results[(tenant, i)] = api._execute_sql_sync("SELECT * FROM CONDITIONS", tenant)

    threads = [threading.Thread(target=run, args=(t, i)) for i in range(10) for t in ("A", "B")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for (tenant, _), rows in results.items():
        assert {r[api.TENANT_ID_COLUMN] for r in rows} == {tenant}


def test_limit_applied(api, warehouse, monkeypatch):
    _, data = warehouse
    use_session(api, monkeypatch, data)
    assert len(api._execute_sql_sync("SELECT * FROM conditions", "A", limit=1)) == 1


def test_only_referenced_tables_are_refreshed(api, warehouse, monkeypatch):
    _, data = warehouse
    spark = use_session(api, monkeypatch, data)

    api._execute_sql_sync("SELECT * FROM CONDITIONS", "A")
    api._execute_sql_sync("SELECT * FROM alerts a JOIN conditions c ON a.id = c.id", "A")

    assert spark.refreshed == ["conditions", "alerts", "conditions"]


def test_refresh_failure_falls_back_to_reload_and_stays_filtered(api, warehouse, monkeypatch):
    paths, data = warehouse
    spark = use_session(api, monkeypatch, data)
    api._execute_sql_sync("SELECT * FROM alerts", "A")  # registers (2 loads)
    spark.refresh_error = True

    rows = api._execute_sql_sync("SELECT * FROM alerts", "A")

    assert len(rows) == 1
    assert spark.loads.count(paths["alerts"]) == 2  # registration + fallback reload
    assert {r[api.TENANT_ID_COLUMN] for r in spark.views["alerts"].rows} == {"A"}


def test_absent_table_reports_original_name(api, warehouse, monkeypatch):
    _, data = warehouse
    use_session(api, monkeypatch, data)
    with pytest.raises(RuntimeError, match="typologies"):
        api._execute_sql_sync("SELECT * FROM typologies", "A")


@pytest.mark.parametrize(
    "query, table, expected",
    [
        ("SELECT * FROM alerts", "alerts", True),
        ("SELECT * FROM ALERTS", "alerts", True),
        ("SELECT * FROM `Alerts`", "alerts", True),
        ("SELECT * FROM my_alerts", "alerts", False),
        ("SELECT * FROM conditions", "alerts", False),
    ],
)
def test_references_table_is_case_insensitive(api, query, table, expected):
    assert api._references_table(query, table) is expected


def test_late_arriving_table_is_loaded_once_its_path_exists(api, warehouse, monkeypatch, tmp_path):
    paths, data = warehouse
    spark = use_session(api, monkeypatch, data)
    with pytest.raises(RuntimeError, match="typologies"):
        api._execute_sql_sync("SELECT * FROM typologies", "A")  # path missing at session start

    late = tmp_path / "does-not-exist"
    late.mkdir()  # the ETL wrote its first batch
    data[paths["typologies"]] = [{api.TENANT_ID_COLUMN: "A", "id": 9}, {api.TENANT_ID_COLUMN: "B", "id": 10}]

    rows = api._execute_sql_sync("SELECT * FROM TYPOLOGIES", "A")

    assert [r["id"] for r in rows] == [9]
    assert spark.loads.count(paths["typologies"]) == 1


def test_failed_load_is_retried_on_a_later_request(api, warehouse, monkeypatch):
    paths, data = warehouse
    spark = use_session(api, monkeypatch, data)
    real_load = spark.read.load
    calls = {"n": 0}

    def flaky(path):
        if path == paths["alerts"] and calls["n"] < 2:  # registration + same-request retry
            calls["n"] += 1
            raise RuntimeError("transient hudi read error")
        return real_load(path)

    monkeypatch.setattr(spark.read, "load", flaky)

    with pytest.raises(RuntimeError, match="alerts"):
        api._execute_sql_sync("SELECT * FROM alerts", "A")
    assert len(api._execute_sql_sync("SELECT * FROM alerts", "A")) == 1


def test_unreferenced_missing_tables_are_not_loaded(api, warehouse, monkeypatch, tmp_path):
    paths, data = warehouse
    spark = use_session(api, monkeypatch, data)
    api._execute_sql_sync("SELECT * FROM alerts", "A")
    (tmp_path / "does-not-exist").mkdir()
    data[paths["typologies"]] = [{api.TENANT_ID_COLUMN: "A", "id": 9}]

    api._execute_sql_sync("SELECT * FROM alerts", "A")

    assert paths["typologies"] not in spark.loads


def test_frames_stay_paired_with_their_session(api, warehouse, monkeypatch):
    _, data = warehouse
    first = FakeSpark(data)
    second = FakeSpark(data)

    frames_first = api._ensure_registered(first)
    snapshot = dict(frames_first)
    frames_second = api._ensure_registered(second)  # session replaced while a request holds frames_first

    assert frames_first is not frames_second
    assert frames_first == snapshot  # the earlier request's dict was not mutated or cleared
    assert all(df.session is first for df in frames_first.values())
    assert all(df.session is second for df in frames_second.values())

