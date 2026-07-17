"""
Regression tests for the efrup_subruleref extraction in
Table_ETLs.ALertsETL.AlertsETL._flatten_silver().

Covers the GitHub issue scenarios for the RLIKE fix at ALertsETL.py:225
(AND r.id RLIKE '^EFRuP@[0-9]+\\.[0-9]+\\.[0-9]+$'), which replaced an
exact-version match that only recognised EFRuP@1.0.0 and missed
EFRuP@2.0.0 (and any other version).
"""

from __future__ import annotations

import json

import pytest
from pyspark.sql import functions as F

from Table_ETLs.ALertsETL import AlertsETL


TRANSACTION_JSON = json.dumps(
    {
        "TxTp": "pacs.008.001.10",
        "TenantId": "TAZAMA",
        "FIToFIPmtSts": {
            "GrpHdr": {"MsgId": "MSG-1", "CreDtTm": "2026-01-01T00:00:00Z"},
            "TxInfAndSts": {
                "TxSts": "ACCC",
                "AccptncDtTm": "2026-01-01T00:00:01Z",
                "OrgnlInstrId": "INSTR-1",
                "OrgnlEndToEndId": "E2E-1",
                "InstgAgt": {"FinInstnId": {"ClrSysMmbId": {"MmbId": "BANK-A"}}},
                "InstdAgt": {"FinInstnId": {"ClrSysMmbId": {"MmbId": "BANK-B"}}},
                "ChrgsInf": [
                    {
                        "Agt": {"FinInstnId": {"ClrSysMmbId": {"MmbId": "BANK-A"}}},
                        "Amt": {"Amt": 1.5, "Ccy": "USD"},
                    }
                ],
            },
        },
    }
)

NETWORK_JSON = json.dumps(
    {
        "cfg": "1.0.0",
        "active": True,
        "tenantId": "TAZAMA",
        "messages": [{"id": "MSG-CFG-1"}],
    }
)


def _rule(rule_id: str, sub_ref, weight: int = 10) -> dict:
    """Build a ruleResults entry. sub_ref may be a string, '', or None."""
    return {"id": rule_id, "wght": weight, "subRuleRef": sub_ref}


def _typology(rules: list, typ_id: str = "TYP-1", result: int = 80) -> dict:
    return {
        "id": typ_id,
        "result": result,
        "review": False,
        "workflow": {
            "flowProcessor": "P1",
            "alertThreshold": 50,
            "interdictionThreshold": 90,
        },
        "ruleResults": rules,
    }


def _alert_data_json(typology_results: list, alert_id: int) -> str:
    return json.dumps(
        {
            "timestamp": "2026-01-01T00:00:00Z",
            "status": "COMPLETED",
            "evaluationID": f"EVAL-{alert_id}",
            "metaData": {"prcgTmDP": 5, "prcgTmED": 7},
            "tadpResult": {
                "id": "TADP-1",
                "cfg": "1.0.0",
                "prcgTm": 12,
                "typologyResult": typology_results,
            },
        }
    )


def _build_bronze_df(spark, alert_id: int, typology_results: list):
    """Build a one-row bronze-shaped DataFrame ready for _flatten_silver()."""
    row = {
        # Hudi metadata columns normally added by spark.read.format("hudi").load()
        "_hoodie_commit_time": "20260101000000",
        "_hoodie_commit_seqno": "seq-1",
        "_hoodie_record_key": str(alert_id),
        "_hoodie_partition_path": "",
        "_hoodie_file_name": "file1.parquet",
        "alert_id": alert_id,
        "tenant_id": "TAZAMA",
        "priority": "HIGH",
        "priority_score": 0.9,
        "alert_type": "FRAUD",
        "prediction_outcome": "TRUE_POSITIVE",
        "source": "ENGINE",
        "txtp": "pacs.008.001.10",
        "message": "test alert",
        "alert_data": _alert_data_json(typology_results, alert_id),
        "transaction": TRANSACTION_JSON,
        "network_map": NETWORK_JSON,
        "confidence_per": 90,
        "case_id": alert_id,
        "created_at": "2026-01-01T00:00:00Z",
    }
    df = spark.createDataFrame([row])
    return df.withColumn("created_at_ts", F.current_timestamp()).withColumn(
        "source_file_path", F.lit("test/path")
    )


def _parsed(etl: AlertsETL, bronze_df):
    """Mirror the JSON-parsing step AlertsETL.silver() performs before flattening."""
    alert_schema = etl.infer_json_schema(bronze_df, "alert_data")
    tx_schema = etl.infer_json_schema(bronze_df, "transaction")
    net_schema = etl.infer_json_schema(bronze_df, "network_map")
    return (
        bronze_df.withColumn("alert_data_obj", F.from_json("alert_data", alert_schema))
        .withColumn("transaction_obj", F.from_json("transaction", tx_schema))
        .withColumn("network_map_obj", F.from_json("network_map", net_schema))
    )


@pytest.fixture
def etl(spark) -> AlertsETL:
    return AlertsETL(spark, warehouse_root="/tmp/warehouse")


def _efrup_value(etl: AlertsETL, spark, alert_id: int, typology_results: list):
    bronze = _build_bronze_df(spark, alert_id, typology_results)
    parsed = _parsed(etl, bronze)
    return etl._flatten_silver(parsed).collect()[0]["efrup_subruleref"]


class TestEfrupSubruleref:
    def test_efrup_v1_regression(self, etl, spark):
        """EFRuP@1.0.0 must keep resolving to its subRuleRef (regression case)."""
        typ = [_typology([_rule("EFRuP@1.0.0", "Block")])]
        assert _efrup_value(etl, spark, 1, typ) == "Block"

    def test_efrup_v2_primary_fix_target(self, etl, spark):
        """EFRuP@2.0.0 was NULL before the fix; the version-agnostic RLIKE must match it."""
        typ = [_typology([_rule("EFRuP@2.0.0", "Block")])]
        assert _efrup_value(etl, spark, 2, typ) == "Block"

    def test_no_efrup_rule(self, etl, spark):
        typ = [_typology([_rule("SomeOtherRule@1.0.0", "Ignore")])]
        assert _efrup_value(etl, spark, 3, typ) is None

    def test_multiple_typologies_efrup_on_second(self, etl, spark):
        """First non-empty match wins across typologies (element_at(flatten(...), 1))."""
        typ = [
            _typology([_rule("SomeOtherRule@1.0.0", "Ignore")], typ_id="TYP-1"),
            _typology([_rule("EFRuP@1.0.0", "Override")], typ_id="TYP-2"),
        ]
        assert _efrup_value(etl, spark, 4, typ) == "Override"

    @pytest.mark.parametrize("sub_ref", ["", None])
    def test_efrup_empty_or_null_subruleref_passthrough(self, etl, spark, sub_ref):
        """Empty-string/null subRuleRef passes through as-is; no extra filtering added."""
        typ = [_typology([_rule("EFRuP@1.0.0", sub_ref)])]
        assert _efrup_value(etl, spark, 5, typ) == sub_ref

    def test_prefix_collision_rejected_by_anchored_regex(self, etl, spark):
        """
        The deployed RLIKE ('^EFRuP@[0-9]+\\.[0-9]+\\.[0-9]+$') is fully anchored,
        so a rule id that merely starts with EFRuP@<semver> but has trailing
        text must NOT match.
        """
        typ = [_typology([_rule("EFRuP@1.0.0-debug", "Block")])]
        assert _efrup_value(etl, spark, 6, typ) is None
