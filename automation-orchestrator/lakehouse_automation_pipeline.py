from __future__ import annotations

import os
from pathlib import PurePosixPath
from typing import Optional

from pyspark.sql import SparkSession

from Table_ETLs.AccountETL import AccountETL
from Table_ETLs.Account_HolderETL import AccountHolderETL
from Table_ETLs.ALertsETL import AlertsETL
from Table_ETLs.CasesETL import CasesETL
from Table_ETLs.ConditionsETL import ConditionsETL
from Table_ETLs.Netwrok_MapETL import NetworkMapETL
from Table_ETLs.Pacs002ETL import Pacs002ETL
from Table_ETLs.Pacs008ETL import Pacs008ETL
from Table_ETLs.RulesETL import RulesETL
from Table_ETLs.TasksETL import TasksETL
from Table_ETLs.TypologiesETL import TypologiesETL
from Table_ETLs.TransactionsETL import TransactionsETL
from Table_ETLs.CommentsETL import CommentsETL
from Table_ETLs.EntityETL import EntityETL
from Table_ETLs.EvaluationETL import EvaluationETL
from Table_ETLs.cms_usernames import CmsUsernamesETL
from Table_ETLs.DynamicETL import DynamicETL
from Table_ETLs.CasePriorityThresholdsETL import CasePriorityThresholdsETL
from Table_ETLs.SlaEscalationRecordsETL import SlaEscalationRecordsETL
from Table_ETLs.SlaEscalationThresholdsETL import SlaEscalationThresholdsETL
from Table_ETLs.SlaPoliciesETL import SlaPoliciesETL
from Table_ETLs.InvestigationGroupsETL import InvestigationGroupsETL

# Views orchestrator
from Table_ETLs.views_orchestrator import ViewsOrchestrator


def _env(name: str, default: str = "") -> str:
    """Read an environment variable, stripping whitespace; return *default* if unset."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip()


class FullETLOrchestrator:
    """
    Routes raw JSON files to the correct domain ETL pipeline.

    Usage:
        orchestrator = FullETLOrchestrator(spark, warehouse_root="/data/warehouse")
        
        # Run table ETL only
        result = orchestrator.run(table="alerts", bucket="tazama", object_key="...")
        
        # Run table ETL + build any eligible views afterwards
        result = orchestrator.run(table="alerts", bucket="tazama", object_key="...", trigger_views=True)
    """

    DEFAULT_WAREHOUSE_ROOT = "/opt/biar/test_warehouse"

    _ETL_REGISTRY: dict[str, type] = {
        "account": AccountETL,
        "account_holder": AccountHolderETL,
        "alerts": AlertsETL,
        "cases": CasesETL,
        "conditions": ConditionsETL,
        "condition": ConditionsETL,
        "network_map": NetworkMapETL,
        "pacs002": Pacs002ETL,
        "pacs008": Pacs008ETL,
        "rule": RulesETL,
        "rules": RulesETL,
        "tasks": TasksETL,
        "typology": TypologiesETL,
        "typologies": TypologiesETL,
        "transaction": TransactionsETL,
        "transactions": TransactionsETL,
        "comment": CommentsETL,
        "comments": CommentsETL,
        "entity": EntityETL,
        "entities": EntityETL,
        "evaluation": EvaluationETL,
        "cms_usernames": CmsUsernamesETL,
        "case_priority_thresholds": CasePriorityThresholdsETL,
        "case_priority_threshold": CasePriorityThresholdsETL,
        "sla_escalation_records": SlaEscalationRecordsETL,
        "sla_escalation_record": SlaEscalationRecordsETL,
        "sla_escalation_thresholds": SlaEscalationThresholdsETL,
        "sla_escalation_threshold": SlaEscalationThresholdsETL,
        "sla_policies": SlaPoliciesETL,
        "sla_policy": SlaPoliciesETL,
        "investigation_groups": InvestigationGroupsETL,
        "investigation_group": InvestigationGroupsETL,
    }

    def __init__(
        self,
        spark: SparkSession,
        warehouse_root: Optional[str] = None,
    ) -> None:
        self.spark = spark
        self.warehouse_root = warehouse_root or self.DEFAULT_WAREHOUSE_ROOT

    # ------------------------------------------------------------------
    # PUBLIC API
    # ------------------------------------------------------------------

    def run(
        self,
        raw_path: Optional[str] = None,
        db_name: Optional[str] = None,
        table: Optional[str] = None,
        bucket: Optional[str] = None,
        object_key: Optional[str] = None,
        trigger_views: bool = False,
    ) -> dict:
        """
        Execute the full ETL pipeline for the given table.

        Parameters
        ----------
        trigger_views : bool, default False
            If True, run the ViewsOrchestrator after the table ETL completes
            to build any views whose upstream dependencies are now available.
        """
        raw_path, table, bucket, object_key, source_path = self._resolve_params(
            raw_path, db_name, table, bucket, object_key
        )

        print("* Starting Full Tazama Hudi ETL Pipeline...")
        print(f"Bucket: {bucket}, Table: {table}, Object Key: {object_key}")
        print(f"Source Path: {source_path}")

        # 1. Run domain table ETL
        etl_result = self._route_etl(table, source_path, db_name=db_name)
        # 2. Optionally build views
        views_result = None
        if trigger_views:
            print("* Checking for buildable views...")
            views_result = self._run_views()

        print("FULL PIPELINE COMPLETED SUCCESSFULLY!")
        return {
            "table": table,
            "raw_path": raw_path,
            "bucket": bucket,
            "object_key": object_key,
            "source_path": source_path,
            "etl_result": etl_result,
            "views_result": views_result,
        }

    def run_views_only(self) -> None:
        """Convenience method to run only the views orchestrator."""
        print("* Running views orchestrator only...")
        self._run_views()

    # ------------------------------------------------------------------
    # INTERNAL HELPERS
    # ------------------------------------------------------------------

    def _resolve_params(
        self,
        raw_path: Optional[str],
        db_name: Optional[str],
        table: Optional[str],
        bucket: Optional[str],
        object_key: Optional[str],
    ) -> tuple[str, str, str, str, str]:
        """Resolve and default all input parameters."""
        table = self._resolve_table_name(table=table, raw_path=raw_path, object_key=object_key)

        if not raw_path:
            object_key_clean = (object_key or "").strip().lstrip("/")
            if "/" in object_key_clean:
                raw_path = f"s3a://{bucket}/{object_key_clean}"
            else:
                key_parts = []
                if db_name:
                    key_parts.append(db_name.strip().strip("/"))
                if table:
                    key_parts.append(table)
                if object_key_clean:
                    key_parts.append(object_key_clean)
                raw_path = f"s3a://{bucket}/{'/'.join(key_parts)}"

        source_path = raw_path
        return raw_path, table, bucket, object_key, source_path

    @classmethod
    def _normalize_table_name(cls, table: Optional[str]) -> str:
        """Normalize incoming table names from NiFi/API payloads."""
        normalized = (table or "").strip().lower().replace("-", "_")
        normalized = normalized.strip("/")
        if "/" in normalized:
            normalized = normalized.split("/")[-1]
        return normalized

    @classmethod
    def _resolve_table_name(
        cls,
        table: Optional[str],
        raw_path: Optional[str],
        object_key: Optional[str],
    ) -> str:
        """Resolve a table name from explicit table first, then object/raw paths."""
        table_name = cls._normalize_table_name(table)
        path_table = cls._resolve_table_name_from_paths(raw_path=raw_path, object_key=object_key)

        if table_name:
            return table_name

        return path_table

    @classmethod
    def _resolve_table_name_from_paths(
        cls,
        raw_path: Optional[str],
        object_key: Optional[str],
    ) -> str:
        """Resolve a registered table name from object/raw path parts."""

        for candidate in (object_key, raw_path):
            if not candidate:
                continue
            parts = [
                p.lower().replace("-", "_")
                for p in PurePosixPath(str(candidate).replace("s3a://", "")).parts
                if p and p not in {"/", "."}
            ]
            for part in reversed(parts):
                normalized = cls._normalize_table_name(part)
                if normalized in cls._ETL_REGISTRY:
                    return normalized

        return ""

    def _route_etl(self, table: str, source_path: str, db_name: Optional[str] = None) -> str:
        """Dispatch to the correct ETL class based on table name."""
        table = self._normalize_table_name(table)

        if table in self._ETL_REGISTRY:
            self._ETL_REGISTRY[table](self.spark, self.warehouse_root).run(source_path)
            return "All Done"

        # FALLBACK — unknown table → DynamicETL with db_name
        print(f"[FullETLOrchestrator] '{table}' not in ETL registry. Falling back to DynamicETL.")
        DynamicETL(
            self.spark,
            self.warehouse_root,
            db_name=db_name,
            table=table,
        ).run(source_path)
        
        return "All Done (DynamicETL fallback)"

    def _run_views(self) -> str:
        """Run the views orchestrator to build all available views."""
        try:
            ViewsOrchestrator(self.spark, self.warehouse_root).run()
            return "Views build triggered"
        except Exception as e:
            print(f"[FullETLOrchestrator] Views orchestrator failed: {e}")
            return f"Views failed: {e}"
