from __future__ import annotations

import os
from typing import Optional

from pyspark.sql import SparkSession

from Table_ETLs.AccountETL import AccountETL
from Table_ETLs.Account_HolderETL import AccountHolderETL
from Table_ETLs.ALertsETL import AlertsETL
from Table_ETLs.CasesETL import CasesETL
from Table_ETLs.CombinedPacs import CombinedPacsETL
from Table_ETLs.ConditionsETL import ConditionsETL
from Table_ETLs.Netwrok_MapETL import NetworkMapETL
from Table_ETLs.RulesETL import RulesETL
from Table_ETLs.TasksETL import TasksETL
from Table_ETLs.TypologiesETL import TypologiesETL
from Table_ETLs.CommentsETL import CommentsETL
from Table_ETLs.EntityETL import EntityETL
from Table_ETLs.EvaluationETL import EvaluationETL
from Table_ETLs.DynamicETL import DynamicETL

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
        result = orchestrator.run(table="alerts", bucket="marcel", object_key="...")
        
        # Run table ETL + build any eligible views afterwards
        result = orchestrator.run(table="alerts", bucket="marcel", object_key="...", trigger_views=True)
    """

    DEFAULT_WAREHOUSE_ROOT = "/opt/biar/test_warehouse"

    _ETL_REGISTRY: dict[str, type] = {
        "account": AccountETL,
        "account_holder": AccountHolderETL,
        "alerts": AlertsETL,
        "cases": CasesETL,
        "conditions": ConditionsETL,
        "network_map": NetworkMapETL,
        "rule": RulesETL,
        "rules": RulesETL,
        "tasks": TasksETL,
        "typology": TypologiesETL,
        "typologies": TypologiesETL,
        "comment": CommentsETL,
        "comments": CommentsETL,
        "entity": EntityETL,
        "entities": EntityETL,
        "evaluation": EvaluationETL,
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
            raw_path, table, bucket, object_key
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
        table: Optional[str],
        bucket: Optional[str],
        object_key: Optional[str],
    ) -> tuple[str, str, str, str, str]:
        """Resolve and default all input parameters."""
        if not all([raw_path, table, bucket, object_key]):
            bucket = bucket 
            table = table 
            object_key = object_key 
            raw_path = raw_path or f"s3a://{bucket}/{table}/{object_key}"

        source_path = raw_path
        return raw_path, table, bucket, object_key, source_path

    def _route_etl(self, table: str, source_path: str, db_name: Optional[str] = None) -> str:
        """Dispatch to the correct ETL class based on table name."""
        if table in ("pacs008", "pacs002"):
            CombinedPacsETL(self.spark, self.warehouse_root, table=table).run(source_path)
            return "All Done"

        if table in ("transaction", "transactions"):
            print(
                "Skipping standalone Transactions ETL: it is triggered only after "
                "pacs008 + pacs002 reach GOLD (via CombinedPacsETL)."
            )
            return "Skipped: transactions are derived from PACS gold"

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