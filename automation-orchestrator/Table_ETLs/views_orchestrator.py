"""
views_orchestrator.py
---------------------
Runs all view builders conditionally based on Hudi table availability.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from Table_ETLs.alert_history_view import AlertHistoryViewETL
from Table_ETLs.alert_navigator import AlertNavigatorETL
from Table_ETLs.network_navigator_view import NetworkNavigatorViewETL
from Table_ETLs.transaction_detail_view import TransactionDetailViewETL
from Table_ETLs.transaction_history_view import TransactionHistoryViewETL
from Table_ETLs.ConditionsTimelineView import ConditionsTimelineViewETL


class ViewsOrchestrator:
    """
    Orchestrates the creation of all Hudi-backed views.

    Each view is built only when its upstream dependencies are present.
    """

    def __init__(
        self,
        spark: SparkSession,
        warehouse_root: str,
    ) -> None:
        self.spark = spark
        self.warehouse_root = warehouse_root
        self.views_root = f"{self.warehouse_root}/views"

    # ------------------------------------------------------------------
    # INTERNAL HELPERS
    # ------------------------------------------------------------------

    @staticmethod
    def _hudi_ready(path: str) -> bool:
        """Lightweight check: does *path* look like a live Hudi table?"""
        return os.path.isdir(path) and os.path.exists(
            os.path.join(path, ".hoodie", "hoodie.properties")
        )

    def _run_view(self, builder, name: str) -> None:
        """Instantiate and run a view builder, catching and logging errors."""
        try:
            builder(self.spark, self.warehouse_root).run()
            print(f"[ViewsOrchestrator] {name} completed")
        except Exception as e:
            print(f"[ViewsOrchestrator] Skipping {name} due to error: {e}")

    # ------------------------------------------------------------------
    # ORCHESTRATION
    # ------------------------------------------------------------------

    def run_all(self) -> None:
        """Create all views whose upstream tables are available."""
        print("[ViewsOrchestrator] Creating all views...")

        # ---- Alert Navigator (requires silver/alerts) ----
        if self._hudi_ready(f"{self.warehouse_root}/silver/alerts"):
            self._run_view(AlertNavigatorETL, "alert_navigator")
        else:
            print("[ViewsOrchestrator] Skipping alert_navigator (missing silver/alerts)")

        # ---- Transaction Detail + History (require bronze/transactions) ----
        if self._hudi_ready(f"{self.warehouse_root}/bronze/transactions"):
            self._run_view(TransactionDetailViewETL, "vw_transaction_detail")
            self._run_view(TransactionHistoryViewETL, "vw_transaction_history")
        else:
            print("[ViewsOrchestrator] Skipping transaction views (missing bronze/transactions)")

        # ---- Network Navigator (requires bronze/transactions + gold alerts/cases/tasks) ----
        if (
            self._hudi_ready(f"{self.warehouse_root}/bronze/transactions")
            and self._hudi_ready(f"{self.warehouse_root}/gold/alerts")
            and self._hudi_ready(f"{self.warehouse_root}/gold/cases")
            and self._hudi_ready(f"{self.warehouse_root}/gold/tasks")
        ):
            self._run_view(NetworkNavigatorViewETL, "network_navigator")
        else:
            print(
                "[ViewsOrchestrator] Skipping network navigator views "
                "(missing transactions/alerts/cases/tasks)"
            )

        # ---- Alert History (requires gold alerts/cases/tasks + vw_transaction_detail) ----
        if (
            self._hudi_ready(f"{self.warehouse_root}/gold/alerts")
            and self._hudi_ready(f"{self.warehouse_root}/gold/cases")
            and self._hudi_ready(f"{self.warehouse_root}/gold/tasks")
            and self._hudi_ready(f"{self.views_root}/vw_transaction_detail")
        ):
            self._run_view(AlertHistoryViewETL, "alert_history")
        else:
            print(
                "[ViewsOrchestrator] Skipping alert_history "
                "(missing gold alerts/cases/tasks or vw_transaction_detail)"
            )

        # ---- Conditions Timeline (requires gold conditions + gold transactions + gold alerts) ----
        if (
            self._hudi_ready(f"{self.warehouse_root}/gold/conditions")
            and self._hudi_ready(f"{self.warehouse_root}/gold/transactions")
            and self._hudi_ready(f"{self.warehouse_root}/gold/alerts")
        ):
            self._run_view(ConditionsTimelineViewETL, "conditions_timeline")
        else:
            print(
                "[ViewsOrchestrator] Skipping conditions_timeline "
                "(missing gold conditions/transactions/alerts)"
            )

        print("[ViewsOrchestrator] View build finished")

    def run(self) -> None:
        """Alias for run_all() for consistency with other orchestrators."""
        self.run_all()