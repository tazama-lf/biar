"""
views_orchestrator.py
---------------------
Runs all view builders conditionally based on Hudi table availability.
"""

from __future__ import annotations

import os
from typing import Optional

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

    # Upstream base tables each view depends on, keyed by the same
    # normalized table names FullETLOrchestrator._ETL_REGISTRY uses.
    # Used only to decide whether a view is worth *considering* when a
    # tables_hint is supplied to run()/run_all() — the per-view Hudi
    # readiness checks below still run unconditionally regardless.
    # Derived by reading each view builder's actual .load()/_safe_load() calls
    # (Table_ETLs/alert_navigator.py, transaction_detail_view.py, etc.), not
    # guessed from the view's name — several of these read more upstream
    # tables than their name suggests (e.g. alert_navigator also enriches
    # from typologies/network_map/rule; vw_transaction_detail also reads the
    # PACS gold tables for enrichment).
    _VIEW_DEPENDENCIES: dict[str, set[str]] = {
        "alert_navigator": {"alerts", "transactions", "typologies", "network_map", "rule"},
        "vw_transaction_detail": {"transactions", "pacs008", "pacs002"},
        "vw_transaction_history": {"transactions", "pacs008", "alerts", "cases", "tasks"},
        "network_navigator": {"transactions", "pacs008", "pacs002", "alerts", "cases", "tasks"},
        # alert_history reads vw_transaction_detail (not its own base tables),
        # so it's also relevant whenever anything vw_transaction_detail itself
        # depends on changes — transactions and the PACS gold tables — on top
        # of the alerts/cases/tasks it joins directly.
        "alert_history": {"alerts", "cases", "tasks", "transactions", "pacs008", "pacs002"},
        "conditions_timeline": {"condition", "transactions", "alerts", "cases", "tasks"},
    }

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

    def _wanted(self, view_name: str, tables_hint: Optional[set[str]]) -> bool:
        """Whether *view_name* should be considered this run.

        tables_hint=None means "consider everything" (a full rebuild).
        Otherwise, only views whose upstream tables intersect the hint
        are worth attempting.
        """
        if tables_hint is None:
            return True
        return bool(self._VIEW_DEPENDENCIES[view_name] & tables_hint)

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

    def run_all(self, tables_hint: Optional[set[str]] = None) -> int:
        """Create views whose upstream tables are available.

        tables_hint : optional set of normalized table names that changed
            since the last build (see FullETLOrchestrator._ETL_REGISTRY for
            the naming convention). When provided, a view is skipped unless
            one of its upstream tables appears in the hint — this is on top
            of, not instead of, the per-view Hudi-readiness checks below,
            which still run unconditionally. When omitted (the default),
            every view is considered, matching the original behaviour.

        Returns the number of view builders skipped specifically because
        tables_hint ruled them out (as opposed to skipped for missing
        upstream data) — exported by callers as a metric, since a silent
        hint-based skip would otherwise defeat the optimisation invisibly.
        """
        print("[ViewsOrchestrator] Creating views..."+ (f" (hint: {sorted(tables_hint)})" if tables_hint is not None else " (full rebuild)"))

        skipped_by_hint = 0

        # ---- Alert Navigator (requires silver/alerts) ----
        if not self._wanted("alert_navigator", tables_hint):
            print("[ViewsOrchestrator] Skipping alert_navigator (no relevant table changes)")
            skipped_by_hint += 1
        elif self._hudi_ready(f"{self.warehouse_root}/silver/alerts"):
            self._run_view(AlertNavigatorETL, "alert_navigator")
        else:
            print("[ViewsOrchestrator] Skipping alert_navigator (missing silver/alerts)")

        # ---- Transaction Detail + History (require bronze/transactions) ----
        if not (self._wanted("vw_transaction_detail", tables_hint) or self._wanted("vw_transaction_history", tables_hint)):
            print("[ViewsOrchestrator] Skipping transaction views (no relevant table changes)")
            skipped_by_hint += 2
        elif self._hudi_ready(f"{self.warehouse_root}/bronze/transactions"):
            self._run_view(TransactionDetailViewETL, "vw_transaction_detail")
            self._run_view(TransactionHistoryViewETL, "vw_transaction_history")
        else:
            print("[ViewsOrchestrator] Skipping transaction views (missing bronze/transactions)")

        # ---- Network Navigator (requires transactions + PACS enrichment + gold alerts/cases/tasks) ----
        if not self._wanted("network_navigator", tables_hint):
            print("[ViewsOrchestrator] Skipping network_navigator (no relevant table changes)")
            skipped_by_hint += 1
        elif (
            self._hudi_ready(f"{self.warehouse_root}/bronze/transactions")
            and self._hudi_ready(f"{self.warehouse_root}/gold/pacs008")
            and self._hudi_ready(f"{self.warehouse_root}/gold/pacs002")
            and self._hudi_ready(f"{self.warehouse_root}/gold/alerts")
            and self._hudi_ready(f"{self.warehouse_root}/gold/cases")
            and self._hudi_ready(f"{self.warehouse_root}/gold/tasks")
        ):
            self._run_view(NetworkNavigatorViewETL, "network_navigator")
        else:
            print(
                "[ViewsOrchestrator] Skipping network navigator views "
                "(missing transactions/pacs008/pacs002/alerts/cases/tasks)"
            )

        # ---- Alert History (requires gold alerts/cases/tasks + vw_transaction_detail) ----
        if not self._wanted("alert_history", tables_hint):
            print("[ViewsOrchestrator] Skipping alert_history (no relevant table changes)")
            skipped_by_hint += 1
        elif (
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

        # ---- Conditions Timeline (requires gold condition + gold transactions + gold alerts) ----
        if not self._wanted("conditions_timeline", tables_hint):
            print("[ViewsOrchestrator] Skipping conditions_timeline (no relevant table changes)")
            skipped_by_hint += 1
        elif (
            self._hudi_ready(f"{self.warehouse_root}/gold/condition")
            and self._hudi_ready(f"{self.warehouse_root}/gold/transactions")
            and self._hudi_ready(f"{self.warehouse_root}/gold/alerts")
        ):
            self._run_view(ConditionsTimelineViewETL, "conditions_timeline")
        else:
            print(
                "[ViewsOrchestrator] Skipping conditions_timeline "
                "(missing gold condition/transactions/alerts)"
            )

        print(f"[ViewsOrchestrator] View build finished (skipped_by_hint={skipped_by_hint})")
        return skipped_by_hint

    def run(self, tables_hint: Optional[set[str]] = None) -> int:
        """Alias for run_all() for consistency with other orchestrators."""
        return self.run_all(tables_hint=tables_hint)
