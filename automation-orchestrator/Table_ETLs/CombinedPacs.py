"""
combined_pacs.py
----------------
Orchestrates Bronze→Silver→Gold for pacs.008 / pacs.002 and triggers
the downstream Transactions ETL once BOTH have reached GOLD.
"""

from __future__ import annotations

import os
import time

from pyspark.sql import SparkSession

from .BaseETL import BaseETL
from .Pacs008ETL import Pacs008ETL
from .Pacs002ETL import Pacs002ETL
from .TransactionsETL import TransactionsETL


class CombinedPacsETL(BaseETL):
    """
    Combined PACs orchestrator.

    Runs the domain ETL for the configured PACS table (pacs008 or pacs002)
    and triggers Transactions ETL ONLY after BOTH tables have reached GOLD.
    """

    def __init__(
        self,
        spark: SparkSession,
        warehouse_root: str,
        table: str,
    ) -> None:
        super().__init__(spark, warehouse_root)
        if table not in {"pacs008", "pacs002"}:
            raise ValueError(f"CombinedPacsETL received unknown table: {table}")
        self.table = table
        self.state_dir = os.path.join(self.warehouse_root, ".pipeline_state")
        ttl_min = os.environ.get("PACS_MARKER_TTL_MINUTES", "60") or "60"
        self.marker_ttl_seconds = max(60, int(ttl_min) * 60)

    @property
    def bronze_path(self) -> str:
        return f"{self.warehouse_root}/bronze/transactions"

    @property
    def silver_path(self) -> str:
        return f"{self.warehouse_root}/silver/transactions"

    @property
    def gold_path(self) -> str:
        return f"{self.warehouse_root}/gold/transactions"

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _touch(path: str) -> None:
        """Create an empty marker file or update its mtime."""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a"):
            os.utime(path, None)

    @staticmethod
    def _is_recent_file(path: str, ttl_seconds: int) -> bool:
        """Return True if *path* exists and was modified within *ttl_seconds*."""
        if not os.path.exists(path):
            return False
        return (time.time() - os.path.getmtime(path)) < ttl_seconds

    @staticmethod
    def _parse_s3a_bucket(source_path: str) -> str | None:
        """Extract the bucket name from an s3a:// URI."""
        if source_path.startswith("s3a://"):
            return source_path.replace("s3a://", "").split("/")[0]
        return None

    # ------------------------------------------------------------------
    # Abstract interface (orchestrator does not write its own layers)
    # ------------------------------------------------------------------

    def bronze(self, source_path: str) -> str:
        raise NotImplementedError(
            "CombinedPacsETL is an orchestrator – it does not write its own bronze layer."
        )

    def silver(self) -> str:
        raise NotImplementedError(
            "CombinedPacsETL is an orchestrator – it does not write its own silver layer."
        )

    def gold(self) -> str:
        raise NotImplementedError(
            "CombinedPacsETL is an orchestrator – it does not write its own gold layer."
        )

    # ------------------------------------------------------------------
    # ORCHESTRATION
    # ------------------------------------------------------------------

    def run(self, source_path: str) -> str:
        """
        Run the combined PACs pipeline.

        1. Execute Bronze→Silver→Gold for *self.table*.
        2. Mark GOLD completion via local marker files.
        3. If BOTH pacs008 and pacs002 have recent markers, trigger
           Transactions ETL and clear the markers.
        """
        print(f"* Starting Combined PACs ETL for {self.table} → {source_path}")

        # 1. Run the domain-specific pipeline
        if self.table == "pacs008":
            Pacs008ETL(self.spark, self.warehouse_root).run(source_path)
        else:
            Pacs002ETL(self.spark, self.warehouse_root).run(source_path)

        # 2. Mark GOLD completion
        pacs008_done = os.path.join(self.state_dir, "pacs008_gold_done")
        pacs002_done = os.path.join(self.state_dir, "pacs002_gold_done")
        self._touch(pacs008_done if self.table == "pacs008" else pacs002_done)

        # 3. Guard against stale markers
        pacs008_recent = self._is_recent_file(pacs008_done, self.marker_ttl_seconds)
        pacs002_recent = self._is_recent_file(pacs002_done, self.marker_ttl_seconds)

        if os.path.exists(pacs008_done) and not pacs008_recent:
            try:
                os.remove(pacs008_done)
            except FileNotFoundError:
                pass
            pacs008_recent = False

        if os.path.exists(pacs002_done) and not pacs002_recent:
            try:
                os.remove(pacs002_done)
            except FileNotFoundError:
                pass
            pacs002_recent = False

        # 4. Trigger transactions only when BOTH are recent
        if pacs008_recent and pacs002_recent:
            print("* pacs008 + pacs002 GOLD complete → triggering etl_transactions")

            bucket = self._parse_s3a_bucket(source_path)
            if not bucket:
                raise ValueError(f"Unable to parse bucket from source_path: {source_path}")

            trigger_lock = os.path.join(self.state_dir, "transactions_trigger.lock")
            os.makedirs(self.state_dir, exist_ok=True)

            # prevent double-trigger when pacs008 & pacs002 finish near-simultaneously
            try:
                fd = os.open(trigger_lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
            except FileExistsError:
                print("[PACs] Transactions trigger already in progress; skipping")
                return self.gold_path

            transactions_ok = False
            try:
                transaction_source_path = f"s3a://{bucket}/transaction/"
                TransactionsETL(self.spark, self.warehouse_root).run(
                    transaction_source_path, mode="from_pacs"
                )
                transactions_ok = True
                print("* Combined PACs + Transactions pipeline completed")
                return self.gold_path
            finally:
                try:
                    os.remove(trigger_lock)
                except FileNotFoundError:
                    pass

                if transactions_ok:
                    # reset markers for next batch
                    for marker in (pacs008_done, pacs002_done):
                        try:
                            os.remove(marker)
                        except FileNotFoundError:
                            pass

        # 5. Report what's still missing
        missing = []
        if not pacs008_recent:
            missing.append("pacs008")
        if not pacs002_recent:
            missing.append("pacs002")
        print(
            "* Waiting for the other PACS table to reach GOLD before triggering transactions"
            + (f" (missing/recent: {missing})" if missing else "")
        )
        return self.gold_path