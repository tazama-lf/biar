"""
rebuild_metrics_tms.py
----------------------
One-time (and safely repeatable) deployment helper for the tenant-isolation
fix in #185 / PR #186.

Why this exists
---------------
``gold/metrics/tms`` is a derived aggregate — it is fully reproducible from
``gold/transactions`` + ``gold/evaluation`` — and it is written with Hudi
``upsert``/``append``. PR #186 adds ``tenant_id`` to the Hudi record key
(6 fields → 7 fields). Hudi materialises ``_hoodie_record_key`` at write time
and does **not** rewrite it when ``hoodie.datasource.write.recordkey.field``
changes, so rows written before this change keep the old key and a NULL
``tenant_id``. Those rows are correctly invisible to the tenant-scoped readers
(``NULL == 'X'`` is falsy in Spark) — but they are also permanently orphaned,
so every tenant's historical TMS metrics would silently disappear.

Dropping the table and regenerating it from its only inputs is therefore the
correct migration (see issue #185's own "Proposed fix": *"Rebuild the table —
it is a derived aggregate and can be regenerated from ``gold/transactions`` +
``gold/evaluation`` at any time."*).

The rebuild is failure-safe: the replacement is built at
``gold/metrics/tms__rebuild_staging`` and only promoted onto the live path
after the source reads, aggregation and write all succeed. During promotion the
live table is moved aside to ``gold/metrics/tms__rebuild_backup`` rather than
deleted, so a failed promotion (S3A renames are copy-then-delete and can fail
partway) triggers a rollback that restores the live table. Rollback is
attempted, not guaranteed: if the restore itself fails, the backup is left in
place and the error names its path, so recovery is manual but the data is never
lost.

Usage
-----
From the automation-orchestrator directory / container:

    python rebuild_metrics_tms.py [--warehouse-root /opt/Tazama_Warehouse]

The ``--warehouse-root`` value must match the warehouse root the running
orchestrator service uses (``DEFAULT_WAREHOUSE_ROOT`` in
``automation_orchestrator_api.py``).
"""

from __future__ import annotations

import argparse

from lakehouse_automation_pipeline import FullETLOrchestrator
from spark_utils import get_spark_session


def main() -> None:
    """Drop gold/metrics/tms and regenerate its full history."""
    parser = argparse.ArgumentParser(
        description="Drop gold/metrics/tms and rebuild it from gold/transactions + gold/evaluation.",
    )
    parser.add_argument(
        "--warehouse-root",
        default="/opt/Tazama_Warehouse",
        help="Warehouse root containing gold/ (default: %(default)s)",
    )
    args = parser.parse_args()

    print(f"[REBUILD] Warehouse root: {args.warehouse_root}")
    spark = get_spark_session()

    FullETLOrchestrator(spark, args.warehouse_root).run(
        table="metrics_tms",
        bucket="",
        rebuild=True,
    )
    print("[REBUILD] gold/metrics/tms rebuilt from gold/transactions + gold/evaluation")


if __name__ == "__main__":
    main()
