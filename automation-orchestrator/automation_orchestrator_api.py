"""
automation_orchestrator_api.py
-------------------------------
FastAPI service that receives NiFi trigger events and dispatches ETL jobs
via FullETLOrchestrator.  Views are built automatically once the job queue
drains (same behaviour as the original monolith-based API).
"""

from __future__ import annotations

import os
import threading
import traceback
from queue import Queue
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# New class-based imports — replaces the monolith's function imports:
#   automation_orchestrator_api        → FullETLOrchestrator.run()
#   run_all_views       → ViewsOrchestrator.run()
#   get_spark_session   → spark_utils.get_spark_session()
#   DEFAULT_WAREHOUSE_ROOT → derived via _env() below
# ---------------------------------------------------------------------------
from lakehouse_automation_pipeline import FullETLOrchestrator, _env
from spark_utils import get_spark_session
from Table_ETLs.views_orchestrator import ViewsOrchestrator


# ===================================================================
# SPARK SESSION FACTORY
# ===================================================================
# get_spark_session() now lives in spark_utils.py so one-off maintenance
# scripts (e.g. rebuild_metrics_tms.py) can reuse it without importing this
# FastAPI module.


# ===================================================================
# CONFIGURATION
# ===================================================================

DEFAULT_WAREHOUSE_ROOT = ("/opt/Tazama_Warehouse")

APP_BASE_DIR = _env("APP_BASE_DIR", os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = _env("OUT_DIR", os.path.join(APP_BASE_DIR, "out"))

NOTEBOOK_PATH = os.path.join(APP_BASE_DIR, "Tazama_Data_Lake_House.ipynb")
OUTPUT_NOTEBOOK = os.path.join(OUT_DIR, "last_run.ipynb")
OUTPUT_REQUEST = os.path.join(OUT_DIR, "last_request.json")

NUM_WORKERS = int(os.getenv("NUM_WORKERS", "1"))

# ===================================================================
# SPARK — one shared session for the lifetime of the process
# ===================================================================

try:
    GLOBAL_SPARK = get_spark_session()
    print("[INIT] Spark session initialized globally")
except Exception as e:
    print(f"[INIT ERROR] Spark init failed, will fallback per job: {e}")
    GLOBAL_SPARK = None

# ===================================================================
# FASTAPI APP
# ===================================================================

app = FastAPI()

# ===================================================================
# REQUEST MODEL
# ===================================================================

class TriggerRequest(BaseModel):
    raw_path:         Optional[str] = None
    db_name:          Optional[str] = None  
    bucket:           Optional[str] = ""
    table:            Optional[str] = ""
    object_key:       Optional[str] = ""
    execute_notebook: Optional[bool] = False


def _normalized_request(req: TriggerRequest) -> TriggerRequest:
    """Normalize request routing fields before the job enters the queue."""
    table = FullETLOrchestrator._resolve_table_name(
        table=req.table,
        raw_path=req.raw_path,
        object_key=req.object_key,
    )
    return req.copy(update={"table": table})

# ===================================================================
# SHARED STATE  (queue + view-build gate)
# ===================================================================

job_queue = Queue()

STATE_LOCK           = threading.Lock()
STATE_COND           = threading.Condition(STATE_LOCK)
VIEW_BUILD_IN_PROGRESS = False
COMPLETED_TABLES: set[str] = set()

# ===================================================================
# HEALTH ENDPOINT
# ===================================================================

@app.get("/health")
def health():
    with STATE_LOCK:
        return {
            "status":                "ok",
            "num_workers":           NUM_WORKERS,
            "view_build_in_progress": VIEW_BUILD_IN_PROGRESS,
            "completed_tables":      sorted(COMPLETED_TABLES),
        }

# ===================================================================
# VIEW BUILD  (triggered after queue drains)
# ===================================================================

# gold/metrics/tms has no NiFi-fed source file to trigger off of — it aggregates
# gold/transactions and gold/evaluation. Refresh it right after either of those
# tables completes in a batch, rather than on a fixed timer.
METRICS_TMS_DEPENDENCY_TABLES = {"transaction", "transactions", "evaluation"}


def maybe_run_views_after_full_pipeline() -> None:
    """
    Build views once the entire job queue has drained successfully, then
    refresh gold/metrics/tms if one of its dependency tables just completed.

    Logic is identical to the original API:
      1. Skip if a view build is already running.
      2. Skip if the queue still has unfinished tasks.
      3. Skip if no tables completed successfully in this batch.
      4. Acquire VIEW_BUILD_IN_PROGRESS, run ViewsOrchestrator, then release.
    """
    global VIEW_BUILD_IN_PROGRESS

    with STATE_COND:
        if VIEW_BUILD_IN_PROGRESS:
            return
        if job_queue.unfinished_tasks != 0:
            return
        if not COMPLETED_TABLES:
            return
        VIEW_BUILD_IN_PROGRESS = True
        completed_this_batch = set(COMPLETED_TABLES)

    try:
        spark = GLOBAL_SPARK if GLOBAL_SPARK else get_spark_session()
        # Replaced: run_all_views(spark, DEFAULT_WAREHOUSE_ROOT)
        ViewsOrchestrator(spark, DEFAULT_WAREHOUSE_ROOT).run()

        if completed_this_batch & METRICS_TMS_DEPENDENCY_TABLES:
            print("[METRICS-REFRESH] transactions/evaluation updated — refreshing gold/metrics/tms")
            FullETLOrchestrator(spark, DEFAULT_WAREHOUSE_ROOT).run(table="metrics_tms", bucket="")
    except Exception:
        print("[VIEWS ERROR] Failed to build views")
        traceback.print_exc()
    finally:
        with STATE_COND:
            VIEW_BUILD_IN_PROGRESS = False
            COMPLETED_TABLES.clear()
            STATE_COND.notify_all()

# ===================================================================
# JOB RUNNER
# ===================================================================

def run_job(req: TriggerRequest) -> dict:
    print(f"[JOB] Starting ETL for: {req.raw_path}")

    spark = GLOBAL_SPARK if GLOBAL_SPARK else get_spark_session()
    req = _normalized_request(req)

    result = FullETLOrchestrator(spark, DEFAULT_WAREHOUSE_ROOT).run(
        raw_path=req.raw_path,
        db_name=req.db_name,          # NEW
        bucket=req.bucket,
        table=req.table,
        object_key=req.object_key,
        trigger_views=False,
    )

    print(f"[JOB] Completed ETL for: {req.raw_path}")
    return {
        "status": "python_running",
        "result": result,
    }
    
# ===================================================================
# BACKGROUND WORKER THREAD
# ===================================================================

def worker(worker_id: int) -> None:
    """
    Long-running worker thread.  Picks jobs off job_queue one at a time,
    blocks while a view build is in progress, then triggers view building
    after each job completes (which will only actually run views when the
    queue is fully drained).
    """
    print(f"[WORKER-{worker_id}] Started")
    while True:
        req = job_queue.get()
        print(f"[WORKER-{worker_id}] Picked job: {req.raw_path}")

        # Block new ETL work while views are being built.
        with STATE_COND:
            while VIEW_BUILD_IN_PROGRESS:
                STATE_COND.wait()

        job_success = False
        try:
            print(f"[WORKER-{worker_id}] Processing: {req.raw_path}")
            job_result = run_job(req)
            print(f"[WORKER-{worker_id}] Done: {req.raw_path}")
            job_success = True
        except Exception as e:
            print(f"[WORKER-{worker_id} ERROR] {e}")
            traceback.print_exc()
        finally:
            job_queue.task_done()

            completed_table = None
            if job_success:
                completed_table = (
                    job_result.get("result", {}).get("table")
                    if isinstance(job_result, dict)
                    else None
                ) or req.table

            if job_success and completed_table:
                with STATE_LOCK:
                    COMPLETED_TABLES.add(completed_table)

            maybe_run_views_after_full_pipeline()

# ===================================================================
# START WORKER THREADS
# ===================================================================

for i in range(NUM_WORKERS):
    threading.Thread(target=worker, args=(i,), daemon=True).start()

# ===================================================================
# MAIN ENDPOINT
# ===================================================================

@app.post("/checksubmit")
def submit(
    req: TriggerRequest,
    x_api_key: str = Header(None, description="API Key for authentication"),
):
    normalized_req = _normalized_request(req)

    payload = {
        "raw_path":         normalized_req.raw_path,
        "bucket":           normalized_req.bucket,
        "table":            normalized_req.table,
        "object_key":       normalized_req.object_key,
        "execute_notebook": normalized_req.execute_notebook,
    }

    # ------------------------------------------------------------------
    # Always queue the ETL job (non-blocking)
    # ------------------------------------------------------------------
    try:
        job_queue.put(normalized_req)
        print(
            f"[QUEUE] Added job: {normalized_req.raw_path or normalized_req.object_key} "
            f"| table={normalized_req.table} | Queue size: {job_queue.qsize()}"
        )
        return {
            "status":   "queued",
            "message":  "ETL job added to queue",
            "raw_path": normalized_req.raw_path,
            "bucket":   normalized_req.bucket,
            "table":    normalized_req.table,
            "data":     payload,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
# ===================================================================
# ENTRY POINT
# ===================================================================

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(_env("ORCHESTRATOR_PORT", "7619")),
        loop="asyncio",
    )
