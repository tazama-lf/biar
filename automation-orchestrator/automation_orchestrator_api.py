"""
automation_orchestrator_api.py
-------------------------------
FastAPI service that receives NiFi trigger events and dispatches ETL jobs
via FullETLOrchestrator.  Views are rebuilt on a fixed timer
(VIEW_REFRESH_INTERVAL_SECONDS) by an independent scheduler thread, rather
than being triggered by job_queue draining to zero — see
run_scheduled_view_refresh() for why (BIAR-04).
"""

from __future__ import annotations

import os
import queue
import signal
import threading
import time
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
from Table_ETLs.views_orchestrator import ViewsOrchestrator


# ===================================================================
# SPARK SESSION FACTORY  (ported verbatim from the monolith)
# ===================================================================

def get_spark_session():
    """
    Build and return a SparkSession configured for Tazama / Hudi / S3A.
    Identical configuration to the monolith's get_spark_session().
    """
    from pyspark.sql import SparkSession

    spark_home = _env("SPARK_HOME", "/opt/spark")
    os.environ["SPARK_HOME"] = spark_home

    spark_master = _env("SPARK_MASTER", "local[2]")
    spark_local_dir = _env("SPARK_LOCAL_DIR", "/tmp/spark")
    os.environ.setdefault("SPARK_LOCAL_DIRS", spark_local_dir)
 
    default_jars = [
        "/opt/jars/hudi-spark3.4-bundle_2.12-0.14.1.jar",
        "/opt/jars/hadoop-aws-3.3.4.jar",
        "/opt/jars/aws-java-sdk-bundle-1.12.262.jar",
    ]
    jars_env  = _env("SPARK_JARS", ",".join(default_jars))
    jar_files = [j.strip() for j in jars_env.split(",") if j.strip()]

    s3_endpoint = _env("S3A_ENDPOINT", "")
    s3_access_key = _env("S3A_ACCESS_KEY", "")
    s3_secret_key = _env("S3A_SECRET_KEY", "")

    spark = (
        SparkSession.builder
        .appName("Tazama_Hudi_ETL")
        .master(spark_master)
        .config("spark.jars", ",".join(jar_files))
        .config("spark.driver.extraClassPath", ":".join(jar_files))
        .config("spark.executor.extraClassPath", ":".join(jar_files))
        # S3A / Ozone
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.impl.disable.cache", "true")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        # Hudi
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .config("spark.hadoop.parquet.avro.write-old-list-structure", "false")
        # Memory & performance
        .config("spark.local.dir", _env("SPARK_LOCAL_DIR", "/tmp/spark"))
        .config("spark.driver.memory", _env("SPARK_DRIVER_MEMORY", "6g"))
        .config("spark.driver.memoryOverhead", _env("SPARK_DRIVER_MEMORY_OVERHEAD", "2g"))
        .config("spark.driver.maxResultSize", _env("SPARK_DRIVER_MAX_RESULT_SIZE", "4g"))
        .config("spark.executor.memory", _env("SPARK_EXECUTOR_MEMORY", "6g"))
        .config("spark.executor.memoryOverhead", _env("SPARK_EXECUTOR_MEMORY_OVERHEAD", "2g"))
        .config("spark.sql.shuffle.partitions", _env("SPARK_SQL_SHUFFLE_PARTITIONS", "48"))
        .config("spark.default.parallelism", _env("SPARK_DEFAULT_PARALLELISM", "48"))
        .config("spark.executor.cores", _env("SPARK_EXECUTOR_CORES", "2"))
        .config("spark.driver.cores", _env("SPARK_DRIVER_CORES", "2"))
        .config("spark.memory.fraction", _env("SPARK_MEMORY_FRACTION", "0.6"))
        .config("spark.memory.storageFraction", _env("SPARK_MEMORY_STORAGE_FRACTION", "0.3"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", _env("SPARK_SQL_ADVISORY_PARTITION_SIZE", "64mb"))
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark Version: {spark.version}")
    return spark


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

# How often the view-refresh scheduler ticks. Views used to only rebuild
# when job_queue drained to zero (see run_scheduled_view_refresh() below for
# why that trigger has been replaced) — this interval is now the only thing
# that gates freshness, independent of queue/worker state.
#
# NOTE ON NUM_WORKERS: raising it alone will not raise ETL throughput here,
# because every worker thread shares one GLOBAL_SPARK session (see
# get_spark_session() above), and SPARK_MASTER defaults to local[2] — i.e.
# only 2 cores regardless of how many Python threads submit jobs to it.
# Sizing NUM_WORKERS correctly requires benchmarking against SPARK_MASTER /
# SPARK_EXECUTOR_CORES together, against real load — that harness doesn't
# exist yet, so this default is left untouched rather than guessed at.
VIEW_REFRESH_INTERVAL_SECONDS = int(os.getenv("VIEW_REFRESH_INTERVAL_SECONDS", "300"))

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
# GRACEFUL SHUTDOWN
# ===================================================================

def _handle_sigterm(signum, frame) -> None:
    """
    Just sets the stop flag. Deliberately does no blocking work here — a
    signal handler runs in the main thread and stopping Spark can take a
    while, so the actual drain-and-stop sequence lives in
    shutdown_watcher(), a plain thread that's already waiting on SHUTDOWN.
    """
    print("[SHUTDOWN] SIGTERM received — draining workers before stopping Spark")
    SHUTDOWN.set()


signal.signal(signal.SIGTERM, _handle_sigterm)


def shutdown_watcher() -> None:
    """
    Waits for SHUTDOWN, then waits for every worker (and the scheduler
    thread, in case a view build is in flight) to actually finish before
    calling GLOBAL_SPARK.stop(). Calling stop() any earlier risks tearing
    down the Spark context while a worker or a view build is still writing
    to it — exactly the "half-torn-down context" this is meant to avoid.
    """
    SHUTDOWN.wait()
    print("[SHUTDOWN] Waiting for in-flight jobs and any running view build to finish...")
    for t in MANAGED_THREADS:
        t.join()
    print("[SHUTDOWN] All workers and the scheduler are quiescent")
    if GLOBAL_SPARK:
        try:
            GLOBAL_SPARK.stop()
            print("[SHUTDOWN] Spark session stopped")
        except Exception as e:
            print(f"[SHUTDOWN] Error stopping Spark session: {e}")

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

# Tables that finished ETL since the last view build. Read by the
# scheduler below to decide *which* views are worth rebuilding on a tick;
# no longer used to decide *whether* to rebuild — that's the timer's job.
TABLES_CHANGED_SINCE_LAST_BUILD: set[str] = set()

# True once the scheduler has run its first tick since process start.
# The first tick always does a full rebuild — see run_scheduled_view_refresh().
FIRST_VIEW_REFRESH_DONE = False

# Set on SIGTERM. Workers stop pulling new jobs once this is set (they still
# finish whatever job they're already running); the scheduler thread's wait
# loop also exits promptly instead of sleeping out the rest of the interval.
SHUTDOWN = threading.Event()

# Every worker thread and the scheduler thread, so shutdown_watcher() can
# join() all of them — i.e. wait for them to actually go quiescent — before
# calling GLOBAL_SPARK.stop(). Populated when the threads are started below.
MANAGED_THREADS: list[threading.Thread] = []

# Lightweight in-process stand-in for "view-refresh tick counter" / "view-
# refresh duration histogram" from the issue. There's no metrics library
# (prometheus_client etc.) anywhere in this codebase yet, so these are
# exposed via /health rather than a real scrape-able histogram — swap in a
# real metrics client here if/when one is introduced.
VIEW_REFRESH_METRICS = {
    "ticks_total": 0,
    "builds_run_total": 0,
    "skipped_builder_total": 0,
    "last_duration_seconds": None,
    "max_duration_seconds": 0.0,
    "total_duration_seconds": 0.0,
}

# ===================================================================
# HEALTH ENDPOINT
# ===================================================================

@app.get("/health")
def health():
    with STATE_LOCK:
        return {
            "status":                           "ok",
            "num_workers":                      NUM_WORKERS,
            "view_refresh_interval_seconds":     VIEW_REFRESH_INTERVAL_SECONDS,
            "view_build_in_progress":            VIEW_BUILD_IN_PROGRESS,
            "tables_changed_since_last_build":   sorted(TABLES_CHANGED_SINCE_LAST_BUILD),
            "view_refresh_metrics":              dict(VIEW_REFRESH_METRICS),
        }

# ===================================================================
# VIEW BUILD  (triggered on a timer, independent of job_queue state)
# ===================================================================

# gold/metrics/tms has no NiFi-fed source file to trigger off of — it aggregates
# gold/transactions and gold/evaluation. Refresh it right after either of those
# tables completes in a batch, rather than on a fixed timer.
METRICS_TMS_DEPENDENCY_TABLES = {"transaction", "transactions", "evaluation"}


def run_scheduled_view_refresh() -> None:
    """
    Rebuild views on a fixed interval, independent of job_queue state, then
    refresh gold/metrics/tms if one of its dependency tables was part of
    this tick (see METRICS_TMS_DEPENDENCY_TABLES above).

    This replaces the old "rebuild once job_queue drains to zero" trigger:
    under sustained NiFi load the queue rarely if ever hit zero, so views
    almost never rebuilt. A timer has no such dependency.

    The change-tracking set (TABLES_CHANGED_SINCE_LAST_BUILD) is in-memory
    and starts empty on every restart, so the first tick after startup
    always does a full, unconditional rebuild (tables_hint=None) to catch
    whatever changed during the outage — including gold/metrics/tms.
    Every tick after that only rebuilds views touched by tables that
    actually changed.
    """
    global VIEW_BUILD_IN_PROGRESS, FIRST_VIEW_REFRESH_DONE

    with STATE_COND:
        VIEW_REFRESH_METRICS["ticks_total"] += 1

        if VIEW_BUILD_IN_PROGRESS:
            print("[SCHEDULER] Skipping tick — a view build is already in progress")
            return

        is_first_tick = not FIRST_VIEW_REFRESH_DONE
        if not is_first_tick and not TABLES_CHANGED_SINCE_LAST_BUILD:
            print("[SCHEDULER] Skipping tick — no tables changed since last build")
            return

        tables_hint = None if is_first_tick else set(TABLES_CHANGED_SINCE_LAST_BUILD)
        VIEW_BUILD_IN_PROGRESS = True

    started_at = time.monotonic()
    try:
        spark = GLOBAL_SPARK if GLOBAL_SPARK else get_spark_session()
        if tables_hint is None:
            print("[SCHEDULER] First tick since startup — full rebuild to catch outage-window changes")
        else:
            print(f"[SCHEDULER] Rebuilding views for changed tables: {sorted(tables_hint)}")
        skipped_by_hint = ViewsOrchestrator(spark, DEFAULT_WAREHOUSE_ROOT).run(tables_hint=tables_hint)

        if tables_hint is None or (tables_hint & METRICS_TMS_DEPENDENCY_TABLES):
            print("[METRICS-REFRESH] transactions/evaluation updated — refreshing gold/metrics/tms")
            FullETLOrchestrator(spark, DEFAULT_WAREHOUSE_ROOT).run(table="metrics_tms", bucket="")
    except Exception:
        print("[SCHEDULER ERROR] Failed to build views")
        traceback.print_exc()
        skipped_by_hint = 0
    finally:
        duration = time.monotonic() - started_at
        with STATE_COND:
            VIEW_BUILD_IN_PROGRESS = False
            FIRST_VIEW_REFRESH_DONE = True
            VIEW_REFRESH_METRICS["builds_run_total"] += 1
            VIEW_REFRESH_METRICS["skipped_builder_total"] += skipped_by_hint or 0
            VIEW_REFRESH_METRICS["last_duration_seconds"] = duration
            VIEW_REFRESH_METRICS["total_duration_seconds"] += duration
            VIEW_REFRESH_METRICS["max_duration_seconds"] = max(VIEW_REFRESH_METRICS["max_duration_seconds"], duration)
            # Snapshot-and-diff rather than .clear(): a job that completed
            # *during* this build already added its table to the set, and
            # a plain .clear() would silently drop it from the next tick.
            if tables_hint is None:
                TABLES_CHANGED_SINCE_LAST_BUILD.clear()
            else:
                TABLES_CHANGED_SINCE_LAST_BUILD.difference_update(tables_hint)
            STATE_COND.notify_all()


def view_refresh_scheduler() -> None:
    """Background thread: ticks run_scheduled_view_refresh() every VIEW_REFRESH_INTERVAL_SECONDS."""
    print(f"[SCHEDULER] View refresh scheduler started (interval={VIEW_REFRESH_INTERVAL_SECONDS}s)")
    while not SHUTDOWN.is_set():
        if SHUTDOWN.wait(VIEW_REFRESH_INTERVAL_SECONDS):
            break
        run_scheduled_view_refresh()
    print("[SCHEDULER] View refresh scheduler stopped")

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
    Long-running worker thread. Picks jobs off job_queue one at a time,
    blocks while a view build is in progress (to avoid ETL writes racing a
    view build's reads), then records which table it touched so the
    view-refresh scheduler knows what to rebuild on its next tick.

    Also retains an immediate, opportunistic view-refresh trigger on queue
    drain (on top of, not instead of, the periodic timer): if the queue
    just went idle, an idle window shouldn't have to wait out the rest of
    VIEW_REFRESH_INTERVAL_SECONDS to get fresh views. run_scheduled_view_
    refresh() is safe to call opportunistically like this — it's a no-op
    if nothing changed or a build is already running.

    Stops pulling new jobs once SHUTDOWN is set (graceful drain), but a
    job already picked up always runs to completion first. job_queue.get()
    uses a short timeout instead of blocking forever so a worker sitting
    idle on an empty queue still notices SHUTDOWN promptly.
    """
    print(f"[WORKER-{worker_id}] Started")
    while not SHUTDOWN.is_set():
        try:
            req = job_queue.get(timeout=1)
        except queue.Empty:
            continue
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
                    TABLES_CHANGED_SINCE_LAST_BUILD.add(completed_table)

            if job_queue.unfinished_tasks == 0:
                run_scheduled_view_refresh()
    print(f"[WORKER-{worker_id}] Stopped (shutdown)")

# ===================================================================
# START WORKER THREADS
# ===================================================================

for i in range(NUM_WORKERS):
    t = threading.Thread(target=worker, args=(i,), daemon=True)
    MANAGED_THREADS.append(t)
    t.start()

scheduler_thread = threading.Thread(target=view_refresh_scheduler, daemon=True)
MANAGED_THREADS.append(scheduler_thread)
scheduler_thread.start()

threading.Thread(target=shutdown_watcher, daemon=True).start()

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
