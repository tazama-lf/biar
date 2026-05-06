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
    raw_path:         str
    bucket:           Optional[str] = ""
    table:            Optional[str] = ""
    object_key:       Optional[str] = ""
    execute_notebook: Optional[bool] = False

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

def maybe_run_views_after_full_pipeline() -> None:
    """
    Build views once the entire job queue has drained successfully.

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

    try:
        spark = GLOBAL_SPARK if GLOBAL_SPARK else get_spark_session()
        # Replaced: run_all_views(spark, DEFAULT_WAREHOUSE_ROOT)
        ViewsOrchestrator(spark, DEFAULT_WAREHOUSE_ROOT).run()
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
    """
    Execute a single ETL job via FullETLOrchestrator.

    Returns the result dict produced by FullETLOrchestrator.run(), whose
    shape is: {table, raw_path, bucket, object_key, source_path, result, views_result}.
    The "result" key carries the status string ("All Done", "Skipped: …") —
    matching what the original API read from automation_orchestrator_api()'s return value.
    """
    print(f"[JOB] Starting ETL for: {req.raw_path}")

    spark = GLOBAL_SPARK if GLOBAL_SPARK else get_spark_session()

    # Replaced: automation_orchestrator_api(spark, raw_path, bucket, table, object_key)
    result = FullETLOrchestrator(spark, DEFAULT_WAREHOUSE_ROOT).run(
        raw_path=req.raw_path,
        bucket=req.bucket,
        table=req.table,
        object_key=req.object_key,
        trigger_views=False,   # Views are handled by maybe_run_views_after_full_pipeline
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
            run_job(req)
            print(f"[WORKER-{worker_id}] Done: {req.raw_path}")
            job_success = True
        except Exception as e:
            print(f"[WORKER-{worker_id} ERROR] {e}")
            traceback.print_exc()
        finally:
            job_queue.task_done()

            if job_success and req.table:
                with STATE_LOCK:
                    COMPLETED_TABLES.add(req.table)

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
    payload = {
        "raw_path":         req.raw_path,
        "bucket":           req.bucket,
        "table":            req.table,
        "object_key":       req.object_key,
        "execute_notebook": req.execute_notebook,
    }

    # Metadata-only mode — no execution.
    if not req.execute_notebook:
        return {
            "status":       "received_only",
            "message":      "Metadata received from NiFi and written to out folder",
            "request_file": OUTPUT_REQUEST,
            "data":         payload,
        }

    # Queue the job and return immediately (non-blocking).
    try:
        job_queue.put(req)
        print(f"[QUEUE] Added job: {req.raw_path} | Queue size: {job_queue.qsize()}")
        return {
            "status":   "queued",
            "message":  "ETL job added to queue",
            "raw_path": req.raw_path,
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