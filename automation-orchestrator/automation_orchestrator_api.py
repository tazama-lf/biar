"""
automation_orchestrator_api.py
-------------------------------
FastAPI service that receives NiFi trigger events and dispatches ETL jobs
via FullETLOrchestrator.  Views are built automatically once the job queue
drains (same behaviour as the original monolith-based API).
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import traceback
import uuid
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

# Durable, append-only log of job lifecycle events (#164) — job_queue alone
# is in-memory only, so a job that's queued but not yet picked up by a
# worker is silently lost if the process restarts. See the JOB TRACKING
# section below for how this is written to and replayed.
JOB_JOURNAL_PATH = _env("JOB_JOURNAL_PATH", os.path.join(OUT_DIR, "job_journal.jsonl"))

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
# JOB TRACKING  (durable journal + in-memory per-job status) — #164
# ===================================================================
#
# job_queue by itself can't answer "did job X actually run?": it's
# in-memory only, so a job that's put() but not yet get()'d by a worker
# disappears if the process restarts, while the caller already got a 200.
#
# JOB_JOURNAL_PATH is an append-only log of "queued"/"running"/"done"/
# "failed" events. submit() below writes the "queued" event and waits for
# it to land on disk *before* acking the request; worker() writes "running"
# when it picks the job up and the terminal event once it finishes.
# _recover_unfinished_jobs() is called once at startup to replay the
# journal: jobs with a "queued" event but no terminal event are re-enqueued
# (exactly the jobs the issue describes as silently lost), and jobs that
# *did* reach a terminal event are used to repopulate JOB_STATUS for them
# — see that function's docstring for why the journal itself still drops
# terminal entries on compaction even though JOB_STATUS keeps them.
#
# JOB_STATUS is the in-memory counterpart backing GET /checkstatus/{job_id}.
# It is not itself durable — a job shown as "running" when the process died
# reverts to "queued" after the journal replay below (correct, since that
# run never actually finished), while a job that reached "done"/"failed"
# is restored to that same terminal status from the journal.

JOB_STATUS_LOCK = threading.Lock()
JOB_STATUS: dict[str, dict] = {}

# Guards every read-modify-write of JOB_JOURNAL_PATH — both the plain
# append in _journal_append() and the atomic rewrite in
# _recover_unfinished_jobs()'s compaction step. /checksubmit runs in
# FastAPI's threadpool, so concurrent requests (and, with NUM_WORKERS>1,
# concurrent workers writing terminal events) can call _journal_append()
# at the same time; O_APPEND only guarantees the *position* of each
# write() is atomic, not that a whole multi-line buffered write can't
# interleave with another thread's, so an unlocked writer risks a
# malformed line that _recover_unfinished_jobs() would then silently skip.
JOURNAL_LOCK = threading.Lock()


def _journal_append(event: dict) -> None:
    """
    Append one event to the durable job journal (creating its directory if
    needed) and fsync before returning. Raises on failure rather than
    swallowing it — submit() below relies on that to avoid acking a job
    whose "queued" event didn't actually make it to disk.
    """
    directory = os.path.dirname(JOB_JOURNAL_PATH) or "."
    os.makedirs(directory, exist_ok=True)
    with JOURNAL_LOCK, open(JOB_JOURNAL_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")
        f.flush()
        os.fsync(f.fileno())


def _replace_journal_atomically(lines: list[str]) -> None:
    """
    Replace JOB_JOURNAL_PATH's contents with `lines` without ever leaving it
    truncated: write to a temp file in the same directory, fsync it, then
    os.replace() it over the real path (atomic on POSIX — the journal is
    either the old, fully-written file or the new one, never a partial
    file). Also fsyncs the containing directory so the rename itself
    survives a crash, not just the file's own contents.

    Directory fsync is a best-effort POSIX durability step — some
    platforms/filesystems don't support fsync'ing a directory fd, so that
    part is allowed to fail silently rather than break the replace, which
    has already succeeded by that point.
    """
    directory = os.path.dirname(JOB_JOURNAL_PATH) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".job_journal.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.writelines(lines)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, JOB_JOURNAL_PATH)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise

    try:
        dir_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError:
        pass


def _recover_unfinished_jobs() -> tuple[list[tuple[str, TriggerRequest]], list[dict]]:
    """
    Replay JOB_JOURNAL_PATH and return (unfinished, terminal_status_entries):

      - unfinished: (job_id, TriggerRequest) pairs whose "queued" event has
        no matching "done"/"failed" event — these are the ones a previous
        process exit (crash or restart) lost before a worker finished them,
        to be re-enqueued at startup.
      - terminal_status_entries: full JOB_STATUS-shaped dicts for jobs that
        *did* reach "done"/"failed" before this restart. JOB_STATUS is
        in-memory only and gets wiped on every restart, so without this,
        GET /checkstatus/{job_id} would 404 for a job that completed just
        before the process stopped, even though its outcome is sitting
        right there in the journal. Reconstructed from whichever of
        "queued"/"running"/the terminal event are present for that job_id
        — "running"'s started_at may be absent if the process died before
        that event was journaled, in which case it's reported as null
        rather than guessed.

    The journal itself is still compacted down to just the still-unfinished
    "queued" events (started_at/running and terminal events are dropped),
    so a long-running instance's journal stays bounded by the current
    backlog rather than growing forever — terminal status is preserved
    in-memory for the restart that follows, not forever. Written atomically
    (temp file + os.replace) so a crash mid-rewrite can never leave the
    journal truncated/empty — the one thing that would defeat the entire
    point of this journal.
    """
    with JOURNAL_LOCK:
        if not os.path.exists(JOB_JOURNAL_PATH):
            return [], []

        queued_events: dict[str, dict] = {}
        running_events: dict[str, dict] = {}
        terminal_events: dict[str, dict] = {}

        with open(JOB_JOURNAL_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except ValueError:
                    # A hard crash can truncate the last line mid-write —
                    # skip it rather than let one bad line block startup.
                    continue
                job_id = event.get("job_id")
                if not job_id:
                    continue
                if event.get("event") == "queued":
                    queued_events[job_id] = event
                elif event.get("event") == "running":
                    running_events[job_id] = event
                elif event.get("event") in ("done", "failed"):
                    terminal_events[job_id] = event

        unfinished: list[tuple[str, TriggerRequest]] = []
        terminal_status_entries: list[dict] = []
        for job_id, event in queued_events.items():
            terminal = terminal_events.get(job_id)
            if terminal is None:
                try:
                    req = TriggerRequest(**event["request"])
                except Exception:
                    print(f"[RECOVERY] Skipping unreplayable journal entry for job_id={job_id}")
                    continue
                unfinished.append((job_id, req))
            else:
                request = event.get("request") or {}
                running = running_events.get(job_id)
                terminal_status_entries.append({
                    "job_id":      job_id,
                    "status":      terminal.get("event"),
                    "table":       request.get("table"),
                    "raw_path":    request.get("raw_path"),
                    "queued_at":   event.get("queued_at"),
                    "started_at":  running.get("started_at") if running else None,
                    "finished_at": terminal.get("finished_at"),
                    "error":       terminal.get("error"),
                })

        # Compact: rewrite the journal to keep only the still-unfinished
        # "queued" events (see docstring above for why terminal/running
        # events aren't kept here even though they were just used above).
        _replace_journal_atomically([
            json.dumps(queued_events[job_id]) + "\n" for job_id, _req in unfinished
        ])

    return unfinished, terminal_status_entries

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

    Also updates JOB_STATUS and writes the job's terminal journal event
    (#164), so GET /checkstatus/{job_id} can report real completion instead
    of callers only ever seeing the "queued" ack from /checksubmit.
    """
    print(f"[WORKER-{worker_id}] Started")
    while True:
        job_id, req = job_queue.get()
        print(f"[WORKER-{worker_id}] Picked job: {req.raw_path} (job_id={job_id})")

        started_at = time.time()
        with JOB_STATUS_LOCK:
            entry = JOB_STATUS.setdefault(job_id, {"job_id": job_id, "table": req.table, "raw_path": req.raw_path, "error": None})
            entry["status"] = "running"
            entry["started_at"] = started_at
        try:
            # Best-effort: if this is lost (process dies before it lands),
            # a later restart's status reconstruction just reports
            # started_at as null for this job rather than failing anything.
            _journal_append({"job_id": job_id, "event": "running", "started_at": started_at})
        except Exception as e:
            print(f"[WORKER-{worker_id}] Failed to journal running event for job_id={job_id}: {e}")

        # Block new ETL work while views are being built.
        with STATE_COND:
            while VIEW_BUILD_IN_PROGRESS:
                STATE_COND.wait()

        job_success = False
        job_error = None
        try:
            print(f"[WORKER-{worker_id}] Processing: {req.raw_path}")
            job_result = run_job(req)
            print(f"[WORKER-{worker_id}] Done: {req.raw_path}")
            job_success = True
        except Exception as e:
            job_error = str(e)
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

            terminal_event = "done" if job_success else "failed"
            finished_at = time.time()
            with JOB_STATUS_LOCK:
                entry = JOB_STATUS.setdefault(job_id, {"job_id": job_id, "table": req.table, "raw_path": req.raw_path})
                entry["status"] = terminal_event
                entry["finished_at"] = finished_at
                entry["error"] = job_error

            # Retry a few times before giving up: if this write never lands,
            # _recover_unfinished_jobs() has no way to tell this job apart
            # from one that's genuinely still queued, and would re-run it
            # after a future restart — retrying durably here is cheaper and
            # safer than relying on every ETL's run() being idempotent.
            journal_event = {"job_id": job_id, "event": terminal_event, "finished_at": finished_at, "error": job_error}
            for attempt in range(3):
                try:
                    _journal_append(journal_event)
                    break
                except Exception as e:
                    if attempt < 2:
                        print(f"[WORKER-{worker_id}] Retry {attempt + 1}/3: failed to journal terminal event for job_id={job_id}: {e}")
                        time.sleep(0.5 * (attempt + 1))
                    else:
                        # Out of retries. This only affects restart-recovery
                        # bookkeeping for this one job (it may get needlessly
                        # re-run after a future restart) — must not crash the
                        # worker thread or skip the real completion work
                        # above, which has already happened.
                        print(f"[WORKER-{worker_id}] Giving up after 3 attempts: failed to journal terminal event for job_id={job_id}: {e}")

            maybe_run_views_after_full_pipeline()

# ===================================================================
# STARTUP RECOVERY  (#164 — re-queue anything lost by a previous exit)
# ===================================================================

_unfinished_jobs, _terminal_status_entries = _recover_unfinished_jobs()

for _job_id, _req in _unfinished_jobs:
    print(f"[RECOVERY] Re-queuing job from journal: {_req.raw_path} (job_id={_job_id})")
    with JOB_STATUS_LOCK:
        JOB_STATUS[_job_id] = {
            "job_id":     _job_id,
            "status":     "queued",
            "table":      _req.table,
            "raw_path":   _req.raw_path,
            "queued_at":  None,
            "started_at": None,
            "finished_at": None,
            "error":      None,
        }
    job_queue.put((_job_id, _req))

# Restore status for jobs that finished before this restart, so
# /checkstatus/{job_id} keeps answering for them this time around instead
# of 404ing just because JOB_STATUS itself doesn't survive a restart.
if _terminal_status_entries:
    print(f"[RECOVERY] Restoring status for {len(_terminal_status_entries)} job(s) that finished before this restart")
    with JOB_STATUS_LOCK:
        for _entry in _terminal_status_entries:
            JOB_STATUS[_entry["job_id"]] = _entry

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
    job_id = str(uuid.uuid4())

    payload = {
        "raw_path":         normalized_req.raw_path,
        "bucket":           normalized_req.bucket,
        "table":            normalized_req.table,
        "object_key":       normalized_req.object_key,
        "execute_notebook": normalized_req.execute_notebook,
    }

    # ------------------------------------------------------------------
    # Persist the job to the durable journal *before* acking it (#164).
    # job_queue.put() below is in-memory only, so without this, a restart
    # between put() and a worker picking the job up would silently drop it
    # while the caller already believes (from the 200 below) that it's
    # queued. If the journal write itself fails, deliberately don't fall
    # through to queuing/acking a job we can't actually recover.
    # ------------------------------------------------------------------
    try:
        _journal_append({
            "job_id":    job_id,
            "event":     "queued",
            "queued_at": time.time(),
            "request":   normalized_req.dict(),
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to persist job: {e}")

    with JOB_STATUS_LOCK:
        JOB_STATUS[job_id] = {
            "job_id":      job_id,
            "status":      "queued",
            "table":       normalized_req.table,
            "raw_path":    normalized_req.raw_path,
            "queued_at":   time.time(),
            "started_at":  None,
            "finished_at": None,
            "error":       None,
        }

    # ------------------------------------------------------------------
    # Always queue the ETL job (non-blocking)
    # ------------------------------------------------------------------
    try:
        job_queue.put((job_id, normalized_req))
        print(
            f"[QUEUE] Added job: {normalized_req.raw_path or normalized_req.object_key} "
            f"| table={normalized_req.table} | job_id={job_id} | Queue size: {job_queue.qsize()}"
        )
        return {
            "status":   "queued",
            "job_id":   job_id,
            "message":  "ETL job added to queue",
            "raw_path": normalized_req.raw_path,
            "bucket":   normalized_req.bucket,
            "table":    normalized_req.table,
            "data":     payload,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ===================================================================
# JOB STATUS ENDPOINT  (#164)
# ===================================================================

@app.get("/checkstatus/{job_id}")
def check_status(job_id: str):
    """
    Report a single job's actual lifecycle state — "queued" (accepted, not
    yet picked up), "running", "done", or "failed" — so a caller that wants
    to confirm real completion (rather than just the /checksubmit ack) has
    somewhere to ask. 404s for a job_id this process has no record of.
    """
    with JOB_STATUS_LOCK:
        status_entry = JOB_STATUS.get(job_id)
        if status_entry is not None:
            status_entry = dict(status_entry)
    if status_entry is None:
        raise HTTPException(status_code=404, detail=f"Unknown job_id: {job_id}")
    return status_entry

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
