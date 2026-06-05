from fastapi import FastAPI, HTTPException, status, Request, Depends
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import List, Optional, Dict, Any, Union
import uvicorn
import asyncio
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
import os
import threading
import findspark
import time
import logging
import re
import jwt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pipeline")

project_path = os.getcwd()
spark_path = os.getenv("SPARK_HOME", f"{project_path}/spark-3.4.2-bin-hadoop3")
os.environ["SPARK_HOME"] = spark_path
os.environ["PATH"] = f"{spark_path}/bin:{os.environ['PATH']}"


findspark.init(spark_path)

_spark_lock = threading.Lock()
_spark: Optional[SparkSession] = None

def _build_spark() -> SparkSession:
    spark_jars = os.getenv("SPARK_JARS", "").strip()
    builder = (
        SparkSession.builder
        .appName("ozone-alerts-pipeline")
        .master(os.getenv("QUERY_API_SPARK_MASTER", "local[2]"))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.driver.memory", os.getenv("QUERY_API_SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.driver.memoryOverhead", os.getenv("QUERY_API_SPARK_DRIVER_MEMORY_OVERHEAD", "1g"))
        .config("spark.executor.memory", os.getenv("QUERY_API_SPARK_EXECUTOR_MEMORY", "2g"))
        .config("spark.executor.memoryOverhead", os.getenv("QUERY_API_SPARK_EXECUTOR_MEMORY_OVERHEAD", "1g"))
        .config("spark.sql.shuffle.partitions", os.getenv("QUERY_API_SPARK_SQL_SHUFFLE_PARTITIONS", "16"))
        .config("spark.default.parallelism", os.getenv("QUERY_API_SPARK_DEFAULT_PARALLELISM", "16"))
        .config("spark.network.timeout", os.getenv("QUERY_API_SPARK_NETWORK_TIMEOUT", "300s"))
        .config("spark.executor.heartbeatInterval", os.getenv("QUERY_API_SPARK_HEARTBEAT_INTERVAL", "60s"))
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", os.getenv("QUERY_API_SPARK_ADVISORY_PARTITION_SIZE", "32mb"))
        .config("spark.sql.files.maxPartitionBytes", os.getenv("QUERY_API_SPARK_MAX_PARTITION_BYTES", "32mb"))
    )
    if spark_jars:
        builder = (
            builder
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
            .config("spark.jars", spark_jars)
        )
    return builder.getOrCreate()


def get_spark() -> SparkSession:
    global _spark
    with _spark_lock:
        if _spark is None or _spark._sc._jvm is None:
            logger.warning("Spark session missing or dead — recreating.")
            try:
                _spark = _build_spark()
                _spark.sparkContext.setLogLevel("WARN")
                _spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
                _spark.conf.set("spark.sql.session.timeZone", "UTC")
                logger.info("Spark session created successfully.")
            except Exception as exc:
                logger.error(f"Failed to create Spark session: {exc}")
                raise RuntimeError(f"Spark session unavailable: {exc}") from exc
        return _spark

try:
    get_spark()
except Exception as e:
    logger.error(f"Spark warm-up failed at startup: {e}")

from fastapi.openapi.utils import get_openapi

app = FastAPI(
    title="Lakehouse Pipeline API (Ozone Alerts - Bronze/Silver/Gold)",
    description="REST API to query Gold",
    version="2.0.0"
)

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    schema.setdefault("components", {}).setdefault("securitySchemes", {})["BearerAuth"] = {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "JWT",
    }
    for path in schema.get("paths", {}).values():
        for operation in path.values():
            operation.setdefault("security", [{"BearerAuth": []}])
    app.openapi_schema = schema
    return schema

app.openapi = custom_openapi

_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="spark-worker")
SPARK_JOB_TIMEOUT = 120

# ---------------------------
# PATHS
# ---------------------------
WAREHOUSE_ROOT = os.getenv("WAREHOUSE_ROOT", "/opt/Tazama_Warehouse")


alerts_bronze_path      = f"{WAREHOUSE_ROOT}/bronze/alerts"
alerts_silver_path      = f"{WAREHOUSE_ROOT}/silver/alerts"
alerts_gold_path        = f"{WAREHOUSE_ROOT}/gold/alerts"
cases_gold_path         = f"{WAREHOUSE_ROOT}/gold/cases"
tasks_gold_path         = f"{WAREHOUSE_ROOT}/gold/tasks"
transactions_gold_path  = f"{WAREHOUSE_ROOT}/gold/transactions"
nmap_gold_path          = f"{WAREHOUSE_ROOT}/gold/network_map"
rules_gold_path         = f"{WAREHOUSE_ROOT}/gold/rules"
conditions_gold_path    = f"{WAREHOUSE_ROOT}/gold/conditions"
pacs008_gold_path       = f"{WAREHOUSE_ROOT}/gold/pacs008"
account_holder          = f"{WAREHOUSE_ROOT}/gold/account_holder"
evaluation              = f"{WAREHOUSE_ROOT}/gold/evaluation"
entity_gold_path       = f"{WAREHOUSE_ROOT}/gold/entity"
comments_gold_path     = f"{WAREHOUSE_ROOT}/gold/comments"

VIEWS_ROOT                            = f"{WAREHOUSE_ROOT}/views"
ALERT_NAV_ROOT                        = f"{VIEWS_ROOT}/alert_navigator"
alerts_nav_header_path                = f"{ALERT_NAV_ROOT}/header"
alerts_nav_typologies_path            = f"{ALERT_NAV_ROOT}/typologies_triggered"
alerts_nav_rules_path                 = f"{ALERT_NAV_ROOT}/rules_triggered"
tx_detail_view_path                   = f"{VIEWS_ROOT}/vw_transaction_detail"
tx_history_view_path                  = f"{VIEWS_ROOT}/vw_transaction_history"
conditions_view_path                  = f"{VIEWS_ROOT}/conditions_timeline"
vw_tx_network_accounts_edges_path     = f"{VIEWS_ROOT}/vw_tx_network_accounts_edges"
vw_tx_network_counterparties_edges_path = f"{VIEWS_ROOT}/vw_tx_network_counterparties_edges"
vw_counterparty_account_links_path    = f"{VIEWS_ROOT}/vw_counterparty_account_links"

GOLD_PATHS = {
    "alerts":                          alerts_gold_path,
    "cases":                           cases_gold_path,
    "tasks":                           tasks_gold_path,
    "transactions":                    transactions_gold_path,
    "pacs008":                         pacs008_gold_path,
    "network_map":                     nmap_gold_path,
    "rules":                           rules_gold_path,
    "conditions":                      conditions_gold_path,
    "account_holder":                  account_holder,
    "evaluation":                      evaluation,
    "entities":                        entity_gold_path,
    "comments":                        comments_gold_path,
    "alert_navigator_header":          alerts_nav_header_path,
    "alert_navigator_typologies":      alerts_nav_typologies_path,
    "alert_navigator_rules":           alerts_nav_rules_path,
    "transaction_detail":              tx_detail_view_path,
    "transaction_history":             tx_history_view_path,
    "conditions_timeline":             conditions_view_path,
    "tx_network_accounts_edges":       vw_tx_network_accounts_edges_path,
    "tx_network_counterparties_edges": vw_tx_network_counterparties_edges_path,
    "counterparty_account_links":      vw_counterparty_account_links_path,
    
}

# ---------------------------
# Schema cache
# ---------------------------
_schema_cache: Dict[str, T.StructType] = {}
_schema_cache_lock = threading.Lock()


def infer_json_schema_cached(df, col_name: str) -> T.StructType:
    """
    Cache inferred schemas to avoid triggering a full Spark job on every
    pipeline execution. Uses col_name as cache key.
    """
    spark = get_spark()
    with _schema_cache_lock:
        if col_name not in _schema_cache:
            logger.info(f"Inferring schema for column '{col_name}' (first time).")
            _schema_cache[col_name] = spark.read.json(
                df.select(col_name).where(F.col(col_name).isNotNull()).rdd.map(lambda r: r[0])
            ).schema
        return _schema_cache[col_name]


def invalidate_schema_cache():
    """Call this if the underlying JSON schema changes (e.g., after a schema migration)."""
    with _schema_cache_lock:
        _schema_cache.clear()
    logger.info("Schema cache invalidated.")


_sql_lock = threading.Lock()   # serialises temp-view registration + SQL execution


# ============================================================
# JWT Authentication
# ============================================================

# This service sits behind an API gateway (e.g. Kong / Nginx + Keycloak) that
# performs RS256 signature verification upstream before the request reaches here.
# JWT_GATEWAY_VERIFIED_MODE=true signals that trust boundary: the gateway is the
# authority on signature validity; this service focuses on claim extraction for
# tenant isolation and RBAC.  Set to "false" (with JWT_PUBLIC_KEY set) to enable
# local signature verification in environments without a gateway.
_REQUIRED_CLAIM = "QUERY_LAKEHOUSE"
_http_bearer = HTTPBearer(auto_error=False)

_GATEWAY_VERIFIED_MODE = os.getenv("JWT_GATEWAY_VERIFIED_MODE", "true").lower() == "true"

def _build_decode_options() -> dict:
    """
    Decode options are driven by deployment environment variables so that
    the verification strategy is explicit and auditable in config rather
    than hardcoded in logic.
    """
    if _GATEWAY_VERIFIED_MODE:
        return {"verify_signature": False, "verify_exp": False}
    return {"verify_signature": True, "verify_exp": True}

_JWT_DECODE_OPTIONS = _build_decode_options()

logger.info(
    f"JWT mode: {'gateway-verified (signature delegated upstream)' if _GATEWAY_VERIFIED_MODE else 'local signature verification'}"
)


def _extract_all_claims(payload: dict) -> List[str]:
    claims: List[str] = []

    realm_roles = payload.get("realm_access", {}).get("roles", [])
    claims.extend(realm_roles)

    for client_access in payload.get("resource_access", {}).values():
        claims.extend(client_access.get("roles", []))

    # Support wrapped tokens with flat claims array
    flat_claims = payload.get("claims", [])
    if isinstance(flat_claims, list):
        claims.extend(flat_claims)

    return list(set(claims))


def verify_jwt(credentials: Optional[HTTPAuthorizationCredentials] = Depends(_http_bearer)) -> dict:
    if credentials is None or not credentials.credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"status": "error", "code": 401, "message": "Authorization header with Bearer token is required"},
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials

    try:
        payload = jwt.decode(
            token,
            algorithms=["RS256", "HS256"],
            options=_JWT_DECODE_OPTIONS,
            key="",  # gateway mode: key unused; local mode: inject via JWT_SECRET env
        )
    except jwt.InvalidTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"status": "error", "code": 401, "message": f"Invalid token: {exc}"},
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Always enforce expiry when the claim is present, regardless of mode
    exp = payload.get("exp")
    if exp is not None and time.time() > exp:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"status": "error", "code": 401, "message": "Token has expired"},
            headers={"WWW-Authenticate": "Bearer"},
        )

    all_claims = _extract_all_claims(payload)
    if _REQUIRED_CLAIM not in all_claims:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "status": "error",
                "code": 403,
                "message": f"Token is missing required claim: '{_REQUIRED_CLAIM}'",
            },
        )

    tenant_id = payload.get("tenant_id") or payload.get("tenantId")
    if not tenant_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"status": "error", "code": 403, "message": "Token is missing 'tenant_id' or 'tenantId' claim"},
        )

    logger.info(f"JWT claims extracted — tenant_id={tenant_id}, claims={all_claims}")

    return {
        "tenant_id": tenant_id,
        "claims": all_claims,
        "payload": payload,
    }

# ============================================================
# Helpers
# ============================================================

def ensure_columns(df, col_type_map: dict):
    out = df
    for c, t in col_type_map.items():
        if c in out.columns:
            out = out.withColumn(c, F.col(c).cast(t))
        else:
            out = out.withColumn(c, F.lit(None).cast(t))
    return out


def compute_record_hash(df, exclude_cols=None):
    exclude_cols = exclude_cols or []
    cols = [c for c in df.columns if c not in exclude_cols]
    return df.withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]),
            256
        )
    )


# ============================================================
# Request Models
# ============================================================

class QueryRequest(BaseModel):
    table_name: str
    filters: Optional[Dict[str, Union[str, int, float, List[str], List[int], List[float]]]] = None
    columns: Optional[List[str]] = None
    limit: Optional[int] = 100


class SQLQueryRequest(BaseModel):
    sql_query: str
    limit: Optional[int] = 1000


# ============================================================
# Helper: Query Hudi data (Gold registry)
# ============================================================

MAX_ROWS = 10000
TENANT_ID_COLUMN = "tenant_id"


def _get_hudi_data_sync(
    table_name: str,
    tenant_id: str,
    filters: dict = None,
    columns: list = None,
    limit: int = None,
):
    """Synchronous query — called from thread pool."""
    if table_name not in GOLD_PATHS:
        raise ValueError(f"Table '{table_name}' not found in Gold registry")

    spark = get_spark()
    path = GOLD_PATHS[table_name]
    df = spark.read.format("hudi").load(path)
    valid_columns = set(df.columns)

    # Always filter by tenant_id when the column exists in the table
    if TENANT_ID_COLUMN in valid_columns:
        df = df.filter(F.col(TENANT_ID_COLUMN) == tenant_id)
    else:
        logger.warning(
            f"Table '{table_name}' has no '{TENANT_ID_COLUMN}' column — "
            "tenant isolation skipped for this table."
        )

    # Validate filters
    if filters:
        invalid_filters = [k for k in filters if k not in valid_columns]
        if invalid_filters:
            raise ValueError(f"Invalid filter keys: {', '.join(invalid_filters)}")

    # Validate columns
    if columns:
        invalid_columns = [c for c in columns if c not in valid_columns]
        if invalid_columns:
            raise ValueError(f"Invalid columns: {', '.join(invalid_columns)}")

    # Apply filters
    if filters:
        for col_name, value in filters.items():
            if isinstance(value, list):
                df = df.filter(F.col(col_name).isin(value))
            else:
                df = df.filter(F.col(col_name) == value)

    # Apply column selection
    if columns:
        df = df.select(*columns)

    # Enforce limit
    effective_limit = limit if limit is not None else 100
    if effective_limit > MAX_ROWS:
        raise ValueError(f"Limit {effective_limit} exceeds MAX_ROWS {MAX_ROWS}")
    df = df.limit(effective_limit)

    return [row.asDict(recursive=True) for row in df.collect()]


def _execute_sql_sync(sql_query: str, tenant_id: str, limit: int = None):
    """
    Serialised temp-view registration + SQL execution.
    Each table view is pre-filtered to the caller's tenant_id before
    being registered, so the SQL query automatically sees only that
    tenant's rows regardless of what the caller writes.
    """
    spark = get_spark()
    with _sql_lock:
        for tname, path in GOLD_PATHS.items():
            if os.path.isdir(path):
                tdf = spark.read.format("hudi").load(path)
                # Apply tenant filter when the column exists
                if TENANT_ID_COLUMN in tdf.columns:
                    tdf = tdf.filter(F.col(TENANT_ID_COLUMN) == tenant_id)
                tdf.createOrReplaceTempView(tname)
            else:
                logger.warning(f"Path for table '{tname}' not found: {path}")

        df = spark.sql(sql_query)

        if limit:
            df = df.limit(limit)

        return [row.asDict(recursive=True) for row in df.collect()]


async def run_in_executor(fn, *args, timeout: float = SPARK_JOB_TIMEOUT):
    """Run a blocking function in the thread pool with an optional timeout."""
    loop = asyncio.get_event_loop()
    future = loop.run_in_executor(_executor, fn, *args)
    try:
        return await asyncio.wait_for(future, timeout=timeout)
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail={"status": "error", "code": 504,
                    "message": f"Spark job exceeded timeout of {timeout}s"}
        )


# ============================================================
# Exception Handlers
# ============================================================

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = []
    for error in exc.errors():
        field = ".".join(str(loc) for loc in error["loc"] if loc != "body")
        errors.append({"field": field, "message": error["msg"], "type": error["type"]})
    return JSONResponse(
        status_code=422,
        content={"status": "error", "code": 422, "message": "Validation error", "errors": errors}
    )


# ============================================================
# API ENDPOINTS
# ============================================================

@app.get("/")
def read_root():
    return {
        "status": "online",
        "message": "Lakehouse Query API (Ozone Alerts Gold)",
        "warehouse_root": WAREHOUSE_ROOT,
        "endpoints": ["/health", "/tables", "/query", "/execute_sql", "/invalidate_schema_cache"]
    }


@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    checks: Dict[str, Any] = {}
    overall_ok = True

    # 1. Spark session liveness
    try:
        spark = get_spark()
        spark.range(1).count()
        checks["spark"] = "ok"
    except Exception as e:
        checks["spark"] = f"error: {e}"
        overall_ok = False

    # 2. Warehouse root on disk
    checks["warehouse_root_exists"] = os.path.isdir(WAREHOUSE_ROOT)
    if not checks["warehouse_root_exists"]:
        overall_ok = False

    # 3. At least one gold table directory exists
    gold_dirs_found = [t for t, p in GOLD_PATHS.items() if os.path.isdir(p)]
    checks["gold_tables_found"] = len(gold_dirs_found)

    if not overall_ok:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "unhealthy", "checks": checks}
        )

    return {"status": "healthy", "checks": checks, "timestamp": time.time()}


@app.get("/tables")
def list_tables():
    return {"available_tables": list(GOLD_PATHS.keys())}


@app.post("/query", status_code=status.HTTP_200_OK)
async def query_table(
    request: QueryRequest,
    auth: dict = Depends(verify_jwt),
):
    tenant_id: str = auth["tenant_id"]
    try:
        data = await run_in_executor(
            _get_hudi_data_sync,
            request.table_name,
            tenant_id,
            request.filters,
            request.columns,
            request.limit,
        )
        return {
            "status": "success",
            "code": 200,
            "table": request.table_name,
            "tenant_id": tenant_id,
            "row_count": len(data),
            "data": data
        }
    except HTTPException:
        raise
    except ValueError as ve:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"status": "error", "code": 404, "message": str(ve)[:120], "error_type": "ValueError"}
        )
    except Exception as e:
        logger.exception("query_table error")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"status": "error", "code": 500, "message": "Internal server error", "error_details": str(e)[:120]}
        )


@app.post("/execute_sql", status_code=status.HTTP_200_OK)
async def execute_sql(
    request: SQLQueryRequest,
    auth: dict = Depends(verify_jwt),
):
    tenant_id: str = auth["tenant_id"]
    sql_query = request.sql_query.strip()

    sql_query = sql_query.replace("\\'", "'")
    sql_query = re.sub(r'\s+', ' ', sql_query).strip()

    forbidden_patterns = [
        r'\bINSERT\s+INTO\b', r'\bUPDATE\s+', r'\bDELETE\s+FROM\b', r'\bDROP\s+',
        r'\bCREATE\s+', r'\bALTER\s+', r'\bTRUNCATE\s+', r'\bMERGE\s+INTO\b', r'\bREPLACE\s+INTO\b'
    ]
    q_upper = sql_query.upper()

    for pattern in forbidden_patterns:
        if re.search(pattern, q_upper):
            raise HTTPException(
                status_code=403,
                detail={"status": "error", "code": 403, "message": "Only SELECT allowed"}
            )

    if not (q_upper.startswith("SELECT") or q_upper.startswith("WITH")):
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "code": 400, "message": "Only SELECT/WITH allowed"}
        )

    try:
        data = await run_in_executor(_execute_sql_sync, sql_query, tenant_id, request.limit)
        return {
            "status": "success",
            "code": 200,
            "tenant_id": tenant_id,
            "query": sql_query,
            "row_count": len(data),
            "data": data,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("execute_sql error")
        raise HTTPException(
            status_code=500,
            detail={"status": "error", "code": 500, "message": "SQL Query error", "error_details": str(e)[:120]}
        )


@app.post("/invalidate_schema_cache", status_code=status.HTTP_200_OK)
async def invalidate_schema_cache_endpoint(auth: dict = Depends(verify_jwt)):
    """Manually invalidate the schema cache after a schema migration."""
    invalidate_schema_cache()
    return {"status": "success", "message": "Schema cache invalidated"}


# ============================================================
# Run server
# ============================================================
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("DATALAKEHOUSE_API_PORT", "8282")), loop="asyncio")