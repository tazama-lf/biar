import os
import json
import re
from typing import List, Optional, Dict, Any, Union

import uvicorn
import nest_asyncio

from fastapi import FastAPI, HTTPException, status, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

nest_asyncio.apply()

# -----------------------------
# Spark init - use embedded PySpark
# -----------------------------
project_path = os.getcwd()

# -----------------------------
# FastAPI
# -----------------------------
app = FastAPI(
    title="API for Arif Bhai's Hudi Pipeline",
    description="Bronze layer data access with streaming support for large datasets.",
    version="1.0.0"
)

# Custom exception handler for validation errors
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = []
    for error in exc.errors():
        field = ".".join(str(loc) for loc in error["loc"] if loc != "body")
        errors.append({
            "field": field,
            "message": error["msg"],
            "type": error["type"]
        })
    
    return JSONResponse(
        status_code=422,
        content={
            "status": "error",
            "code": 422,
            "message": "Validation error",
            "errors": errors
        }
    )

# ---------------------------
# PATHS
# ---------------------------
# Use environment variable or default to relative path
WAREHOUSE_ROOT = os.environ.get("WAREHOUSE_ROOT", os.path.join(project_path, "Tazama_Hudi_warehouse"))

pacs008_bronze_path = f"{WAREHOUSE_ROOT}/bronze/pacs008"
pacs002_bronze_path = f"{WAREHOUSE_ROOT}/bronze/pacs002"
transactions_bronze_path = f"{WAREHOUSE_ROOT}/bronze/transactions"

# Query registry (GOLD ONLY)
Bronze_PATHS = {
    "pacs008": pacs008_bronze_path,
    "pacs002": pacs002_bronze_path,
    "transactions": transactions_bronze_path,
}

# ---------------------------
# Spark Session (Lazy initialization)
# ---------------------------
_spark_session = None

def get_spark_session():
    """Lazy initialization of Spark session to avoid startup failures"""
    global _spark_session
    if _spark_session is None:
        _spark_session = (
            SparkSession.builder
            .appName("ozone-alerts-pipeline")
            .master("local[*]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
            .config("spark.jars", "/app/lib/hudi-spark3.4-bundle_2.12-0.14.1.jar")
            # --- MEMORY UPGRADES ---
            .config("spark.driver.memory", "24g")
            .config("spark.executor.memory", "24g")
            .config("spark.driver.maxResultSize", "20g")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.default.parallelism", "8")
            .getOrCreate()
        )
        _spark_session.sparkContext.setLogLevel("WARN")
        _spark_session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        _spark_session.conf.set("spark.sql.session.timeZone", "UTC")
    return _spark_session

# -----------------------------
# Request Models
# -----------------------------
class QueryRequest(BaseModel):
    table_name: str
    filters: Optional[Dict[str, Union[str, int, float, List[str], List[int], List[float]]]] = None
    columns: Optional[List[str]] = None
    limit: Optional[int] = 100

class SQLQueryRequest(BaseModel):
    sql_query: str
    limit: Optional[int] = 1000

# ============================================================
# Helpers
# ============================================================

def get_hudi_data(table_name: str, filters: dict = None, columns: list = None, limit: int = None):
    """Standard batch fetch (Warning: memory heavy for large limits)"""
    spark = get_spark_session()
    if table_name not in Bronze_PATHS:
        raise ValueError(f"Table {table_name} not found in Bronze registry")

    path = Bronze_PATHS[table_name]
    df = spark.read.format("hudi").load(path)
    valid_columns = set(df.columns)

    if filters:
        for col_name, value in filters.items():
            if col_name not in valid_columns or value is None or (isinstance(value, list) and len(value) == 0):
                continue
            if isinstance(value, list):
                df = df.filter(F.col(col_name).isin(value))
            else:
                df = df.filter(F.col(col_name) == value)

    if columns:
        valid_select_cols = [c for c in columns if c in valid_columns]
        if valid_select_cols:
            df = df.select(*valid_select_cols)

    if limit:
        df = df.limit(limit)

    return [row.asDict(recursive=True) for row in df.collect()]


def stream_hudi_data(table_name: str, filters: dict = None, columns: list = None, limit: int = None):
    """Generator for streaming data row-by-row to avoid OOM crashes."""
    spark = get_spark_session()
    path = Bronze_PATHS[table_name]
    df = spark.read.format("hudi").load(path)
    valid_columns = set(df.columns)

    if filters:
        for col_name, value in filters.items():
            if col_name not in valid_columns or value is None or (isinstance(value, list) and len(value) == 0):
                continue
            if isinstance(value, list):
                df = df.filter(F.col(col_name).isin(value))
            else:
                df = df.filter(F.col(col_name) == value)
    
    if columns:
        valid_select_cols = [c for c in columns if c in valid_columns]
        if valid_select_cols:
            df = df.select(*valid_select_cols)

    if limit:
        df = df.limit(limit)

    # Use toLocalIterator to pull data partition-by-partition safely
    for row in df.toLocalIterator():
        yield json.dumps(row.asDict(recursive=True)) + "\n"

# ============================================================
# API ENDPOINTS
# ============================================================

@app.get("/")
def read_root():
    return {
        "status": "online",
        "message": "API for Arif Bhai's Hudi Pipeline",
        "warehouse_root": WAREHOUSE_ROOT,
        "endpoints": ["/tables", "/query", "/query_stream", "/execute_sql"]
    }

@app.get("/tables")
def list_tables():
    return {"available_tables": list(Bronze_PATHS.keys())}

@app.post("/query", status_code=status.HTTP_200_OK)
async def query_table(request: QueryRequest):
    """Batch query endpoint (Best for < 1000 rows)."""
    try:
        data = get_hudi_data(
            table_name=request.table_name,
            filters=request.filters,
            columns=request.columns,
            limit=request.limit
        )
        return {
            "status": "success", "code": 200, "table": request.table_name,
            "row_count": len(data), "data": data
        }
    except ValueError as ve:
        raise HTTPException(status_code=404, detail={"status": "error", "message": str(ve)[:50]})
    except Exception as e:
        raise HTTPException(status_code=500, detail={"status": "error", "message": "Internal server error", "error": str(e)[:50]})

@app.post("/execute_sql", status_code=status.HTTP_200_OK)
async def execute_sql(request: SQLQueryRequest):
    sql_query = request.sql_query.strip()
    sql_query = re.sub(r"(\\')+(\\')+(\\')+'", "'", sql_query)
    sql_query = re.sub(r"\\'\\'\\'", "'", sql_query)
    sql_query = re.sub(r"\\'", "'", sql_query)
    sql_query = re.sub(r'\s+', ' ', sql_query).strip()

    forbidden_patterns = [
        r'\bINSERT\s+INTO\b', r'\bUPDATE\s+', r'\bDELETE\s+FROM\b', r'\bDROP\s+',
        r'\bCREATE\s+', r'\bALTER\s+', r'\bTRUNCATE\s+', r'\bMERGE\s+INTO\b', r'\bREPLACE\s+INTO\b'
    ]
    q_upper = sql_query.upper()

    for pattern in forbidden_patterns:
        if re.search(pattern, q_upper):
            raise HTTPException(status_code=403, detail={"message": "Only SELECT allowed"})

    if not (q_upper.startswith("SELECT") or q_upper.startswith("WITH")):
        raise HTTPException(status_code=400, detail={"message": "Only SELECT/WITH allowed"})

    try:
        for tname, path in Bronze_PATHS.items():
            spark.read.format("hudi").load(path).createOrReplaceTempView(tname)

        df = spark.sql(sql_query)
        if request.limit:
            df = df.limit(request.limit)
 
        data = [row.asDict(recursive=True) for row in df.collect()]
        return {"status": "success", "code": 200, "query": sql_query, "row_count": len(data), "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"message": "SQL Query error", "error_details": str(e)[:50]})


if __name__ == "__main__":
    uvicorn.run("FAST_API_For_Arif_bhai:app", host="localhost", port=8181, reload=True)