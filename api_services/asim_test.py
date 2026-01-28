# app.py (Notebook-friendly FastAPI + Spark/Hudi)
# JSONL/JSON -> Bronze (Hudi) -> Silver (Hudi, ONE TABLE) -> Gold (Hudi, ONE TABLE) + Query APIs

from fastapi import FastAPI, HTTPException, Query, Request, status
from pydantic import BaseModel
from typing import List, Optional, Dict, Any, Union
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window
import json
import uvicorn
import nest_asyncio
import os
import re

nest_asyncio.apply()

# -----------------------------
# Spark init - use embedded PySpark
# -----------------------------
project_path = os.getcwd()
print(f"   Project path: {project_path}")

# -----------------------------
# FastAPI
# -----------------------------
app = FastAPI(
    title="Lakehouse Pipeline API (Bronze/Silver/Gold)",
    description="REST API to ingest JSON/JSONL into Hudi Bronze->Silver->Gold and query Gold",
    version="1.0.0"
)

# -----------------------------
# Middleware to log request bodies
# -----------------------------
@app.middleware("http")
async def log_request_body(request: Request, call_next):
    body = await request.body()

    print("\n" + "="*80)
    print(f"📥 Incoming Request")
    print("="*80)
    print(f"URL: {request.url}")
    print(f"Method: {request.method}")
    print(f"Client: {request.client.host if request.client else 'Unknown'}")
    print(f"Content-Type: {request.headers.get('content-type', 'Not specified')}")
    print(f"\nRaw Body ({len(body)} bytes):")
    print("-"*80)
    try:
        if body:
            body_str = body.decode('utf-8', errors='replace')
            print(body_str)
            try:
                json_body = json.loads(body_str)
                print("\nParsed JSON:")
                print(json.dumps(json_body, indent=2))
            except:
                pass
        else:
            print("(empty body)")
    except Exception as e:
        print(f"Error decoding body: {e}")
    print("="*80 + "\n")

    async def receive():
        return {"type": "http.request", "body": body}

    from starlette.requests import Request as StarletteRequest
    request = StarletteRequest(request.scope, receive)

    response = await call_next(request)
    return response

# -----------------------------
# Validation error handler
# -----------------------------
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    print("VALIDATION ERROR:")
    print(f"Request URL: {request.url}")
    print(f"Errors: {exc.errors()}")
    print(f"Body: {exc.body}")

    return JSONResponse(
        status_code=422,
        content={
            "detail": {
                "status": "error",
                "code": 422,
                "message": "Request validation failed",
                "errors": exc.errors()
            }
        }
    )

# -----------------------------
# Spark Session (Lazy initialization)
# -----------------------------
_spark_session = None

def get_spark_session():
    """Lazy initialization of Spark session to avoid startup failures"""
    global _spark_session
    if _spark_session is None:
        _spark_session = (
            SparkSession.builder
            .appName("Lakehouse_FastAPI")
            .master("local[*]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
            .config("spark.jars", "/app/lib/hudi-spark3.4-bundle_2.12-0.14.1.jar")
            .getOrCreate()
        )
        _spark_session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return _spark_session

# -----------------------------
# Warehouse + Table Paths
# -----------------------------
# Use environment variable or default to relative path
WAREHOUSE_ROOT = os.environ.get("WAREHOUSE_ROOT", os.path.join(project_path, "warehouse"))

# We keep ONE GOLD table for analytics
TABLE_PATHS = {
    "bronze_alerts": f"{WAREHOUSE_ROOT}/bronze/alerts",
    "silver_alerts": f"{WAREHOUSE_ROOT}/silver/alerts",
    "gold_alerts_kpi": f"{WAREHOUSE_ROOT}/gold/alerts_kpi",
}

# Query registry (GOLD ONLY)
GOLD_PATHS = {
    "alerts_kpi": TABLE_PATHS["gold_alerts_kpi"]
}

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

class JSONPayloadRequest(BaseModel):
    payload: str
    limit: Optional[int] = 100

class JSONToHudiRequest(BaseModel):
    payload: str
    table_name: str  
    limit: Optional[int] = None
    run_silver: bool = True
    run_gold: bool = True

class JSONLPathRequest(BaseModel):
    jsonl_path: str
    # table_name: str 
    run_silver: bool = True
    run_gold: bool = True

# -----------------------------
# Allowed tables + schema contract
# -----------------------------
ALLOWED_HUDI_TABLES = {
    "bronze_alerts": {
        "alert_id": "long",
        "tenant_id": "string",
        "priority": "string",
        "priority_score": "double",
        "alert_type": "string",
        "prediction_outcome": "string",
        "source": "string",
        "txtp": "string",
        "message": "string",
        "alert_data": "string",
        "transaction": "string",
        "network_map": "string",
        "confidence_per": "int",
        "case_id": "long",
        "created_at": "string"
    }
}

# -----------------------------
# Hudi options
# -----------------------------
def hudi_common_opts(table_name: str) -> dict:
    return {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",

        # schema evolution
        "hoodie.datasource.write.schema.evolution.enable": "true",
        "hoodie.datasource.read.schema.evolution.enable": "true",
        "hoodie.datasource.write.reconcile.schema": "true",
        "hoodie.schema.on.read.enable": "true",

        # housekeeping
        "hoodie.clean.automatic": "true",
        "hoodie.clean.async": "true",
        "hoodie.cleaner.commits.retained": "20",
        "hoodie.keep.min.commits": "21",
        "hoodie.keep.max.commits": "30",

        "hoodie.index.type": "BLOOM",
        "hoodie.metadata.enable": "false",
    }

def bronze_hudi_opts() -> dict:
    o = hudi_common_opts("bronze_alerts")
    o.update({
        "hoodie.datasource.write.recordkey.field": "alert_id",
        "hoodie.datasource.write.precombine.field": "created_at_ts",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
        "hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
    })
    return o

def silver_hudi_opts() -> dict:
    o = hudi_common_opts("silver_alerts")
    o.update({
        "hoodie.datasource.write.recordkey.field": "alert_id",
        "hoodie.datasource.write.precombine.field": "created_at_ts",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
        "hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
    })
    return o

def gold_hudi_opts() -> dict:
    # Partitioned Gold by event_date (matches SimpleKeyGenerator style; avoids earlier conflicts)
    o = hudi_common_opts("gold_alerts_kpi")
    o.update({
        "hoodie.datasource.write.recordkey.field": "alert_id",
        "hoodie.datasource.write.precombine.field": "created_at_ts",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.SimpleKeyGenerator",
        "hoodie.datasource.write.partitionpath.field": "event_date",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
    })
    return o

# -----------------------------
# Core: Query Hudi data (Gold registry)
# -----------------------------
def get_hudi_data(table_name: str, filters: dict = None, columns: list = None, limit: int = None):
    spark = get_spark_session()
    if table_name not in GOLD_PATHS:
        raise ValueError(f"Table {table_name} not found. Available: {list(GOLD_PATHS.keys())}")

    path = GOLD_PATHS[table_name]
    df = spark.read.format("hudi").load(path)
    valid_columns = set(df.columns)

    if filters:
        for col_name, value in filters.items():
            if col_name not in valid_columns:
                continue
            if value is None:
                continue
            if isinstance(value, list) and len(value) == 0:
                continue

            if isinstance(value, list):
                df = df.filter(col(col_name).isin(value))
            else:
                df = df.filter(col(col_name) == value)

    if columns:
        valid_select_cols = [c for c in columns if c in valid_columns]
        if valid_select_cols:
            df = df.select(*valid_select_cols)

    if limit:
        df = df.limit(limit)

    return [row.asDict(recursive=True) for row in df.collect()]

# -----------------------------
# Helpers: JSON schema inference for string columns
# -----------------------------
def infer_json_schema(df, col_name: str) -> T.StructType:
    """
    Infer JSON schema, handling empty arrays gracefully by filtering them out
    """
    spark = get_spark_session()
    # Get non-null, non-empty JSON strings
    rdd = df.select(col_name).where(
        (F.col(col_name).isNotNull()) & 
        (F.col(col_name) != "") &
        (F.col(col_name) != "{}") &
        (F.col(col_name) != "[]")
    ).rdd.map(lambda r: r[0])
    
    # If we have data, infer schema
    if not rdd.isEmpty():
        return spark.read.json(rdd).schema
    
    # Return empty struct if no valid data
    return T.StructType([])

# -----------------------------
# Pipeline: Bronze from DataFrame (contract cast + metadata)
# -----------------------------
def bronze_write_from_df(df_in):
    # enforce contract columns for bronze_alerts
    schema = ALLOWED_HUDI_TABLES["bronze_alerts"]

    df = df_in
    # cast/add missing schema columns
    for c, t in schema.items():
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(t))
        else:
            df = df.withColumn(c, F.lit(None).cast(t))

    df = (
        df.withColumn("created_at_ts", F.current_timestamp())
          .withColumn("source_file_path", F.lit("api_ingestion"))
    )

    # stable hash exclude created_at_ts
    hash_cols = [c for c in df.columns if c != "created_at_ts"]
    df = df.withColumn(
        "record_hash",
        F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in hash_cols]), 256)
    )

    df = df.withColumn("_row_payload_json", F.to_json(F.struct(*[F.col(c) for c in df.columns])))

    (
        df.write.format("hudi")
        .options(**bronze_hudi_opts())
        .mode("append")
        .save(TABLE_PATHS["bronze_alerts"])
    )

    return df

# -----------------------------
# Pipeline: Silver (ONE TABLE)
# -----------------------------
def silver_build_and_write():
    spark = get_spark_session()
    bronze = spark.read.format("hudi").load(TABLE_PATHS["bronze_alerts"])
    bronze = bronze.drop(*[c for c in bronze.columns if c.startswith("_hoodie_")])

    alert_schema = infer_json_schema(bronze, "alert_data")
    tx_schema    = infer_json_schema(bronze, "transaction")
    net_schema   = infer_json_schema(bronze, "network_map")

    s = (
        bronze
        .withColumn("alert_data_obj", F.from_json("alert_data", alert_schema))
        .withColumn("transaction_obj", F.from_json("transaction", tx_schema))
        .withColumn("network_map_obj", F.from_json("network_map", net_schema))
        .withColumn("event_ts", F.to_timestamp(F.col("alert_data_obj.timestamp")))
    )

    silver = (
        s
        .withColumn("alert_status", F.col("alert_data_obj.status"))
        .withColumn("evaluation_id", F.col("alert_data_obj.evaluationID"))
        .withColumn("dp_prcg_tm", F.col("alert_data_obj.metaData.prcgTmDP").cast("long"))
        .withColumn("ed_prcg_tm", F.col("alert_data_obj.metaData.prcgTmED").cast("long"))
        .withColumn("tadp_id", F.col("alert_data_obj.tadpResult.id"))
        .withColumn("tadp_cfg", F.col("alert_data_obj.tadpResult.cfg"))
        .withColumn("tadp_prcg_tm", F.col("alert_data_obj.tadpResult.prcgTm").cast("long"))

        .withColumn("typology_count", 
                    F.when(F.col("alert_data_obj.tadpResult.typologyResult").isNotNull(), 
                           F.size(F.col("alert_data_obj.tadpResult.typologyResult")))
                     .otherwise(0))
        .withColumn("typology_ids", 
                    F.when(F.col("alert_data_obj.tadpResult.typologyResult").isNotNull(),
                           F.expr("transform(alert_data_obj.tadpResult.typologyResult, x -> x.id)"))
                     .otherwise(F.array().cast("array<string>")))
        .withColumn("typology_results", 
                    F.when(F.col("alert_data_obj.tadpResult.typologyResult").isNotNull(),
                           F.expr("transform(alert_data_obj.tadpResult.typologyResult, x -> cast(x.result as int))"))
                     .otherwise(F.array().cast("array<int>")))
        .withColumn("typology_reviews", 
                    F.when(F.col("alert_data_obj.tadpResult.typologyResult").isNotNull(),
                           F.expr("transform(alert_data_obj.tadpResult.typologyResult, x -> cast(x.review as boolean))"))
                     .otherwise(F.array().cast("array<boolean>")))
        .withColumn("workflow_processors", 
                    F.when(F.col("alert_data_obj.tadpResult.typologyResult").isNotNull(),
                           F.expr("transform(alert_data_obj.tadpResult.typologyResult, x -> x.workflow.flowProcessor)"))
                     .otherwise(F.array().cast("array<string>")))
        .withColumn("alert_thresholds", 
                    F.when(F.col("alert_data_obj.tadpResult.typologyResult").isNotNull(),
                           F.expr("transform(alert_data_obj.tadpResult.typologyResult, x -> cast(x.workflow.alertThreshold as int))"))
                     .otherwise(F.array().cast("array<int>")))
        .withColumn("interdiction_thresholds", 
                    F.when(F.col("alert_data_obj.tadpResult.typologyResult").isNotNull(),
                           F.expr("transform(alert_data_obj.tadpResult.typologyResult, x -> cast(x.workflow.interdictionThreshold as int))"))
                     .otherwise(F.array().cast("array<int>")))
        .withColumn(
            "rule_count_total",
            F.when(F.col("alert_data_obj.tadpResult.typologyResult").isNotNull(),
                   F.expr("aggregate(alert_data_obj.tadpResult.typologyResult, 0, (acc, x) -> acc + size(coalesce(x.ruleResults, array())))"))
             .otherwise(0)
        )

        .withColumn("tx_type", F.col("transaction_obj.TxTp"))
        .withColumn("tx_tenant_id", F.col("transaction_obj.TenantId"))
        .withColumn("tx_msg_id", F.col("transaction_obj.FIToFIPmtSts.GrpHdr.MsgId"))
        .withColumn("tx_created_ts", F.to_timestamp(F.col("transaction_obj.FIToFIPmtSts.GrpHdr.CreDtTm")))
        .withColumn("tx_status", F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.TxSts"))
        .withColumn("tx_accept_ts", F.to_timestamp(F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.AccptncDtTm")))
        .withColumn("tx_original_instr_id", F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.OrgnlInstrId"))
        .withColumn("tx_original_e2e_id", F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.OrgnlEndToEndId"))
        .withColumn("instg_mmb_id", F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.InstgAgt.FinInstnId.ClrSysMmbId.MmbId"))
        .withColumn("instd_mmb_id", F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.InstdAgt.FinInstnId.ClrSysMmbId.MmbId"))

        .withColumn("charge_count", 
                    F.when(F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.ChrgsInf").isNotNull(),
                           F.size(F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.ChrgsInf")))
                     .otherwise(0))
        .withColumn("charge_agent_mmb_ids", 
                    F.when((F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.ChrgsInf").isNotNull()) & 
                           (F.size(F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.ChrgsInf")) > 0),
                           F.expr("transform(transaction_obj.FIToFIPmtSts.TxInfAndSts.ChrgsInf, x -> x.Agt.FinInstnId.ClrSysMmbId.MmbId)"))
                     .otherwise(F.array().cast("array<string>")))
        .withColumn("charge_amounts", 
                    F.when((F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.ChrgsInf").isNotNull()) & 
                           (F.size(F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.ChrgsInf")) > 0),
                           F.expr("transform(transaction_obj.FIToFIPmtSts.TxInfAndSts.ChrgsInf, x -> cast(x.Amt.Amt as double))"))
                     .otherwise(F.array().cast("array<double>")))
        .withColumn("charge_ccys", 
                    F.when((F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.ChrgsInf").isNotNull()) & 
                           (F.size(F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.ChrgsInf")) > 0),
                           F.expr("transform(transaction_obj.FIToFIPmtSts.TxInfAndSts.ChrgsInf, x -> x.Amt.Ccy)"))
                     .otherwise(F.array().cast("array<string>")))

        .withColumn("network_cfg", F.col("network_map_obj.cfg"))
        .withColumn("network_active", F.col("network_map_obj.active").cast("boolean"))
        .withColumn("network_message_count", 
                    F.when(F.col("network_map_obj.messages").isNotNull(),
                           F.size(F.col("network_map_obj.messages")))
                     .otherwise(0))
        .withColumn("network_message_ids", 
                    F.when((F.col("network_map_obj.messages").isNotNull()) & 
                           (F.size(F.col("network_map_obj.messages")) > 0),
                           F.expr("transform(network_map_obj.messages, x -> x.id)"))
                     .otherwise(F.array().cast("array<string>")))
    )

    # Dedup latest per alert_id
    w = Window.partitionBy("alert_id").orderBy(F.col("created_at_ts").desc())
    silver = silver.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

    (
        silver.write.format("hudi")
        .options(**silver_hudi_opts())
        .mode("append")
        .save(TABLE_PATHS["silver_alerts"])
    )

    return silver

# -----------------------------
# Pipeline: Gold (ONE TABLE ONLY)
# -----------------------------
def gold_build_and_write():
    spark = get_spark_session()
    silver = spark.read.format("hudi").load(TABLE_PATHS["silver_alerts"])
    silver = silver.drop(*[c for c in silver.columns if c.startswith("_hoodie_")])

    # Dedup latest per alert_id
    w = Window.partitionBy("alert_id").orderBy(F.col("created_at_ts").desc())
    s = silver.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

    gold = (
        s
        .withColumn("prediction_outcome_norm", F.upper(F.col("prediction_outcome")))
        .withColumn("alert_type_norm", F.upper(F.col("alert_type")))
        .withColumn("priority_norm", F.upper(F.col("priority")))

        .withColumn("is_false_positive", (F.col("prediction_outcome_norm") == F.lit("FALSE_POSITIVE")).cast("int"))
        .withColumn("is_false_negative", (F.col("prediction_outcome_norm") == F.lit("FALSE_NEGATIVE")).cast("int"))
        .withColumn("is_true_positive",  (F.col("prediction_outcome_norm") == F.lit("TRUE_POSITIVE")).cast("int"))
        .withColumn("is_true_negative",  (F.col("prediction_outcome_norm") == F.lit("TRUE_NEGATIVE")).cast("int"))

        .withColumn("is_fraud", F.col("alert_type_norm").contains("FRAUD").cast("int"))
        .withColumn("is_aml",   F.col("alert_type_norm").contains("AML").cast("int"))

        .withColumn("max_typology_result",
                    F.when(F.col("typology_results").isNotNull(), F.array_max("typology_results"))
                     .otherwise(F.lit(None).cast("int")))
        .withColumn("needs_review",
                    F.when(F.col("typology_reviews").isNotNull(), F.array_contains("typology_reviews", F.lit(True)))
                     .otherwise(F.lit(False)))

        .withColumn("charge_total_amount",
                    F.when(F.col("charge_amounts").isNotNull(),
                           F.expr("aggregate(charge_amounts, cast(0.0 as double), (acc,x) -> acc + coalesce(x,0.0))"))
                     .otherwise(F.lit(0.0)))
        .withColumn("charge_ccy_set",
                    F.when(F.col("charge_ccys").isNotNull(), F.array_distinct("charge_ccys"))
                     .otherwise(F.array().cast("array<string>")))
        .withColumn("has_multi_currency_charges", (F.size(F.col("charge_ccy_set")) > 1).cast("int"))

        .withColumn("event_to_ingest_ms",
                    F.when(F.col("event_ts").isNotNull(),
                           (F.col("created_at_ts").cast("long") - F.col("event_ts").cast("long")) * 1000)
                     .otherwise(F.lit(None).cast("long")))

        # IMPORTANT: prevent non-nullable Avro long issues (always fill)
        .withColumn(
            "total_processing_time_ms",
            (
                F.coalesce(F.col("dp_prcg_tm").cast("long"), F.lit(0)) +
                F.coalesce(F.col("ed_prcg_tm").cast("long"), F.lit(0)) +
                F.coalesce(F.col("tadp_prcg_tm").cast("long"), F.lit(0))
            ).cast("long")
        )

        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("security_tag", F.concat(F.lit("TENANT:"), F.col("tenant_id")))

        .select(
            "event_date",
            "alert_id", "tenant_id", "case_id",
            "priority_norm", "priority_score", "alert_type_norm",
            "prediction_outcome_norm",
            "event_ts", "created_at_ts",
            "is_fraud", "is_aml",
            "is_false_positive", "is_false_negative", "is_true_positive", "is_true_negative",
            "typology_count", "rule_count_total",
            "max_typology_result", "needs_review",
            "charge_count", "charge_total_amount", "charge_ccy_set", "has_multi_currency_charges",
            "event_to_ingest_ms",
            "total_processing_time_ms",
            "security_tag",
            "source_file_path", "record_hash"
        )
    )

    (
        gold.write.format("hudi")
        .options(**gold_hudi_opts())
        .mode("append")
        .save(TABLE_PATHS["gold_alerts_kpi"])
    )

    return gold

# -----------------------------
# API ENDPOINTS
# -----------------------------
@app.get("/")
def read_root():
    return {
        "status": "online",
        "warehouse_root": WAREHOUSE_ROOT,
        "paths": TABLE_PATHS
    }

@app.get("/tables")
def list_tables():
    """Returns available GOLD tables (ONE for analytics)."""
    return {"available_gold_tables": list(GOLD_PATHS.keys())}

@app.get("/hudi_tables")
def list_hudi_tables():
    """Returns allowed tables for ingestion."""
    return {"allowed_hudi_tables": list(ALLOWED_HUDI_TABLES.keys())}

@app.post("/query", status_code=status.HTTP_200_OK)
async def query_table(request: QueryRequest):
    try:
        data = get_hudi_data(
            table_name=request.table_name,
            filters=request.filters,
            columns=request.columns,
            limit=request.limit
        )
        return {
            "status": "success",
            "code": 200,
            "table": request.table_name,
            "row_count": len(data),
            "data": data
        }
    except ValueError as ve:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"status": "error", "code": 404, "message": str(ve)[:30], "error_type": "ValueError"}
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"status": "error", "code": 500, "message": "Internal server error", "error_details": str(e)[:30]}
        )

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
            raise HTTPException(status_code=403, detail={"status": "error", "code": 403, "message": "Only SELECT allowed"})

    if not (q_upper.startswith("SELECT") or q_upper.startswith("WITH")):
        raise HTTPException(status_code=400, detail={"status": "error", "code": 400, "message": "Only SELECT/WITH allowed"})

    try:
        # register gold tables only
        for tname, path in GOLD_PATHS.items():
            spark.read.format("hudi").load(path).createOrReplaceTempView(tname)

        df = spark.sql(sql_query)
        if request.limit:
            df = df.limit(request.limit)

        data = [row.asDict(recursive=True) for row in df.collect()]
        return {"status": "success", "code": 200, "query": sql_query, "row_count": len(data), "data": data}
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail={"status": "error", "code": 500, "message": "SQL Query error", "error_details": str(e)[:30]})

@app.post("/json_to_dataframe", status_code=status.HTTP_200_OK)
async def json_to_dataframe(request: JSONPayloadRequest):
    try:
        json_data = json.loads(request.payload)
        if not isinstance(json_data, list):
            json_data = [json_data]
        if len(json_data) == 0:
            raise HTTPException(status_code=400, detail={"status": "error", "code": 400, "message": "Empty JSON payload"})

        df = spark.createDataFrame(json_data)
        if request.limit:
            df = df.limit(request.limit)

        return {
            "status": "success",
            "code": 200,
            "row_count": df.count(),
            "columns": df.columns,
            "schema": str(df.schema),
            "data": [row.asDict(recursive=True) for row in df.collect()]
        }
    except json.JSONDecodeError as je:
        raise HTTPException(status_code=400, detail={"status": "error", "code": 400, "message": f"Invalid JSON: {je}"})
    except Exception as e:
        raise HTTPException(status_code=500, detail={"status": "error", "code": 500, "message": f"Error: {e}"})


# -----------------------------
# NEW: JSON -> Bronze -> Silver -> Gold (all automatic)
# -----------------------------
@app.post("/json_to_hudi_pipeline", status_code=status.HTTP_201_CREATED)
async def json_to_hudi_pipeline(request: JSONToHudiRequest):
    """
    Ingest JSON payload into Bronze (Hudi), then build Silver (ONE TABLE) and Gold (ONE TABLE).
    Currently supports: table_name="bronze_alerts"
    """
    try:
        if request.table_name not in ALLOWED_HUDI_TABLES:
            raise HTTPException(
                status_code=400,
                detail={"status": "error", "code": 400, "message": f"Table '{request.table_name}' not allowed"}
            )

        # parse JSON
        json_data = json.loads(request.payload)
        if not isinstance(json_data, list):
            json_data = [json_data]
        if len(json_data) == 0:
            raise HTTPException(status_code=400, detail={"status": "error", "code": 400, "message": "Empty JSON payload"})

        if request.limit:
            json_data = json_data[:request.limit]

        df_in = spark.createDataFrame(json_data)

        # ---- BRONZE ----
        bronze_df = bronze_write_from_df(df_in)
        bronze_rows = bronze_df.count()

        # ---- SILVER ----
        silver_rows = None
        if request.run_silver:
            try:
                silver_df = silver_build_and_write()
                silver_rows = silver_df.count()
            except Exception as e:
                error_msg = str(e)
                print(str(e))
                # Check for schema inference error from empty arrays
                if "INVALID_EXTRACT_BASE_FIELD_TYPE" in error_msg or "Can't extract a value from" in error_msg:
                    raise HTTPException(
                        status_code=400,
                        detail={
                            "status": "error",
                            "code": 400,
                            "message": "Schema inference failed: Empty arrays detected in JSON payload",
                        }
                    )
                # Re-raise other errors
                raise

        # ---- GOLD ----
        gold_rows = None
        if request.run_gold:
            if not request.run_silver:
                try:
                    silver_build_and_write()
                except Exception as e:
                    error_msg = str(e)
                    print(str(e))
                    if "INVALID_EXTRACT_BASE_FIELD_TYPE" in error_msg or "Can't extract a value from" in error_msg:
                        raise HTTPException(
                            status_code=400,
                            detail={
                                "status": "error",
                                "code": 400,
                                "message": "Schema inference failed: Empty arrays detected in JSON payload",
                            }
                        )
                    raise
            gold_df = gold_build_and_write()
            gold_rows = gold_df.count()

        return {
            "status": "success",
            "code": 201,
            "message": "Pipeline completed",
            "written": {
                "bronze_rows": bronze_rows,
                "silver_rows": silver_rows,
                "gold_rows": gold_rows,
            },
            "paths": TABLE_PATHS,
            "gold_query_table": "alerts_kpi"
        }

    except json.JSONDecodeError as je:
        print(je)
        raise HTTPException(status_code=400, detail={"status": "error", "code": 400, "message": f"Invalid JSON"})
    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail={"status": "error", "code": 500, "message": f"Pipeline error: {e}"})


# -----------------------------
# NEW: JSONL path -> Bronze -> Silver -> Gold (all automatic)
# -----------------------------
@app.post("/jsonl_path_to_hudi_pipeline", status_code=status.HTTP_201_CREATED)
async def jsonl_path_to_hudi_pipeline(request: JSONLPathRequest):
    """
    Provide a local JSONL path; API reads it with Spark and runs Bronze->Silver->Gold.
    """
    try:
        p = request.jsonl_path
        if not (p.startswith("file:") or os.path.exists(p)):
            raise HTTPException(status_code=400, detail={"status": "error", "code": 400, "message": f"Path not found: {p}"})

        df = spark.read.option("multiLine", "false").option("mode", "PERMISSIVE").json(p)
        
        # write bronze via same contract path
        bronze_df = bronze_write_from_df(df)
        bronze_rows = bronze_df.count()

        silver_rows = None
        if request.run_silver:
            
            silver_df = silver_build_and_write()
            silver_rows = silver_df.count()

        gold_rows = None
        if request.run_gold:
            if not request.run_silver:
                silver_build_and_write()
            gold_df = gold_build_and_write()
            gold_rows = gold_df.count()

        return {
            "status": "success",
            "code": 201,
            "message": "Pipeline completed",
            "source": p,
            "written": {
                "bronze_rows": bronze_rows,
                "silver_rows": silver_rows,
                "gold_rows": gold_rows,
            },
            "paths": TABLE_PATHS,
            "gold_query_table": "alerts_kpi"
        }

    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail={"status": "error", "code": 500, "message": f"Pipeline error"})


# -----------------------------
# Run server
# -----------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8010)
