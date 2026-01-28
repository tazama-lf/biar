# ============================================================
# COMPLETE PIPELINE: JSONL -> BRONZE (Hudi) -> SILVER (Hudi) -> GOLD (Hudi)
# Alerts pipeline (pacs.002.001.12 example) with:
# - Schema evolution + reconciliation enabled
# - No rejects (per your requirement)
# - Bronze: created_at_ts, source_file_path, record_hash, _row_payload_json
# - Silver: flattened fields + rule weights JSON + rule stats
# - Gold: BI-ready scalar-only (NO JSON, NO arrays, NO structs) + rule KPIs + top rule
# ============================================================

from fastapi import FastAPI, HTTPException, status, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional, Dict, Any, Union
import uvicorn
import nest_asyncio
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
import os
import tempfile
import json

nest_asyncio.apply()

# -----------------------------
# Spark init - use embedded PySpark
# -----------------------------
project_path = os.getcwd()

# -----------------------------
# FastAPI
# -----------------------------
app = FastAPI(
    title="Lakehouse Pipeline API (Ozone Alerts - Bronze/Silver/Gold)",
    description="REST API to ingest JSONL into Hudi Bronze->Silver->Gold (Scalar) and query Gold",
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
# PATHS (your setup)
# ---------------------------
# Use environment variable or default to relative path
WAREHOUSE_ROOT = os.environ.get("WAREHOUSE_ROOT", os.path.join(project_path, "Tazama_Hudi_warehouse"))

alerts_bronze_path = f"{WAREHOUSE_ROOT}/bronze/alerts"
alerts_silver_path = f"{WAREHOUSE_ROOT}/silver/alerts"
alerts_gold_path   = f"{WAREHOUSE_ROOT}/gold/alerts"
cases_gold_path   = f"{WAREHOUSE_ROOT}/gold/cases"
tasks_gold_path   = f"{WAREHOUSE_ROOT}/gold/tasks"
transactions_gold_path   = f"{WAREHOUSE_ROOT}/gold/transactions"
nmap_gold_path = f"{WAREHOUSE_ROOT}/gold/network_map"
rules_gold_path   = f"{WAREHOUSE_ROOT}/gold/rules"
conditions_gold_path   = f"{WAREHOUSE_ROOT}/gold/conditions"
pacs008_gold_path = f"{WAREHOUSE_ROOT}/gold/pacs008"
account_holder = f"{WAREHOUSE_ROOT}/gold/account_holder"

VIEWS_ROOT = f"{WAREHOUSE_ROOT}/views"
ALERT_NAV_ROOT = f"{VIEWS_ROOT}/alert_navigator"
alerts_nav_header_path = f"{ALERT_NAV_ROOT}/header"
alerts_nav_typologies_path = f"{ALERT_NAV_ROOT}/typologies_triggered"
alerts_nav_rules_path = f"{ALERT_NAV_ROOT}/rules_triggered"
#alerts_nav_network_eval_path = f"{ALERT_NAV_ROOT}/network_evaluated"
tx_detail_view_path = f"{VIEWS_ROOT}/vw_transaction_detail"
tx_history_view_path = f"{VIEWS_ROOT}/vw_transaction_history"
conditions_view_path = f"{VIEWS_ROOT}/conditions_timeline"
vw_tx_network_accounts_edges_path      = f"{VIEWS_ROOT}/vw_tx_network_accounts_edges"
vw_tx_network_counterparties_edges_path= f"{VIEWS_ROOT}/vw_tx_network_counterparties_edges"
vw_counterparty_account_links_path     = f"{VIEWS_ROOT}/vw_counterparty_account_links"

# Query registry (GOLD ONLY)
GOLD_PATHS = {
    "alerts": alerts_gold_path,
    "cases": cases_gold_path,
    "tasks": tasks_gold_path,
    "transactions": transactions_gold_path,
    "pacs008": pacs008_gold_path,
    "network_map": nmap_gold_path,
    "rules": rules_gold_path,
    "conditions": conditions_gold_path,
    "account_holder": account_holder,
    "alert_navigator_header": alerts_nav_header_path,
    "alert_navigator_typologies": alerts_nav_typologies_path,
    "alert_navigator_rules": alerts_nav_rules_path,
    #"alert_navigator_network_evaluated": alerts_nav_network_eval_path,
    "transaction_detail": tx_detail_view_path,
    "transaction_history": tx_history_view_path,
    "conditions_timeline": conditions_view_path,
    "tx_network_accounts_edges": vw_tx_network_accounts_edges_path,
    "tx_network_counterparties_edges": vw_tx_network_counterparties_edges_path,
    "counterparty_account_links": vw_counterparty_account_links_path,
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
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
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

class JSONLPathRequest(BaseModel):
    jsonl_path: str
    run_silver: bool = True
    run_gold: bool = True

class JSONToHudiRequest(BaseModel):
    payload: str
    table_name: str
    run_silver: bool = True
    run_gold: bool = True


# ============================================================
# Helpers
# ============================================================

def infer_json_schema(df, col_name: str) -> T.StructType:
    """
    Infer JSON schema from a string column that contains JSON objects.
    Uses only non-null values.
    """
    spark = get_spark_session()
    return spark.read.json(
        df.select(col_name).where(F.col(col_name).isNotNull()).rdd.map(lambda r: r[0])
    ).schema


def ensure_columns(df, col_type_map: dict):
    """
    Ensure DF has these columns with target Spark SQL types (string, long, double, int, boolean, timestamp, date).
    Missing columns are added as null casted.
    Existing columns are casted.
    """
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
# 1) JSONL -> BRONZE (Hudi)
# ============================================================

def jsonl_to_bronze_alerts(jsonl_path: str, source_file_path: str = None):
    """
    Read JSONL file -> normalize types -> add bronze metadata -> write to Hudi Bronze
    """
    spark = get_spark_session()
    # Read JSONL (one record per line)
    raw = (
        spark.read
             .option("multiLine", "false")
             .option("mode", "PERMISSIVE")
             .json(jsonl_path)
    )

    # Contract for alerts (you can extend later)
    bronze_contract = {
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
        "created_at": "string",
    }

    bronze = ensure_columns(raw, bronze_contract)

    bronze = (
        bronze
        .withColumn("created_at_ts", F.current_timestamp())
        .withColumn("source_file_path", F.lit(source_file_path or jsonl_path))
    )

    # Record hash (exclude created_at_ts so it stays stable)
    bronze = compute_record_hash(bronze, exclude_cols=["created_at_ts"])

    # Full row payload for debugging (store as JSON string)
    bronze = bronze.withColumn("_row_payload_json", F.to_json(F.struct(*[F.col(c) for c in bronze.columns])))

    # Hudi options (non-partitioned bronze)
    hudi_bronze_opts = {
        "hoodie.table.name": "bronze_alerts",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": "alert_id",
        "hoodie.datasource.write.precombine.field": "created_at_ts",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator",

        # schema evolution + reconciliation
        "hoodie.datasource.write.schema.evolution.enable": "true",
        "hoodie.datasource.read.schema.evolution.enable": "true",
        "hoodie.datasource.write.reconcile.schema": "true",
        "hoodie.schema.on.read.enable": "true",

        "hoodie.index.type": "BLOOM",
        "hoodie.metadata.enable": "false",
    }

    (
        bronze.write.format("hudi")
        .options(**hudi_bronze_opts)
        .mode("append")
        .save(alerts_bronze_path)
    )

    return bronze


# ============================================================
# 2) BRONZE -> SILVER (Hudi)
# ============================================================

def bronze_to_silver_alerts():
    """
    Read bronze alerts -> parse JSON columns -> flatten -> add rule_weights_json + rule stats
    -> dedup by alert_id -> write silver
    """
    spark = get_spark_session()
    bronze = spark.read.format("hudi").load(alerts_bronze_path)

    # Infer JSON schemas from bronze (string columns)
    alert_schema = infer_json_schema(bronze, "alert_data")
    tx_schema    = infer_json_schema(bronze, "transaction")
    net_schema   = infer_json_schema(bronze, "network_map")

    b = (
        bronze
        .withColumn("alert_data_obj", F.from_json("alert_data", alert_schema))
        .withColumn("transaction_obj", F.from_json("transaction", tx_schema))
        .withColumn("network_map_obj", F.from_json("network_map", net_schema))
        .withColumn("event_ts", F.to_timestamp(F.col("alert_data_obj.timestamp")))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("tx_created_ts", F.to_timestamp(F.col("transaction_obj.FIToFIPmtSts.GrpHdr.CreDtTm")))
        .withColumn("tx_accept_ts",  F.to_timestamp(F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.AccptncDtTm")))
    )

    silver = (
        b
        # IDs / keys
        .withColumn("alert_id", F.col("alert_id").cast("long"))
        .withColumn("case_id",  F.col("case_id").cast("long"))

        # alert_data flatten
        .withColumn("alert_status", F.col("alert_data_obj.status"))
        .withColumn("evaluation_id", F.col("alert_data_obj.evaluationID"))
        .withColumn("processing_time_dp", F.col("alert_data_obj.metaData.prcgTmDP").cast("long"))
        .withColumn("processing_time_ed", F.col("alert_data_obj.metaData.prcgTmED").cast("long"))
        .withColumn("tadp_id",  F.col("alert_data_obj.tadpResult.id"))
        .withColumn("tadp_cfg", F.col("alert_data_obj.tadpResult.cfg"))
        .withColumn("tadp_processing_time", F.col("alert_data_obj.tadpResult.prcgTm").cast("long"))

        # typology summaries
        .withColumn("typology_count", F.size(F.col("alert_data_obj.tadpResult.typologyResult")))
        .withColumn("typology_ids", F.expr("transform(alert_data_obj.tadpResult.typologyResult, x -> x.id)"))
        .withColumn("typology_results", F.expr("transform(alert_data_obj.tadpResult.typologyResult, x -> cast(x.result as int))"))
        .withColumn("typology_reviews", F.expr("transform(alert_data_obj.tadpResult.typologyResult, x -> cast(x.review as boolean))"))
        .withColumn("workflow_processors", F.expr("transform(alert_data_obj.tadpResult.typologyResult, x -> x.workflow.flowProcessor)"))
        .withColumn("alert_thresholds", F.expr("transform(alert_data_obj.tadpResult.typologyResult, x -> cast(x.workflow.alertThreshold as int))"))
        .withColumn("interdiction_thresholds", F.expr("transform(alert_data_obj.tadpResult.typologyResult, x -> cast(x.workflow.interdictionThreshold as int))"))
        .withColumn("rule_count_total", F.expr("aggregate(alert_data_obj.tadpResult.typologyResult, 0, (acc, x) -> acc + size(x.ruleResults))"))

        # -------- individual rule weights (rule_id + weight) as JSON + scalar summaries ----------
        .withColumn(
            "rule_pairs",
            F.flatten(
                F.expr("""
                  transform(
                    alert_data_obj.tadpResult.typologyResult,
                    t -> transform(
                      t.ruleResults,
                      r -> named_struct('rule_id', r.id, 'weight', cast(r.wght as long))
                    )
                  )
                """)
            )
        )
        .withColumn("rule_pairs", F.expr("filter(rule_pairs, x -> x.rule_id is not null)"))
        # de-dup by rule_id (typed accumulator fix)
        .withColumn(
            "rule_pairs",
            F.expr("""
              aggregate(
                rule_pairs,
                cast(array() as array<struct<rule_id:string, weight:bigint>>),
                (acc, x) ->
                  IF(
                    array_contains(transform(acc, y -> y.rule_id), x.rule_id),
                    acc,
                    concat(acc, array(x))
                  )
              )
            """)
        )
        .withColumn("rule_weights_json", F.to_json(F.col("rule_pairs")))
        .withColumn("rule_id_count_distinct", F.size(F.expr("transform(rule_pairs, x -> x.rule_id)")).cast("int"))
        .withColumn(
            "rule_weight_sum",
            F.expr("aggregate(transform(rule_pairs, x -> x.weight), cast(0 as long), (acc,x) -> acc + coalesce(x, cast(0 as long)))").cast("long")
        )
        .withColumn(
            "rule_weight_max",
            F.when(F.size("rule_pairs") > 0, F.array_max(F.expr("transform(rule_pairs, x -> x.weight)"))).otherwise(F.lit(0)).cast("long")
        )
        .withColumn(
            "rule_weight_min",
            F.when(F.size("rule_pairs") > 0, F.array_min(F.expr("transform(rule_pairs, x -> x.weight)"))).otherwise(F.lit(0)).cast("long")
        )
        # ---------------------------------------------------------------

        # transaction flatten
        .withColumn("tx_type", F.col("transaction_obj.TxTp"))
        .withColumn("tx_tenant_id", F.col("transaction_obj.TenantId"))
        .withColumn("tx_msg_id", F.col("transaction_obj.FIToFIPmtSts.GrpHdr.MsgId"))
        .withColumn("tx_status", F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.TxSts"))
        .withColumn("tx_original_instr_id", F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.OrgnlInstrId"))
        .withColumn("tx_original_e2e_id", F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.OrgnlEndToEndId"))
        .withColumn("instg_mmb_id", F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.InstgAgt.FinInstnId.ClrSysMmbId.MmbId"))
        .withColumn("instd_mmb_id", F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.InstdAgt.FinInstnId.ClrSysMmbId.MmbId"))

        # charges
        .withColumn("charge_count", F.size(F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.ChrgsInf")))
        .withColumn("charge_agent_mmb_ids", F.expr("transform(transaction_obj.FIToFIPmtSts.TxInfAndSts.ChrgsInf, x -> x.Agt.FinInstnId.ClrSysMmbId.MmbId)"))
        .withColumn("charge_amounts", F.expr("transform(transaction_obj.FIToFIPmtSts.TxInfAndSts.ChrgsInf, x -> cast(x.Amt.Amt as double))"))
        .withColumn("charge_ccys", F.expr("transform(transaction_obj.FIToFIPmtSts.TxInfAndSts.ChrgsInf, x -> x.Amt.Ccy)"))

        # network flatten
        .withColumn("network_cfg", F.col("network_map_obj.cfg"))
        .withColumn("network_active", F.col("network_map_obj.active").cast("boolean"))
        .withColumn("network_tenant_id", F.col("network_map_obj.tenantId"))
        .withColumn("network_message_count", F.size(F.col("network_map_obj.messages")))
        .withColumn("network_message_ids", F.expr("transform(network_map_obj.messages, x -> x.id)"))

        # Select final shape
        .select(
            "_hoodie_commit_time","_hoodie_commit_seqno","_hoodie_record_key","_hoodie_partition_path","_hoodie_file_name",
            "alert_id","case_id","tenant_id",
            "priority","priority_score","alert_type","prediction_outcome","source","txtp","message","confidence_per",
            "event_ts","event_date","tx_created_ts","tx_accept_ts","created_at","created_at_ts",
            "alert_status","evaluation_id",
            "processing_time_dp","processing_time_ed",
            "tadp_id","tadp_cfg","tadp_processing_time",
            "typology_count","typology_ids","typology_results","typology_reviews",
            "workflow_processors","alert_thresholds","interdiction_thresholds",
            "rule_count_total",
            "rule_weights_json","rule_id_count_distinct","rule_weight_sum","rule_weight_max","rule_weight_min",
            "tx_type","tx_tenant_id","tx_msg_id","tx_status","tx_original_instr_id","tx_original_e2e_id",
            "instg_mmb_id","instd_mmb_id",
            "charge_count","charge_agent_mmb_ids","charge_amounts","charge_ccys",
            "network_cfg","network_active","network_tenant_id","network_message_count","network_message_ids",
            "source_file_path","record_hash",
            "alert_data","transaction","network_map",
        )
        .withColumn("_row_payload_json", F.to_json(F.struct("*")))
        # keep rule_pairs out of storage (we keep json + scalar stats)
    )

    # Deduplicate by alert_id keep latest ingest
    w = Window.partitionBy("alert_id").orderBy(F.col("created_at_ts").desc())
    silver = silver.withColumn("rn", F.row_number().over(w)).filter("rn=1").drop("rn")

    # Hudi options (non-partitioned silver)
    hudi_silver_opts = {
        "hoodie.table.name": "silver_alerts",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": "alert_id",
        "hoodie.datasource.write.precombine.field": "created_at_ts",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator",

        # schema evolution + reconcile
        "hoodie.datasource.write.schema.evolution.enable": "true",
        "hoodie.datasource.read.schema.evolution.enable": "true",
        "hoodie.datasource.write.reconcile.schema": "true",
        "hoodie.schema.on.read.enable": "true",

        "hoodie.index.type": "BLOOM",
        "hoodie.metadata.enable": "false",
    }

    (
        silver.write.format("hudi")
        .options(**hudi_silver_opts)
        .mode("append")
        .save(alerts_silver_path)
    )

    return silver


# ============================================================
# 3) SILVER -> GOLD (Hudi, scalar-only)
# ============================================================

def silver_to_gold_alerts_scalar_only():
    """
    Gold: BI-ready scalar fact table (NO JSON, NO ARRAYS, NO STRUCTS)
    Includes rule weight KPIs + top_rule_id/top_rule_weight, charges KPIs, latency, processing time.
    """
    spark = get_spark_session()
    silver = spark.read.format("hudi").load(alerts_silver_path)
    silver = silver.drop(*[c for c in silver.columns if c.startswith("hoodie")])

    # Dedup safety
    w = Window.partitionBy("alert_id").orderBy(F.col("created_at_ts").desc())
    s = silver.withColumn("rn", F.row_number().over(w)).filter("rn=1").drop("rn")

    # Parse alert_data ONLY for top_rule_id (since we cannot keep arrays/json in gold)
    alert_schema = infer_json_schema(s, "alert_data")
    g = s.withColumn("alert_data_obj", F.from_json("alert_data", alert_schema))

    # Build rule_pairs transiently (NOT selected into gold)
    g = (
        g
        .withColumn(
            "rule_pairs",
            F.flatten(
                F.expr("""
                  transform(
                    alert_data_obj.tadpResult.typologyResult,
                    t -> transform(
                      t.ruleResults,
                      r -> named_struct('rule_id', r.id, 'weight', cast(r.wght as long))
                    )
                  )
                """)
            )
        )
        .withColumn("rule_pairs", F.expr("filter(rule_pairs, x -> x.rule_id is not null)"))
        .withColumn(
            "rule_pairs",
            F.expr("""
              aggregate(
                rule_pairs,
                cast(array() as array<struct<rule_id:string, weight:bigint>>),
                (acc, x) ->
                  IF(
                    array_contains(transform(acc, y -> y.rule_id), x.rule_id),
                    acc,
                    concat(acc, array(x))
                  )
              )
            """)
        )
        .withColumn("rule_weights", F.expr("transform(rule_pairs, x -> x.weight)"))
    )

    # Scalar rule metrics
    g = (
        g
        .withColumn("rule_id_count_distinct",
                    F.size(F.array_distinct(F.expr("transform(rule_pairs, x -> x.rule_id)"))).cast("int"))
        .withColumn(
            "rule_weight_sum",
            F.expr("aggregate(rule_weights, cast(0 as long), (acc,x) -> acc + coalesce(x, cast(0 as long)))").cast("long")
        )
        .withColumn(
            "rule_weight_max",
            F.when(F.size("rule_weights") > 0, F.array_max("rule_weights")).otherwise(F.lit(0)).cast("long")
        )
        .withColumn(
            "rule_weight_min",
            F.when(F.size("rule_weights") > 0, F.array_min("rule_weights")).otherwise(F.lit(0)).cast("long")
        )
        .withColumn(
            "rule_weight_avg",
            F.when(F.size("rule_weights") > 0,
                   (F.col("rule_weight_sum").cast("double") / F.size("rule_weights").cast("double"))
                  ).otherwise(F.lit(0.0)).cast("double")
        )
        # p95 per-row from array (no percentile_approx on arrays)
        .withColumn(
            "rule_weight_p95",
            F.when(
                F.size("rule_weights") > 0,
                F.expr("""
                  element_at(
                    array_sort(rule_weights),
                    cast(ceil(size(rule_weights) * 0.95) as int)
                  )
                """).cast("double")
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "top_rule_id",
            F.expr("""
              element_at(
                transform(
                  filter(rule_pairs, x -> x.weight = rule_weight_max),
                  x -> x.rule_id
                ),
                1
              )
            """)
        )
        .withColumn("top_rule_weight", F.col("rule_weight_max").cast("long"))
    )

    # Best-effort tx amount from transaction JSON (may be null for pacs.002)
    g = (
        g
        .withColumn(
            "tx_amount",
            F.coalesce(
                F.get_json_object(F.col("transaction"), "$.FIToFIPmtSts.TxInfAndSts.OrgnlTxRef.Amt.InstdAmt.Amt").cast("double"),
                F.get_json_object(F.col("transaction"), "$.FIToFIPmtSts.TxInfAndSts.OrgnlTxRef.Amt.EqvtAmt.Amt").cast("double"),
                F.lit(None).cast("double")
            )
        )
        .withColumn(
            "tx_ccy",
            F.coalesce(
                F.get_json_object(F.col("transaction"), "$.FIToFIPmtSts.TxInfAndSts.OrgnlTxRef.Amt.InstdAmt.Ccy"),
                F.get_json_object(F.col("transaction"), "$.FIToFIPmtSts.TxInfAndSts.OrgnlTxRef.Amt.EqvtAmt.Ccy"),
                F.lit(None).cast("string")
            )
        )
    )

    # Charge totals (array -> scalar)
    g = (
        g
        .withColumn(
            "charge_total_amount",
            F.when(
                F.col("charge_amounts").isNotNull(),
                F.expr("aggregate(charge_amounts, cast(0.0 as double), (acc,x) -> acc + coalesce(x, 0.0))")
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "charge_currency_count",
            F.when(F.col("charge_ccys").isNotNull(), F.size(F.array_distinct("charge_ccys"))).otherwise(F.lit(0))
        )
        .withColumn("has_multi_currency_charges", (F.col("charge_currency_count") > 1).cast("int"))
    )

    # Processing / latency (ensure non-null long)
    g = (
        g
        .withColumn(
            "total_processing_time_ms",
            (
                F.coalesce(F.col("processing_time_dp").cast("long"), F.lit(0)) +
                F.coalesce(F.col("processing_time_ed").cast("long"), F.lit(0)) +
                F.coalesce(F.col("tadp_processing_time").cast("long"), F.lit(0))
            ).cast("long")
        )
        .withColumn(
            "event_to_ingest_ms",
            F.when(
                F.col("event_ts").isNotNull(),
                (F.col("created_at_ts").cast("long") - F.col("event_ts").cast("long")) * 1000
            ).otherwise(F.lit(None).cast("long"))
        )
    )

    # Normalize dims + governance
    g = (
        g
        .withColumn("priority_norm", F.upper("priority"))
        .withColumn("alert_type_norm", F.upper("alert_type"))
        .withColumn("prediction_outcome_norm", F.upper("prediction_outcome"))
        .withColumn("security_tag", F.concat(F.lit("TENANT:"), F.col("tenant_id")))
    )

    # FINAL GOLD (scalar-only)
    gold = g.select(
        "alert_id","case_id","tenant_id",
        "priority_norm","priority_score",
        "alert_type_norm","prediction_outcome_norm",
        "source","txtp",
        "event_ts","created_at_ts","event_date",
        "alert_status","evaluation_id",
        "tx_type","tx_msg_id","tx_status","tx_amount","tx_ccy",
        "typology_count","rule_count_total",
        "rule_id_count_distinct","rule_weight_sum","rule_weight_max","rule_weight_min","rule_weight_avg","rule_weight_p95",
        "top_rule_id","top_rule_weight",
        "charge_count","charge_total_amount","charge_currency_count","has_multi_currency_charges",
        "network_message_count",
        "event_to_ingest_ms","total_processing_time_ms",
        "security_tag","source_file_path","record_hash"
    )

    # Validate no arrays/structs leaked
    bad = [c for c,t in gold.dtypes if t.startswith("array") or t.startswith("struct")]
    if bad:
        raise RuntimeError(f"Gold still contains non-scalar columns: {bad}")

    # Hudi write options (partitioned by event_date) -> SimpleKeyGenerator
    hudi_gold_opts = {
        "hoodie.table.name": "alerts",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": "alert_id",
        "hoodie.datasource.write.precombine.field": "created_at_ts",
        "hoodie.datasource.write.partitionpath.field": "event_date",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.SimpleKeyGenerator",
        "hoodie.datasource.write.hive_style_partitioning": "true",

        # schema evolution + reconcile
        "hoodie.datasource.write.schema.evolution.enable": "true",
        "hoodie.datasource.read.schema.evolution.enable": "true",
        "hoodie.datasource.write.reconcile.schema": "true",
        "hoodie.schema.on.read.enable": "true",

        # avoid NPE
        "hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
        "hoodie.metadata.enable": "false",
    }

    (
        gold.write.format("hudi")
        .options(**hudi_gold_opts)
        .mode("append")
        .save(alerts_gold_path)
    )

    return gold


# ============================================================
# Orchestrator: run full pipeline
# ============================================================

def run_alerts_pipeline(jsonl_path: str):
    print("✓ Step 1/3: JSONL -> Bronze")
    bronze_df = jsonl_to_bronze_alerts(jsonl_path=jsonl_path, source_file_path=f"file://{jsonl_path}")
    print("   Bronze rows:", bronze_df.count())

    print("✓ Step 2/3: Bronze -> Silver")
    silver_df = bronze_to_silver_alerts()
    print("   Silver rows:", silver_df.count())

    print("✓ Step 3/3: Silver -> Gold (scalar-only)")
    gold_df = silver_to_gold_alerts_scalar_only()
    print("   Gold rows:", gold_df.count())

    print("\n✓ Pipeline completed successfully.")
    return bronze_df, silver_df, gold_df


# ============================================================
# Helper: Query Hudi data (Gold registry)
# ============================================================
def get_hudi_data(table_name: str, filters: dict = None, columns: list = None, limit: int = None):
    spark = get_spark_session()
    if table_name not in GOLD_PATHS:
        raise ValueError(f"Table {table_name} not found in Gold registry")

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


# ============================================================
# API ENDPOINTS
# ============================================================

@app.get("/")
def read_root():
    return {
        "status": "online",
        "message": "Ozone Alerts Pipeline API",
        "warehouse_root": WAREHOUSE_ROOT,
        "endpoints": [
            "/tables",
            "/query",
            "/execute_sql",
            "/json_to_hudi_pipeline"
        ]
    }

@app.get("/tables")
def list_tables():
    """Returns a list of all available tables in the Gold Layer."""
    return {"available_tables": list(GOLD_PATHS.keys())}

@app.post("/query", status_code=status.HTTP_200_OK)
async def query_table(request: QueryRequest):
    """
    Query a specific gold table with filters, column selection, and limits.
    """
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
        print(ve)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"status": "error", "code": 404, "message": str(ve)[:30], "error_type": "ValueError"}
        )

    except Exception as e:
        print(str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
             detail={"status": "error", "code": 500, "message": "Internal server error", "error_details": str(e)[:30]}
        )
import re
@app.post("/execute_sql", status_code=status.HTTP_200_OK)
async def execute_sql(request: SQLQueryRequest):
    spark = get_spark_session()
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


@app.post("/json_to_hudi_pipeline", status_code=status.HTTP_201_CREATED)
async def json_to_hudi_pipeline(request: JSONToHudiRequest):
    """
    Ingest JSON payload -> Bronze -> Silver -> Gold (all automatic).
    """
    temp_file = None
    try:
        # Parse the payload as JSON
        try:
            payload_data = json.loads(request.payload)
        except json.JSONDecodeError as e:
            print(str(e))
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "status": "error",
                    "code": 400,
                    "message": f"Invalid JSON payload"
                }
            )

        # Write payload to temporary JSONL file
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.jsonl')
        temp_file.write(json.dumps(payload_data) + '\n')
        temp_file.close()

        # Step 1: Bronze
        print(f"[1/3] Writing Bronze from payload")
        bronze_df = jsonl_to_bronze_alerts(jsonl_path=temp_file.name, source_file_path="api_ingestion")
        bronze_count = bronze_df.count()

        silver_count = None
        gold_count = None

        # Step 2: Silver
        if request.run_silver:
            print("[2/3] Writing Silver from Bronze")
            silver_df = bronze_to_silver_alerts()
            silver_count = silver_df.count()

        # Step 3: Gold
        if request.run_gold and request.run_silver:
            print("[3/3] Writing Gold from Silver")
            gold_df = silver_to_gold_alerts_scalar_only()
            gold_count = gold_df.count()

        return {
            "status": "success",
            "code": 201,
            "message": "Pipeline executed successfully",
            "bronze_count": bronze_count,
            "silver_count": silver_count,
            "gold_count": gold_count,
            "alert_id": payload_data.get("alert_id"),
            "priority": payload_data.get("priority")
        }

    except HTTPException:
        raise

    except Exception as e:
        error_msg = str(e)
        print(error_msg)
        # Check for schema-related errors
        if "INVALID_EXTRACT_BASE_FIELD_TYPE" in error_msg or "Can't extract a value from" in error_msg:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "status": "error",
                    "code": 400,
                    "message": "Schema inference failed: Empty arrays detected in JSON payload"
                }
            )
        
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "status": "error",
                "code": 500,
                "message": f"Pipeline execution failed"
            }
        )
    
    finally:
        # Clean up temporary file
        if temp_file and os.path.exists(temp_file.name):
            try:
                os.unlink(temp_file.name)
            except:
                pass



# ============================================================
# Run server
# ============================================================
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001, loop="asyncio")