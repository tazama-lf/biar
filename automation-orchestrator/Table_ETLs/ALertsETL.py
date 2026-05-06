"""
alerts.py
---------
Bronze → Silver → Gold ETL for the Alerts domain.
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .BaseETL import BaseETL


class AlertsETL(BaseETL):
    """Full Bronze → Silver → Gold pipeline for raw alert JSON payloads."""

    # ------------------------------------------------------------------
    # PATH HELPERS
    # ------------------------------------------------------------------

    @property
    def bronze_path(self) -> str:
        return f"{self.warehouse_root}/bronze/alerts"

    @property
    def silver_path(self) -> str:
        return f"{self.warehouse_root}/silver/alerts"

    @property
    def gold_path(self) -> str:
        return f"{self.warehouse_root}/gold/alerts"

    @property
    def dlq_path(self) -> str:
        return f"{self.warehouse_root}/silver/alerts_dlq"

    # ------------------------------------------------------------------
    # BRONZE
    # ------------------------------------------------------------------

    def bronze(self, source_path: str) -> str:
        """Ingest raw alert JSON → typed bronze Hudi table."""
        raw = self.spark.read.json(source_path)

        cast = raw.select(
            F.col("alert_id").cast("long").alias("alert_id"),
            F.col("tenant_id").cast("string").alias("tenant_id"),
            F.col("priority").cast("string").alias("priority"),
            F.col("priority_score").cast("double").alias("priority_score"),
            F.col("alert_type").cast("string").alias("alert_type"),
            F.col("prediction_outcome").cast("string").alias("prediction_outcome"),
            F.col("source").cast("string").alias("source"),
            F.col("txtp").cast("string").alias("txtp"),
            F.col("message").cast("string").alias("message"),
            F.col("alert_data").cast("string").alias("alert_data"),
            F.col("transaction").cast("string").alias("transaction"),
            F.col("network_map").cast("string").alias("network_map"),
            F.col("confidence_per").cast("int").alias("confidence_per"),
            F.col("case_id").cast("long").alias("case_id"),
            F.col("created_at").cast("string").alias("created_at"),
        )

        df = (
            cast.withColumn("created_at_ts", F.current_timestamp())
            .withColumn("source_file_path", F.input_file_name())
        )

        hash_cols = [c for c in df.columns if c != "created_at_ts"]
        df = df.withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in hash_cols],
                ),
                256,
            ),
        )

        self.write_hudi(
            df,
            self.bronze_path,
            self.hudi_opts("bronze_alerts", "alert_id", "created_at_ts"),
        )
        print(f"[AlertsETL] Bronze written → {self.bronze_path}")
        return self.bronze_path

    # ------------------------------------------------------------------
    # SILVER
    # ------------------------------------------------------------------

    def silver(self) -> str:
        """Parse nested JSON blobs, apply DQ rules, write silver + DLQ."""
        bronze = self.spark.read.format("hudi").load(self.bronze_path)
        

        alert_schema = self.infer_json_schema(bronze, "alert_data")
        tx_schema    = self.infer_json_schema(bronze, "transaction")
        net_schema   = self.infer_json_schema(bronze, "network_map")

        b = (
            bronze
            .withColumn("alert_data_obj",  F.from_json("alert_data",  alert_schema))
            .withColumn("transaction_obj", F.from_json("transaction",  tx_schema))
            .withColumn("network_map_obj", F.from_json("network_map",  net_schema))
            .withColumn("event_ts",        F.to_timestamp(F.col("alert_data_obj.timestamp")))
            .withColumn("event_date",      F.to_date("event_ts"))
            .withColumn("tx_created_ts",   F.to_timestamp(F.col("transaction_obj.FIToFIPmtSts.GrpHdr.CreDtTm")))
            .withColumn("tx_accept_ts",    F.to_timestamp(F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.AccptncDtTm")))
        )

        silver = self._flatten_silver(b)
        silver = self.drop_hoodie_cols(silver)

        # Deduplicate – keep latest per alert_id
        w = Window.partitionBy("alert_id").orderBy(F.col("created_at_ts").desc())
        silver = silver.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

        # DQ split
        silver_pass, silver_fail = self._apply_dq(silver)

        silver_fail = (
            silver_fail
            .withColumn(
                "dlq_id",
                F.sha2(
                    F.coalesce(
                        F.col("evaluation_id").cast("string"),
                        F.col("tx_type").cast("string"),
                    ),
                    256,
                ),
            )
            .withColumn("dlq_ingested_at", F.current_timestamp())
        )

        self.write_hudi(
            silver_fail,
            self.dlq_path,
            self.hudi_opts("silver_alerts_dlq", "dlq_id", "dlq_ingested_at"),
        )

        self.write_hudi(
            silver_pass,
            self.silver_path,
            self.hudi_opts("silver_alerts", "alert_id", "created_at_ts"),
        )
        print(f"[AlertsETL] Silver written → {self.silver_path}")
        return self.silver_path

    def _flatten_silver(self, b):
        """Extract every field needed at the silver layer."""
        rule_pairs_expr = ("""transform(alert_data_obj.tadpResult.typologyResult, t -> transform(t.ruleResults, r -> named_struct('rule_id', r.id, 'weight', cast(r.wght as long))))"""  )
        return (
            b
            .withColumn("alert_id",              F.col("alert_id").cast("long"))
            .withColumn("case_id",               F.col("case_id").cast("long"))
            .withColumn("alert_status",          F.col("alert_data_obj.status"))
            .withColumn("evaluation_id",         F.col("alert_data_obj.evaluationID"))
            .withColumn("processing_time_dp",    F.col("alert_data_obj.metaData.prcgTmDP").cast("long"))
            .withColumn("processing_time_ed",    F.col("alert_data_obj.metaData.prcgTmED").cast("long"))
            .withColumn("tadp_id",               F.col("alert_data_obj.tadpResult.id"))
            .withColumn("tadp_cfg",              F.col("alert_data_obj.tadpResult.cfg"))
            .withColumn("tadp_processing_time",  F.col("alert_data_obj.tadpResult.prcgTm").cast("long"))
            .withColumn("typology_count",        F.size(F.col("alert_data_obj.tadpResult.typologyResult")))
            .withColumn("typology_ids",          F.expr("transform(alert_data_obj.tadpResult.typologyResult, x -> x.id)"))
            .withColumn("typology_results",      F.expr("transform(alert_data_obj.tadpResult.typologyResult, x -> cast(x.result as int))"))
            .withColumn("typology_reviews",      F.expr("transform(alert_data_obj.tadpResult.typologyResult, x -> cast(x.review as boolean))"))
            .withColumn("workflow_processors",   F.expr("transform(alert_data_obj.tadpResult.typologyResult, x -> x.workflow.flowProcessor)"))
            .withColumn("alert_thresholds",      F.expr("transform(alert_data_obj.tadpResult.typologyResult, x -> cast(x.workflow.alertThreshold as int))"))
            .withColumn("interdiction_thresholds", F.expr("transform(alert_data_obj.tadpResult.typologyResult, x -> cast(x.workflow.interdictionThreshold as int))"))
            .withColumn("rule_count_total",      F.expr("aggregate(alert_data_obj.tadpResult.typologyResult, 0, (acc, x) -> acc + size(x.ruleResults))"))
            # rule_pairs (unique by rule_id)
            .withColumn("rule_pairs", F.flatten(F.expr(rule_pairs_expr)))
            .withColumn("rule_pairs", F.expr("aggregate(rule_pairs, cast(array() as array<struct<rule_id:string, weight:bigint>>), (acc, x) -> IF(array_contains(transform(acc, y -> y.rule_id), x.rule_id), acc, concat(acc, array(x))))"))
            .withColumn("rule_pairs", F.expr("filter(rule_pairs, x -> x.rule_id is not null)"))
            .withColumn("rule_weights_json",     F.to_json(F.col("rule_pairs")))
            .withColumn("rule_id_count_distinct", F.size(F.expr("transform(rule_pairs, x -> x.rule_id)")))
            .withColumn("rule_weight_sum",       F.expr("aggregate(transform(rule_pairs, x -> x.weight), cast(0 as long), (acc,x) -> acc + coalesce(x, cast(0 as long)))"))
            .withColumn("rule_weight_max",       F.when(F.size("rule_pairs") > 0, F.array_max(F.expr("transform(rule_pairs, x -> x.weight)"))).otherwise(F.lit(0).cast("long")))
            # transaction + network flattening
            .withColumn("tx_type",               F.col("transaction_obj.TxTp"))
            .withColumn("tx_tenant_id",          F.col("transaction_obj.TenantId"))
            .withColumn("tx_msg_id",             F.col("transaction_obj.FIToFIPmtSts.GrpHdr.MsgId"))
            .withColumn("tx_status",             F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.TxSts"))
            .withColumn("tx_original_instr_id",  F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.OrgnlInstrId"))
            .withColumn("tx_original_e2e_id",    F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.OrgnlEndToEndId"))
            .withColumn("instg_mmb_id",          F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.InstgAgt.FinInstnId.ClrSysMmbId.MmbId"))
            .withColumn("instd_mmb_id",          F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.InstdAgt.FinInstnId.ClrSysMmbId.MmbId"))
            .withColumn("charge_count",          F.size(F.col("transaction_obj.FIToFIPmtSts.TxInfAndSts.ChrgsInf")))
            .withColumn("charge_agent_mmb_ids",  F.expr("transform(transaction_obj.FIToFIPmtSts.TxInfAndSts.ChrgsInf, x -> x.Agt.FinInstnId.ClrSysMmbId.MmbId)"))
            .withColumn("charge_amounts",        F.expr("transform(transaction_obj.FIToFIPmtSts.TxInfAndSts.ChrgsInf, x -> cast(x.Amt.Amt as double))"))
            .withColumn("charge_ccys",           F.expr("transform(transaction_obj.FIToFIPmtSts.TxInfAndSts.ChrgsInf, x -> x.Amt.Ccy)"))
            .withColumn("network_cfg",           F.col("network_map_obj.cfg"))
            .withColumn("network_active",        F.col("network_map_obj.active").cast("boolean"))
            .withColumn("network_tenant_id",     F.col("network_map_obj.tenantId"))
            .withColumn("network_message_count", F.size(F.col("network_map_obj.messages")))
            .withColumn("network_message_ids",   F.expr("transform(network_map_obj.messages, x -> x.id)"))
            .select(
                "_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key",
                "_hoodie_partition_path", "_hoodie_file_name",
                "alert_id", "case_id", "tenant_id", "priority", "priority_score",
                "alert_type", "prediction_outcome", "source", "txtp", "message", "confidence_per",
                "event_ts", "event_date", "tx_created_ts", "tx_accept_ts", "created_at", "created_at_ts",
                "alert_status", "evaluation_id", "processing_time_dp", "processing_time_ed",
                "tadp_id", "tadp_cfg", "tadp_processing_time",
                "typology_count", "typology_ids", "typology_results", "typology_reviews",
                "workflow_processors", "alert_thresholds", "interdiction_thresholds", "rule_count_total",
                "rule_weights_json", "rule_id_count_distinct", "rule_weight_sum", "rule_weight_max",
                "tx_type", "tx_tenant_id", "tx_msg_id", "tx_status", "tx_original_instr_id",
                "tx_original_e2e_id", "instg_mmb_id", "instd_mmb_id",
                "charge_count", "charge_agent_mmb_ids", "charge_amounts", "charge_ccys",
                "network_cfg", "network_active", "network_tenant_id",
                "network_message_count", "network_message_ids",
                "source_file_path", "alert_data", "transaction", "network_map",
            )
            .drop("rule_pairs")
        )

    def _apply_dq(self, silver):
        """Split silver into passing records and DLQ failures."""
        dq_rules = [
            ("ALERT_ID_NULL",       F.col("alert_id").isNull()),
            ("CREATED_AT_TS_NULL",  F.col("created_at_ts").isNull()),
            ("EVENT_TS_NULL",       F.col("event_ts").isNull()),
            ("ALERT_DATA_MISSING_CORE", F.col("alert_data").isNotNull() & F.col("alert_status").isNull()),
            ("TX_MISSING_CORE",     F.col("transaction").isNotNull() & F.col("tx_msg_id").isNull()),
            ("NET_MISSING_CORE",    F.col("network_map").isNotNull() & F.col("network_cfg").isNull()),
        ]
        reason_cols = [
            F.when(cond, F.lit(code)).otherwise(F.lit(None).cast("string"))
            for code, cond in dq_rules
        ]
        dq = (
            silver
            .withColumn("dq_reason_codes_raw", F.array(*reason_cols))
            .withColumn("dq_reason_codes", F.expr("filter(dq_reason_codes_raw, x -> x is not null)"))
            .withColumn("dq_failed", F.size("dq_reason_codes") > 0)
            .drop("dq_reason_codes_raw")
        )
        passing = dq.filter(~F.col("dq_failed")).drop("dq_failed", "dq_reason_codes")
        failing = dq.filter(F.col("dq_failed"))
        return passing, failing

    # ------------------------------------------------------------------
    # GOLD
    # ------------------------------------------------------------------

    def gold(self) -> str:
        """Build the analytics-ready gold alert table."""
        silver = self.spark.read.format("hudi").load(self.silver_path)
        silver = self.drop_hoodie_cols(silver)

        w = Window.partitionBy("alert_id").orderBy(F.col("created_at_ts").desc())
        s = silver.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

        if "record_hash" not in s.columns:
            s = s.withColumn(
                "record_hash",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.coalesce(F.col("alert_id").cast("string"), F.lit("")),
                        F.coalesce(F.col("tenant_id").cast("string"), F.lit("")),
                        F.coalesce(F.col("created_at_ts").cast("string"), F.lit("")),
                    ),
                    256,
                ),
            )

        alert_schema = self.infer_json_schema(
            s.select("alert_data").where(F.col("alert_data").isNotNull()), "alert_data"
        )
        g = s.withColumn("alert_data_obj", F.from_json("alert_data", alert_schema))

        g = (
            g
            .withColumn("rule_pairs", F.flatten(F.expr(
                "transform(alert_data_obj.tadpResult.typologyResult, "
                "  t -> transform(t.ruleResults, r -> named_struct("
                "    'rule_id', r.id, 'weight', cast(r.wght as long)"
                "  ))"
                ")"
            )))
            .withColumn("rule_pairs", F.expr("filter(rule_pairs, x -> x.rule_id is not null)"))
            .withColumn("rule_weights",           F.expr("transform(rule_pairs, x -> x.weight)"))
            .withColumn("rule_id_count_distinct", F.size(F.array_distinct(F.expr("transform(rule_pairs, x -> x.rule_id)"))).cast("int"))
            .withColumn("rule_weight_sum",        F.expr("aggregate(rule_weights, cast(0 as long), (acc,x) -> acc + coalesce(x, cast(0 as long)))").cast("long"))
            .withColumn("rule_weight_max",        F.when(F.size("rule_weights") > 0, F.array_max("rule_weights")).otherwise(F.lit(0)).cast("long"))
            .withColumn("rule_weight_min",        F.when(F.size("rule_weights") > 0, F.array_min("rule_weights")).otherwise(F.lit(0)).cast("long"))
            .withColumn("rule_weight_avg",        F.when(F.size("rule_weights") > 0, (F.col("rule_weight_sum").cast("double") / F.size("rule_weights").cast("double"))).otherwise(F.lit(0.0)).cast("double"))
            .withColumn("rule_weight_p95",        F.when(F.size("rule_weights") > 0, F.expr("element_at(array_sort(rule_weights), cast(ceil(size(rule_weights) * 0.95) as int))").cast("double")).otherwise(F.lit(0.0)))
            .withColumn("top_rule_id",            F.expr("element_at(transform(filter(rule_pairs, x -> x.weight = rule_weight_max), x -> x.rule_id), 1)"))
            .withColumn("top_rule_weight",        F.col("rule_weight_max").cast("long"))
        )

        tx_amount = F.coalesce(
            F.get_json_object(F.col("transaction"), "$.FIToFIPmtSts.TxInfAndSts.OrgnlTxRef.Amt.InstdAmt.Amt").cast("double"),
            F.get_json_object(F.col("transaction"), "$.FIToFIPmtSts.TxInfAndSts.OrgnlTxRef.Amt.EqvtAmt.Amt").cast("double"),
        )
        tx_ccy = F.coalesce(
            F.get_json_object(F.col("transaction"), "$.FIToFIPmtSts.TxInfAndSts.OrgnlTxRef.Amt.InstdAmt.Ccy"),
            F.get_json_object(F.col("transaction"), "$.FIToFIPmtSts.TxInfAndSts.OrgnlTxRef.Amt.EqvtAmt.Ccy"),
        )

        g = (
            g
            .withColumn("charge_total_amount",   F.when(F.col("charge_amounts").isNotNull(), F.expr("aggregate(charge_amounts, cast(0.0 as double), (acc,x) -> acc + coalesce(x, 0.0))")).otherwise(F.lit(0.0)))
            .withColumn("tx_amount",             tx_amount)
            .withColumn("tx_ccy",                tx_ccy)
            .withColumn("charge_currency_count", F.when(F.col("charge_ccys").isNotNull(), F.size(F.array_distinct("charge_ccys"))).otherwise(F.lit(0)))
            .withColumn("has_multi_currency_charges", (F.col("charge_currency_count") > 1).cast("int"))
            .withColumn("total_processing_time_ms", (
                F.coalesce(F.col("processing_time_dp").cast("long"), F.lit(0))
                + F.coalesce(F.col("processing_time_ed").cast("long"), F.lit(0))
                + F.coalesce(F.col("tadp_processing_time").cast("long"), F.lit(0))
            ).cast("long"))
            .withColumn("event_to_ingest_ms",    F.when(F.col("event_ts").isNotNull(), (F.col("created_at_ts").cast("long") - F.col("event_ts").cast("long")) * 1000).otherwise(F.lit(None).cast("long")))
            .withColumn("priority_norm",          F.upper("priority"))
            .withColumn("alert_type_norm",        F.upper("alert_type"))
            .withColumn("prediction_outcome_norm", F.upper("prediction_outcome"))
            .withColumn("is_false_positive",      (F.col("prediction_outcome_norm") == "FALSE_POSITIVE").cast("int"))
            .withColumn("is_false_negative",      (F.col("prediction_outcome_norm") == "FALSE_NEGATIVE").cast("int"))
            .withColumn("is_true_positive",       (F.col("prediction_outcome_norm") == "TRUE_POSITIVE").cast("int"))
            .withColumn("is_true_negative",       (F.col("prediction_outcome_norm") == "TRUE_NEGATIVE").cast("int"))
            .withColumn("security_tag",           F.concat(F.lit("TENANT:"), F.col("tenant_id")))
            .withColumn("typology_id",            F.concat_ws(", ", F.col("typology_ids")))
        )

        gold = g.select(
            "event_date", "alert_id", "case_id", "tenant_id", "priority_norm", "priority_score",
            "alert_type_norm", "prediction_outcome_norm", "source", "txtp", "event_ts", "created_at_ts",
            "alert_status", "evaluation_id", "tx_type", "tx_msg_id", "tx_status", "tx_amount", "tx_ccy",
            "tx_original_e2e_id", "typology_count", "typology_id", "rule_count_total",
            "rule_id_count_distinct", "rule_weight_sum", "rule_weight_max", "rule_weight_min",
            "rule_weight_avg", "rule_weight_p95", "top_rule_id", "top_rule_weight",
            "charge_count", "charge_total_amount", "charge_currency_count", "has_multi_currency_charges",
            "network_message_count", "event_to_ingest_ms", "total_processing_time_ms", "security_tag",
            "source_file_path", "record_hash",
        )

        gold_opts = {
            **self.hudi_opts("alerts", "alert_id", "created_at_ts", partition="event_date"),
            "hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
        }
        self.write_hudi(gold, self.gold_path, gold_opts)
        print(f"[AlertsETL] Gold written → {self.gold_path}")
        return self.gold_path

    # ------------------------------------------------------------------
    # ORCHESTRATOR
    # ------------------------------------------------------------------

    def run(self, source_path: str) -> str:
        print(f"[AlertsETL] Starting Bronze → Silver → Gold from {source_path}")
        self.bronze(source_path)
        self.silver()
        self.gold()
        print("[AlertsETL] ETL complete.")
        return self.gold_path