from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .BaseETL import BaseETL


class AlertNavigatorETL(BaseETL):
    """
    Builds denormalized Alert Navigator views from silver alerts,
    transactions gold, typologies bronze, and network_map bronze.
    """

    def __init__(self, spark, warehouse_root: str) -> None:
        super().__init__(spark, warehouse_root)
        self.views_root = f"{self.warehouse_root}/views"
        self.alert_nav_root = f"{self.views_root}/alert_navigator"

    @property
    def bronze_path(self) -> str:
        return self.alert_nav_root

    @property
    def silver_path(self) -> str:
        return self.alert_nav_root

    @property
    def gold_path(self) -> str:
        return self.alert_nav_root

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _hudi_anv_opts(
        self,
        table_name: str,
        record_key: str,
        precombine: str,
        partition: str | None = None,
    ) -> dict:
        """Hudi options tuned for Alert Navigator view tables."""
        o = {
            "hoodie.table.name": table_name,
            "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.datasource.write.recordkey.field": record_key,
            "hoodie.datasource.write.precombine.field": precombine,
            # schema evolution
            "hoodie.datasource.write.schema.evolution.enable": "true",
            "hoodie.datasource.read.schema.evolution.enable": "true",
            "hoodie.datasource.write.reconcile.schema": "true",
            "hoodie.schema.on.read.enable": "true",
            "hoodie.metadata.enable": "false",
        }
        if partition:
            o.update(
                {
                    "hoodie.datasource.write.partitionpath.field": partition,
                    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.SimpleKeyGenerator",
                    "hoodie.datasource.write.hive_style_partitioning": "true",
                }
            )
        else:
            o["hoodie.datasource.write.keygenerator.class"] = (
                "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
            )
        return o

    def _safe_load(self, path: str, select_expr: list | None = None) -> DataFrame | None:
        """Attempt to load a Hudi table; return None if it does not exist."""
        try:
            df = self.spark.read.format("hudi").load(path)
            if select_expr:
                df = df.select(*select_expr)
            return df
        except Exception:
            return None

    # ------------------------------------------------------------------
    # BUILDERS
    # ------------------------------------------------------------------

    def _build_header(self, a: DataFrame, g_tx: DataFrame | None) -> DataFrame:
        """Build the alerts_nav_header view."""
        header = (
            a.select(
                F.col("alert_id").cast("long").alias("alert_id"),
                F.col("tenant_id").cast("string").alias("tenant_id"),
                F.col("case_id").cast("long").alias("case_id"),
                F.col("tx_msg_id").cast("string").alias("tx_msg_id"),
                F.col("tx_type").cast("string").alias("tx_type"),
                F.trim(F.col("tx_original_e2e_id").cast("string")).alias("_payment_bridge_e2e"),
                F.col("event_ts").cast("timestamp").alias("alert_timestamp"),
                F.to_date("event_ts").alias("alert_date"),
                F.col("message").cast("string").alias("alert_reason"),
                F.col("alert_type").cast("string").alias("alert_type"),
                F.col("prediction_outcome").cast("string").alias("prediction_outcome"),
                F.col("priority").cast("string").alias("priority"),
                F.col("priority_score").cast("double").alias("priority_score"),
                F.col("alert_data_obj.evaluationID").alias("evaluation_id"),
                F.col("alert_data_obj.status").alias("alert_status"),
                F.col("created_at_ts").cast("timestamp").alias("ingested_at_ts"),
            )
            .withColumn("pk", F.col("alert_id").cast("string"))
        )

        if g_tx is not None:
            # Alert-triggering transaction row. Status now comes from Ozone txsts.
            tx_by_msg = g_tx.select(
                F.trim(F.col("tx_msg_id").cast("string")).alias("tx_msg_id"),
                F.trim(F.col("end_to_end_id").cast("string")).alias("msg_end_to_end_id"),
                F.trim(F.col("transaction_id").cast("string")).alias("msg_transaction_id"),
                F.col("tx_status").cast("string").alias("msg_tx_status"),
                F.col("tx_amount").cast("decimal(18,2)").alias("msg_tx_amount"),
                F.col("tx_ccy").cast("string").alias("msg_tx_ccy"),
            ).dropDuplicates(["tx_msg_id"])

            # Payment row for amount/currency enrichment by original/end-to-end id.
            tx_by_e2e = g_tx.filter(
                (F.col("tx_type").isin("pacs.008.001.10", "pain.001.001.11"))
                | F.col("tx_amount").isNotNull()
            ).select(
                F.trim(F.col("end_to_end_id").cast("string")).alias("lookup_end_to_end_id"),
                F.trim(F.col("transaction_id").cast("string")).alias("e2e_transaction_id"),
                F.col("tx_amount").cast("decimal(18,2)").alias("e2e_tx_amount"),
                F.col("tx_ccy").cast("string").alias("e2e_tx_ccy"),
            ).dropDuplicates(["lookup_end_to_end_id"])

            header = (
                header.join(tx_by_msg, on="tx_msg_id", how="left")
                .withColumn(
                    "_payment_bridge_e2e",
                    F.coalesce(F.col("_payment_bridge_e2e"), F.col("msg_end_to_end_id")),
                )
                .join(
                    tx_by_e2e,
                    F.col("_payment_bridge_e2e") == F.col("lookup_end_to_end_id"),
                    "left",
                )
                .withColumn("transaction_status", F.col("msg_tx_status"))
                .withColumn(
                    "transaction_id",
                    F.coalesce(F.col("msg_transaction_id"), F.col("e2e_transaction_id")),
                )
                .withColumn(
                    "transaction_amount",
                    F.coalesce(F.col("msg_tx_amount"), F.col("e2e_tx_amount")),
                )
                .withColumn(
                    "transaction_currency",
                    F.coalesce(F.col("msg_tx_ccy"), F.col("e2e_tx_ccy")),
                )
                .withColumn(
                    "block_or_override_status",
                    F.when(
                        F.col("transaction_status").isin("BLOCKED", "REJECTED"),
                        F.lit("BLOCKED_OR_REJECTED"),
                    )
                    .when(
                        F.col("transaction_status").isNotNull(),
                        F.lit("NOT_BLOCKED"),
                    )
                    .otherwise(F.lit(None)),
                )
                .drop(
                    "_payment_bridge_e2e",
                    "msg_end_to_end_id",
                    "msg_transaction_id",
                    "msg_tx_status",
                    "msg_tx_amount",
                    "msg_tx_ccy",
                    "lookup_end_to_end_id",
                    "e2e_transaction_id",
                    "e2e_tx_amount",
                    "e2e_tx_ccy",
                )
            )

        return header.drop("_payment_bridge_e2e")
          
    def _build_typologies(self, a: DataFrame, b_typ: DataFrame | None) -> DataFrame:
        """Build the alerts_nav_typologies view."""
        typ = (
            a.select(
                F.col("alert_id").cast("long").alias("alert_id"),
                F.col("tenant_id").cast("string").alias("tenant_id"),
                F.col("tx_msg_id").cast("string").alias("tx_msg_id"),
                F.col("event_ts").cast("timestamp").alias("alert_timestamp"),
                F.explode_outer(
                    F.col("alert_data_obj.tadpResult.typologyResult")
                ).alias("typology"),
            )
            .select(
                "alert_id",
                "tenant_id",
                "tx_msg_id",
                "alert_timestamp",
                F.col("typology.id").alias("typology_id"),
                F.col("typology.cfg").alias("typology_cfg"),
                F.col("typology.result").cast("long").alias("typology_score"),
                F.col("typology.review").cast("boolean").alias("typology_review"),
                F.col("typology.prcgTm")
                .cast("long")
                .alias("typology_processing_time_ms"),
                F.col("typology.tenantId").alias("typology_tenant_id"),
                F.col("typology.workflow.flowProcessor").alias("flow_processor"),
                F.col("typology.workflow.alertThreshold")
                .cast("long")
                .alias("alert_threshold"),
                F.col("typology.workflow.interdictionThreshold")
                .cast("long")
                .alias("interdiction_threshold"),
                F.size(F.col("typology.ruleResults")).alias("rule_count_in_typology"),
            )
        )

        if b_typ is not None:
            typ = typ.join(
                b_typ,
                (typ.tenant_id == b_typ.cfg_tenant_id)
                & (typ.typology_id == b_typ.cfg_typology_id)
                & (typ.typology_cfg == b_typ.cfg_typology_cfg),
                "left",
            ).drop("cfg_tenant_id", "cfg_typology_id", "cfg_typology_cfg")

        return (
            typ.withColumn(
                "pk",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.col("alert_id"),
                        F.col("typology_id"),
                        F.coalesce(F.col("typology_cfg"), F.lit("")),
                    ),
                    256,
                ),
            )
            .withColumn(
                "alert_timestamp",
                F.coalesce(F.col("alert_timestamp"), F.current_timestamp()),
            )
            .withColumn("ingested_at_ts", F.current_timestamp())
        )

    def _prepare_rule_metadata(self, b_rules: DataFrame | None) -> DataFrame | None:
        """Parse bronze rules into compact metadata for rules triggered by alerts."""
        if b_rules is None:
            return None

        rule_schema = self.infer_json_schema(b_rules, "rule_configuration_json")

        rule_meta = (
            b_rules.withColumn(
                "rule_obj",
                F.from_json("rule_configuration_json", rule_schema),
            )
            .withColumn("bands_arr", F.col("rule_obj.config.bands"))
            .withColumn("exit_conditions_arr", F.col("rule_obj.config.exitConditions"))
            .select(
                F.upper(F.col("cfg_tenant_id")).alias("cfg_tenant_id_norm"),
                F.col("cfg_tenant_id"),
                F.col("cfg_rule_id"),
                F.col("cfg_rule_cfg"),
                F.col("rule_metadata_ingested_at_ts"),
                F.col("rule_obj.desc").cast("string").alias("rule_desc"),
                F.coalesce(F.size("bands_arr"), F.lit(0)).cast("int").alias("band_count"),
                F.coalesce(F.size("exit_conditions_arr"), F.lit(0))
                .cast("int")
                .alias("exit_condition_count"),
                F.to_json(F.expr("transform(bands_arr, x -> x.reason)")).alias(
                    "band_reasons_json"
                ),
                F.to_json(F.expr("transform(bands_arr, x -> x.subRuleRef)")).alias(
                    "band_sub_rule_refs_json"
                ),
                F.to_json(
                    F.expr(
                        """
                        transform(
                            bands_arr,
                            x -> named_struct(
                                'subRuleRef', x.subRuleRef,
                                'reason', x.reason
                            )
                        )
                        """
                    )
                ).alias("band_reasons_with_sub_rule_refs_json"),
                F.to_json(F.expr("transform(exit_conditions_arr, x -> x.reason)")).alias(
                    "exit_condition_reasons_json"
                ),
                F.to_json(
                    F.expr("transform(exit_conditions_arr, x -> x.subRuleRef)")
                ).alias("exit_condition_sub_rule_refs_json"),
                F.to_json(
                    F.expr(
                        """
                        transform(
                            exit_conditions_arr,
                            x -> named_struct(
                                'subRuleRef', x.subRuleRef,
                                'reason', x.reason
                            )
                        )
                        """
                    )
                ).alias("exit_condition_reasons_with_sub_rule_refs_json"),
                F.col("bands_arr"),
                F.col("exit_conditions_arr"),
            )
        )

        w = Window.partitionBy(
            "cfg_tenant_id", "cfg_rule_id", "cfg_rule_cfg"
        ).orderBy(F.col("rule_metadata_ingested_at_ts").desc_nulls_last())

        return (
            rule_meta.withColumn("rule_metadata_rn", F.row_number().over(w))
            .filter(F.col("rule_metadata_rn") == 1)
            .drop("rule_metadata_rn", "rule_metadata_ingested_at_ts")
        )

    def _build_rules(self, a: DataFrame, b_rules: DataFrame | None) -> DataFrame:
        """Build the alerts_nav_rules view."""
        rules = (
            a.select(
                F.col("alert_id").cast("long").alias("alert_id"),
                F.col("tenant_id").cast("string").alias("tenant_id"),
                F.col("tx_msg_id").cast("string").alias("tx_msg_id"),
                F.col("event_ts").cast("timestamp").alias("alert_timestamp"),
                F.explode_outer(
                    F.col("alert_data_obj.tadpResult.typologyResult")
                ).alias("typology"),
            )
            .select(
                "alert_id",
                "tenant_id",
                "tx_msg_id",
                "alert_timestamp",
                F.col("typology.id").alias("typology_id"),
                F.col("typology.cfg").alias("typology_cfg"),
                F.explode_outer(F.col("typology.ruleResults")).alias("rule"),
            )
            .select(
                "alert_id",
                "tenant_id",
                "tx_msg_id",
                "alert_timestamp",
                "typology_id",
                "typology_cfg",
                F.col("rule.id").alias("rule_id"),
                F.col("rule.cfg").alias("rule_cfg"),
                F.col("rule.wght").cast("long").alias("rule_weight"),
                F.col("rule.indpdntVarbl")
                .cast("double")
                .alias("rule_independent_variable"),
                F.col("rule.subRuleRef").alias("rule_sub_ref"),
                F.col("rule.prcgTm").cast("long").alias("rule_processing_time_ms"),
                F.col("rule.tenantId").alias("rule_tenant_id"),
            )
        )

        rules = (
            rules.withColumn(
                "pk",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.col("alert_id"),
                        F.col("typology_id"),
                        F.coalesce(F.col("typology_cfg"), F.lit("")),
                        F.col("rule_id"),
                        F.coalesce(F.col("rule_cfg"), F.lit("")),
                        F.coalesce(F.col("rule_sub_ref"), F.lit("")),
                    ),
                    256,
                ),
            )
            .withColumn(
                "alert_timestamp",
                F.coalesce(F.col("alert_timestamp"), F.current_timestamp()),
            )
            .withColumn("ingested_at_ts", F.current_timestamp())
        )

        rule_meta = self._prepare_rule_metadata(b_rules)
        if rule_meta is None:
            return rules

        rules = (
            rules.withColumn("tenant_id_norm", F.upper(F.col("tenant_id")))
            .join(
                rule_meta,
                (F.col("rule_id") == F.col("cfg_rule_id"))
                & (F.col("rule_cfg") == F.col("cfg_rule_cfg"))
                & (
                    (F.col("tenant_id_norm") == F.col("cfg_tenant_id_norm"))
                    | (F.col("cfg_tenant_id_norm") == F.lit("DEFAULT"))
                ),
                "left",
            )
            .withColumn(
                "rule_metadata_rank",
                F.when(F.col("tenant_id_norm") == F.col("cfg_tenant_id_norm"), F.lit(0))
                .when(F.col("cfg_tenant_id_norm") == F.lit("DEFAULT"), F.lit(1))
                .otherwise(F.lit(2)),
            )
        )

        w = Window.partitionBy("pk").orderBy(F.col("rule_metadata_rank"))

        return (
            rules.withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .withColumn(
                "matched_band_reason",
                F.expr(
                    "element_at(transform(filter(bands_arr, x -> x.subRuleRef = rule_sub_ref), x -> x.reason), 1)"
                ),
            )
            .withColumn(
                "matched_exit_condition_reason",
                F.expr(
                    "element_at(transform(filter(exit_conditions_arr, x -> x.subRuleRef = rule_sub_ref), x -> x.reason), 1)"
                ),
            )
            .withColumn(
                "matched_rule_reason",
                F.coalesce(
                    F.col("matched_band_reason"),
                    F.col("matched_exit_condition_reason"),
                ),
            )
            .drop(
                "rn",
                "tenant_id_norm",
                "cfg_tenant_id_norm",
                "cfg_tenant_id",
                "cfg_rule_id",
                "cfg_rule_cfg",
                "bands_arr",
                "exit_conditions_arr",
                "rule_metadata_rank",
            )
        )

    def _build_network_eval(
        self,
        alerts_nav_header: DataFrame,
        b_net: DataFrame | None,
    ) -> DataFrame | None:
        """Build the alerts_nav_network_evaluated view."""
        if b_net is None:
            return None

        net_schema = self.infer_json_schema(b_net, "network_configuration_json")

        net_parsed = (
            b_net.withColumn("net_obj", F.from_json("network_configuration_json", net_schema))
            .select(
                F.col("cfg_tenant_id").alias("tenant_id"),
                F.col("net_obj.cfg").alias("network_cfg"),
                F.col("net_obj.active").cast("boolean").alias("network_active"),
                F.explode_outer(F.col("net_obj.messages")).alias("msg"),
            )
            .select(
                "tenant_id",
                "network_cfg",
                "network_active",
                F.col("msg.id").alias("network_message_id"),
                F.col("msg.cfg").alias("network_message_cfg"),
                F.col("msg.txTp").alias("network_tx_type"),
                F.explode_outer(F.col("msg.typologies")).alias("t"),
            )
            .select(
                "tenant_id",
                "network_cfg",
                "network_active",
                "network_message_id",
                "network_message_cfg",
                "network_tx_type",
                F.col("t.id").alias("typology_id"),
                F.col("t.cfg").alias("typology_cfg"),              
                F.explode_outer(F.col("t.rules")).alias("r"),
            )
            .select(
                "tenant_id",
                "network_cfg",
                "network_active",
                "network_message_id",
                "network_message_cfg",
                "network_tx_type",
                "typology_id",
                "typology_cfg",
                F.col("r.id").alias("rule_id"),
                F.col("r.cfg").alias("rule_cfg"),
            )
        )

        network_eval = (
            alerts_nav_header.select(
                F.col("alert_id"),
                F.col("tenant_id").alias("header_tenant_id"),
                F.col("tx_type").alias("header_tx_type"),
            )
            .join(
                net_parsed,
                (F.col("header_tenant_id") == net_parsed.tenant_id)
                & (F.col("header_tx_type") == net_parsed.network_tx_type),
                "left",
            )
            .select(
                "alert_id",
                net_parsed.tenant_id,
                F.col("header_tx_type").alias("tx_type"),
                "network_cfg",
                "network_active",
                "network_message_id",
                "typology_id",
                "typology_cfg",
                "rule_id",
                "rule_cfg",
            )
        )

        return network_eval.withColumn(
            "pk",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("alert_id"),
                    F.col("typology_id"),
                    F.col("rule_id"),
                    F.coalesce(F.col("network_message_id"), F.lit("")),
                ),
                256,
            ),
        )

    # ------------------------------------------------------------------
    # BRONZE  (used as the single entry-point for this view builder)
    # ------------------------------------------------------------------

    def bronze(self, source_path: str = "") -> str:
        """
        Build all Alert Navigator views.

        *source_path* is ignored (views are built from existing Hudi tables).
        Returns the root path of the alert navigator views.
        """
        # -- load dependencies -------------------------------------------------
        silver_alerts_path = f"{self.warehouse_root}/silver/alerts"
        transactions_gold_path = f"{self.warehouse_root}/gold/transactions"
        typologies_bronze_path = f"{self.warehouse_root}/bronze/typologies"
        network_map_bronze_path = f"{self.warehouse_root}/bronze/network_map"
        rules_bronze_path = f"{self.warehouse_root}/bronze/rule"

        s_alerts = self.spark.read.format("hudi").load(silver_alerts_path)

        g_tx = self._safe_load(
            transactions_gold_path,
            select_expr=[
                F.col("msgid").cast("string").alias("tx_msg_id"),
                F.col("txsts").cast("string").alias("tx_status"),
                F.col("amt").cast("double").alias("tx_amount"),
                F.col("ccy").cast("string").alias("tx_ccy"),
                F.col("txtp").cast("string").alias("tx_type"),
                F.col("transaction_id").cast("string").alias("transaction_id"),
                F.col("endtoendid").cast("string").alias("end_to_end_id"),
            ],
        )

        b_typ = self._safe_load(typologies_bronze_path)
        if b_typ is not None:
            b_typ = b_typ.select(
                F.col("tenant_id").alias("cfg_tenant_id"),
                F.col("typology_id").alias("cfg_typology_id"),
                F.col("typology_cfg").alias("cfg_typology_cfg"),
                F.col("configuration").alias("typology_configuration_json"),
            )

        b_net = self._safe_load(network_map_bronze_path)
        if b_net is not None:
            b_net = b_net.select(
                F.col("tenant_id").alias("cfg_tenant_id"),
                F.col("configuration").alias("network_configuration_json"),
            )

        b_rules = self._safe_load(rules_bronze_path)
        if b_rules is not None:
            b_rules = b_rules.select(
                F.col("tenant_id").alias("cfg_tenant_id"),
                F.col("rule_id").alias("cfg_rule_id"),
                F.col("rule_cfg").alias("cfg_rule_cfg"),
                F.col("configuration").alias("rule_configuration_json"),
                F.col("ingested_at_ts").alias("rule_metadata_ingested_at_ts"),
            )

        # -- infer alert schema and parse ------------------------------------
        alert_schema = self.infer_json_schema(s_alerts, "alert_data")
        a = s_alerts.withColumn("alert_data_obj", F.from_json("alert_data", alert_schema))

        # -- build views -------------------------------------------------------
        alerts_nav_header = self._build_header(a, g_tx)
        typ = self._build_typologies(a, b_typ)
        rules = self._build_rules(a, b_rules)
        network_eval = self._build_network_eval(alerts_nav_header, b_net)

        # -- write views -------------------------------------------------------
        alerts_nav_header_path = f"{self.alert_nav_root}/header"
        alerts_nav_typologies_path = f"{self.alert_nav_root}/typologies_triggered"
        alerts_nav_rules_path = f"{self.alert_nav_root}/rules_triggered"
        alerts_nav_network_eval_path = f"{self.alert_nav_root}/network_evaluated"

        self.write_hudi(
            alerts_nav_header,
            alerts_nav_header_path,
            self._hudi_anv_opts(
                "vw_alerts_nav_header",
                record_key="pk",
                precombine="ingested_at_ts",
                partition="alert_date",
            ),
        )
        print(f"[AlertNavigatorETL] Header view written → {alerts_nav_header_path}")

        self.write_hudi(
            typ,
            alerts_nav_typologies_path,
            self._hudi_anv_opts(
                "vw_alerts_nav_typologies",
                record_key="pk",
                precombine="alert_timestamp",
            ),
        )
        print(f"[AlertNavigatorETL] Typologies view written → {alerts_nav_typologies_path}")

        self.write_hudi(
            rules,
            alerts_nav_rules_path,
            self._hudi_anv_opts(
                "vw_alerts_nav_rules",
                record_key="pk",
                precombine="alert_timestamp",
            ),
        )
        print(f"[AlertNavigatorETL] Rules view written → {alerts_nav_rules_path}")

        if network_eval is not None:
            self.write_hudi(
                network_eval,
                alerts_nav_network_eval_path,
                self._hudi_anv_opts(
                    "vw_alerts_nav_network_evaluated",
                    record_key="pk",
                    precombine="alert_id",
                ),
            )
            print(
                f"[AlertNavigatorETL] Network eval view written → {alerts_nav_network_eval_path}"
            )

        return self.alert_nav_root

    # ------------------------------------------------------------------
    # SILVER / GOLD  (no-op for view builders)
    # ------------------------------------------------------------------

    def silver(self) -> str:
        """Views are built entirely in bronze(); silver is a no-op."""
        return self.alert_nav_root

    def gold(self) -> str:
        """Views are built entirely in bronze(); gold is a no-op."""
        return self.alert_nav_root

    # ------------------------------------------------------------------
    # ORCHESTRATOR
    # ------------------------------------------------------------------

    def run(self, source_path: str = "") -> str:
        print("[AlertNavigatorETL] Starting Alert Navigator view build")
        self.bronze(source_path)
        print("[AlertNavigatorETL] View build complete.")
        return self.alert_nav_root
