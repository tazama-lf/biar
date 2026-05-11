from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .BaseETL import BaseETL


class TransactionHistoryViewETL(BaseETL):
    """
    Transaction History View builder.

    Reads bronze/transactions, extracts PACS JSON, joins optional
    alerts/cases/tasks, expands to entity level, and writes event +
    day/week/month/year aggregate rows to Hudi.

    Uses transaction_id (TxTp||endToEndId) as the primary key.
    """

    def __init__(self, spark, warehouse_root: str) -> None:
        super().__init__(spark, warehouse_root)
        self.views_root = f"{self.warehouse_root}/views"
        self.view_path = f"{self.views_root}/vw_transaction_history"
        self.transactions_bronze_path = f"{self.warehouse_root}/bronze/transactions"

    @property
    def bronze_path(self) -> str:
        return self.view_path

    @property
    def silver_path(self) -> str:
        return self.view_path

    @property
    def gold_path(self) -> str:
        return self.view_path

    # ------------------------------------------------------------------
    # INTERNAL HELPERS
    # ------------------------------------------------------------------

    def _safe_load(self, path: str, select_expr: list | None = None) -> DataFrame | None:
        """Attempt to load a Hudi table; return None if it does not exist."""
        try:
            df = self.spark.read.format("hudi").load(path)
            if select_expr:
                df = df.select(*select_expr)
            return df
        except Exception:
            return None

    def _resolve_json_column(self, df: DataFrame) -> DataFrame:
        """Auto-detect and normalize the raw JSON payload column."""
        candidates = [
            "transactionData",
            "transaction_data",
            "transaction",
            "payload",
            "raw_payload",
        ]
        json_col = next((c for c in candidates if c in df.columns), None)
        if json_col is None:
            raise ValueError(
                f"No raw JSON column found in bronze/transactions. "
                f"Tried: {candidates}\nAvailable: {df.columns}"
            )
        print(f"[TransactionHistoryViewETL] Using JSON column: {json_col} → transaction_data")
        return df.withColumn("transaction_data", F.col(json_col).cast("string"))

    def _extract_base(self, df: DataFrame) -> DataFrame:
        """Build the base transaction frame from parsed PACS JSON.

        Supports PACS.008, PACS.002, and pain.001 with DataCache fallbacks.
        Uses regex as ultimate fallback since DataCache key names are stable
        even when nesting varies.
        """
        # Use bronze tx_type if present, else fall back to JSON extraction
        tx_type = F.coalesce(F.col("tx_type"), F.get_json_object("transaction_data", "$.TxTp"))

        # --- Regex helpers (work regardless of JSON nesting) ---
        _re = lambda key, grp=1: F.regexp_extract("transaction_data", rf'"{key}"\s*:\s*"([^"]+)"', grp)
        _re_num = lambda key, grp=1: F.regexp_extract("transaction_data", rf'"{key}"\s*:\s*([0-9.]+)', grp)

        # --- Message IDs & timestamps (get_json_object FIRST to avoid "" trap) ---
        tx_msg_id = F.coalesce(
            F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.GrpHdr.MsgId"),
            F.get_json_object("transaction_data", "$.FIToFIPmtSts.GrpHdr.MsgId"),
            F.get_json_object("transaction_data", "$.Document.FIToFICstmrCdtTrf.GrpHdr.MsgId"),
            F.get_json_object("transaction_data", "$.CstmrCdtTrfInitn.GrpHdr.MsgId"),
            F.get_json_object("transaction_data", "$.DataCache.MsgId"),
            _re("MsgId"),
            _re("msgId"),
        )
        event_ts = F.to_timestamp(
            F.coalesce(
                F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.GrpHdr.CreDtTm"),
                F.get_json_object("transaction_data", "$.FIToFIPmtSts.GrpHdr.CreDtTm"),
                F.get_json_object("transaction_data", "$.Document.FIToFICstmrCdtTrf.GrpHdr.CreDtTm"),
                F.get_json_object("transaction_data", "$.CstmrCdtTrfInitn.GrpHdr.CreDtTm"),
                F.get_json_object("transaction_data", "$.DataCache.CreDtTm"),
                F.get_json_object("transaction_data", "$.DataCache.creDtTm"),
                _re("CreDtTm"),
                _re("creDtTm"),
            )
        )
        event_date = F.to_date(event_ts)

        # --- Debtor / Creditor names ---
        dbtr_name = F.coalesce(
            F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.Dbtr.Nm"),
            F.get_json_object("transaction_data", "$.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Dbtr.Nm"),
            F.get_json_object("transaction_data", "$.CstmrCdtTrfInitn.PmtInf.Dbtr.Nm"),
            _re("dbtrId"),
            F.get_json_object("transaction_data", "$.DataCache.dbtrId"),
        )
        cdtr_name = F.coalesce(
            F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.Cdtr.Nm"),
            F.get_json_object("transaction_data", "$.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Cdtr.Nm"),
            F.get_json_object("transaction_data", "$.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.Cdtr.Nm"),
            _re("cdtrId"),
            F.get_json_object("transaction_data", "$.DataCache.cdtrId"),
        )

        # --- Debtor / Creditor IDs ---
        dbtr_id = F.regexp_replace(
            F.coalesce(
                _re("dbtrId"),
                F.get_json_object("transaction_data", "$.DataCache.dbtrId"),
                F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.Dbtr.Id.PrvtId.Othr[0].Id"),
                F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.Dbtr.Id.OrgId.Othr[0].Id"),
                F.get_json_object("transaction_data", "$.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Dbtr.Id.PrvtId.Othr[0].Id"),
                F.get_json_object("transaction_data", "$.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Dbtr.Id.OrgId.Othr[0].Id"),
                F.get_json_object("transaction_data", "$.CstmrCdtTrfInitn.PmtInf.Dbtr.Id.PrvtId.Othr[0].Id"),
                F.get_json_object("transaction_data", "$.CstmrCdtTrfInitn.PmtInf.Dbtr.Id.OrgId.Othr[0].Id"),
            ),
            "TAZAMA_EID$", "",
        )
        cdtr_id = F.regexp_replace(
            F.coalesce(
                _re("cdtrId"),
                F.get_json_object("transaction_data", "$.DataCache.cdtrId"),
                F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.Cdtr.Id.PrvtId.Othr[0].Id"),
                F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.Cdtr.Id.OrgId.Othr[0].Id"),
                F.get_json_object("transaction_data", "$.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Cdtr.Id.PrvtId.Othr[0].Id"),
            F.get_json_object("transaction_data", "$.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Cdtr.Id.OrgId.Othr[0].Id"),
            F.get_json_object("transaction_data", "$.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.Cdtr.Id.PrvtId.Othr[0].Id"),
            F.get_json_object("transaction_data", "$.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.Cdtr.Id.OrgId.Othr[0].Id"),
            ),
            "TAZAMA_EID$", "",
        )

        # --- Account IDs (PACS.008 FIRST to match transaction_detail) ---
        dbtr_acct = F.coalesce(
            F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.DbtrAcct.Id.Othr[0].Id"),
            F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.DbtrAcct.Id.IBAN"),
            F.get_json_object("transaction_data", "$.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.DbtrAcct.Id.Othr[0].Id"),
            F.get_json_object("transaction_data", "$.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.DbtrAcct.Id.IBAN"),
            F.get_json_object("transaction_data", "$.CstmrCdtTrfInitn.PmtInf.DbtrAcct.Id.Othr[0].Id"),
            F.get_json_object("transaction_data", "$.CstmrCdtTrfInitn.PmtInf.DbtrAcct.Id.IBAN"),
            F.get_json_object("transaction_data", "$.DataCache.dbtrAcctId"),
            _re("dbtrAcctId"),
        )
        cdtr_acct = F.coalesce(
            F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAcct.Id.Othr[0].Id"),
            F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAcct.Id.IBAN"),
            F.get_json_object("transaction_data", "$.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAcct.Id.Othr[0].Id"),
            F.get_json_object("transaction_data", "$.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAcct.Id.IBAN"),
            F.get_json_object("transaction_data", "$.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.CdtrAcct.Id.Othr[0].Id"),
            F.get_json_object("transaction_data", "$.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.CdtrAcct.Id.IBAN"),
            F.get_json_object("transaction_data", "$.DataCache.cdtrAcctId"),
            _re("cdtrAcctId"),
        )

        # --- Amount & currency ---
        tx_amount = F.coalesce(
            _re_num("amt"),
            F.get_json_object("transaction_data", "$.DataCache.instdAmt.amt"),
            F.get_json_object("transaction_data", "$.DataCache.intrBkSttlmAmt.amt"),
            F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.InstdAmt.Amt.Amt"),
            F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.IntrBkSttlmAmt.Amt"),
            F.get_json_object("transaction_data", "$.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.InstdAmt.Amt.Amt"),
            F.get_json_object("transaction_data", "$.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.Amt.InstdAmt.Amt"),
        ).cast("double")
        tx_ccy = F.coalesce(
            _re("ccy"),
            F.get_json_object("transaction_data", "$.DataCache.instdAmt.ccy"),
            F.get_json_object("transaction_data", "$.DataCache.intrBkSttlmAmt.ccy"),
            F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.InstdAmt.Amt.Ccy"),
            F.get_json_object("transaction_data", "$.FIToFICstmrCdtTrf.CdtTrfTxInf.IntrBkSttlmAmt.Ccy"),
            F.get_json_object("transaction_data", "$.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.InstdAmt.Amt.Ccy"),
            F.get_json_object("transaction_data", "$.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.Amt.InstdAmt.Ccy"),
        )

        has_source = "source_file_path" in df.columns
        has_hash = "record_hash" in df.columns

        base = (
            df
            .withColumn("tx_type", tx_type)
            .withColumn("tx_msg_id", tx_msg_id)
            .withColumn("event_ts", event_ts)
            .withColumn("event_date", event_date)
            .withColumn("tx_amount", tx_amount)
            .withColumn("tx_ccy", tx_ccy)
            .withColumn("dbtr_name", dbtr_name)
            .withColumn("dbtr_id", dbtr_id)
            .withColumn("cdtr_name", cdtr_name)
            .withColumn("cdtr_id", cdtr_id)
            .withColumn("dbtr_account_id", dbtr_acct)
            .withColumn("cdtr_account_id", cdtr_acct)
        )

        stats = base.agg(
            F.count("*").alias("total"),
            F.sum(F.when(F.col("dbtr_id").isNotNull(), 1).otherwise(0)).alias("dbtr_id_ok"),
            F.sum(F.when(F.col("cdtr_id").isNotNull(), 1).otherwise(0)).alias("cdtr_id_ok"),
            F.sum(F.when(F.col("dbtr_account_id").isNotNull(), 1).otherwise(0)).alias("dbtr_acct_ok"),
            F.sum(F.when(F.col("cdtr_account_id").isNotNull(), 1).otherwise(0)).alias("cdtr_acct_ok"),
            F.sum(F.when(F.col("tx_amount").isNotNull(), 1).otherwise(0)).alias("amount_ok"),
        ).collect()[0]
        if stats["total"] > 0:
            print(f"[TransactionHistoryViewETL] Extraction coverage out of {stats['total']} rows:")
        
        return (
            base
            .select(
                F.col("transaction_id").cast("string").alias("transaction_id"),
                F.col("end_to_end_id").cast("string").alias("end_to_end_id"),
                F.col("tenant_id").cast("string").alias("tenant_id"),
                F.col("tx_type").cast("string").alias("tx_type"),
                F.col("tx_msg_id").cast("string").alias("tx_msg_id"),
                F.col("event_ts").cast("timestamp").alias("event_ts"),
                F.col("event_date").cast("date").alias("event_date"),
                F.col("tx_amount").cast("double").alias("tx_amount"),
                F.col("tx_ccy").cast("string").alias("tx_ccy"),
                F.col("dbtr_name").cast("string").alias("dbtr_name"),
                F.col("dbtr_id").cast("string").alias("dbtr_id"),
                F.col("cdtr_name").cast("string").alias("cdtr_name"),
                F.col("cdtr_id").cast("string").alias("cdtr_id"),
                F.col("dbtr_account_id").cast("string").alias("dbtr_account_id"),
                F.col("cdtr_account_id").cast("string").alias("cdtr_account_id"),
                (
                    F.col("source_file_path").cast("string")
                    if has_source
                    else F.lit(None).cast("string")
                ).alias("source_file_path"),
                (
                    F.col("record_hash").cast("string")
                    if has_hash
                    else F.lit(None).cast("string")
                ).alias("record_hash"),
            )
            .filter(F.col("event_ts").isNotNull())
        )

    def _join_flags(self, base: DataFrame) -> DataFrame:
        """Join optional alerts, cases, and tasks to add is_alerted / is_investigated."""
        alerts_g = self._safe_load(
            f"{self.warehouse_root}/gold/alerts",
            select_expr=[
                F.col("tx_msg_id").cast("string").alias("tx_msg_id"),
                F.col("alert_id").cast("long").alias("alert_id"),
                F.col("case_id").cast("long").alias("case_id"),
            ],
        )
        if alerts_g is not None:
            alerts_g = alerts_g.dropna(subset=["tx_msg_id"]).dropDuplicates(["tx_msg_id"])

        # ------------------------------------------------------------------
        # CASES — status column is optional
        # ------------------------------------------------------------------
        cases_g = self._safe_load(f"{self.warehouse_root}/gold/cases")
        if cases_g is not None:
            if "status" in cases_g.columns:
                cases_g = cases_g.select(
                    F.col("case_id").cast("long").alias("case_id"),
                    F.col("status").cast("string").alias("case_status"),
                )
            else:
                cases_g = cases_g.select(
                    F.col("case_id").cast("long").alias("case_id"),
                    F.lit(None).cast("string").alias("case_status"),
                )
            cases_g = cases_g.dropDuplicates(["case_id"])

        tasks_g = self._safe_load(
            f"{self.warehouse_root}/gold/tasks",
            select_expr=[
                F.col("case_id").cast("long").alias("case_id"),
                F.col("is_completed").cast("int").alias("is_completed"),
            ],
        )

        flags = base
        if alerts_g is not None:
            flags = flags.join(alerts_g, "tx_msg_id", "left")
        if cases_g is not None:
            flags = flags.join(cases_g, "case_id", "left")
        if tasks_g is not None:
            tasks_agg = tasks_g.groupBy("case_id").agg(
                F.max("is_completed").alias("has_completed_task")
            )
            flags = flags.join(tasks_agg, "case_id", "left")

        # ------------------------------------------------------------------
        # Guard: ensure these columns exist even when the table was missing
        # ------------------------------------------------------------------
        if "alert_id" not in flags.columns:
            flags = flags.withColumn("alert_id", F.lit(None).cast("long"))
        if "case_status" not in flags.columns:
            flags = flags.withColumn("case_status", F.lit(None).cast("string"))
        if "has_completed_task" not in flags.columns:
            flags = flags.withColumn("has_completed_task", F.lit(0).cast("int"))

        return (
            flags.withColumn(
                "is_alerted",
                F.when(F.col("alert_id").isNotNull(), F.lit(1)).otherwise(F.lit(0)),
            )
            .withColumn(
                "is_investigated",
                F.when(
                    (F.col("case_status").isNotNull())
                    | (F.coalesce(F.col("has_completed_task"), F.lit(0)) == 1),
                    F.lit(1),
                ).otherwise(F.lit(0)),
            )
            .drop("has_completed_task", "alert_id", "case_status", "case_id")
        )

    def _expand_entities(self, df: DataFrame) -> DataFrame:
        """Explode each transaction into entity rows (accounts + counterparties)."""
        return (
            df.select(
                "*",
                F.array(
                    F.when(
                        F.col("dbtr_account_id").isNotNull(),
                        F.struct(
                            F.lit("ACCOUNT").alias("entity_type"),
                            F.lit("DEBTOR").alias("entity_role"),
                            F.col("dbtr_account_id").alias("entity_id"),
                            F.col("dbtr_name").alias("entity_name"),
                        ),
                    ),
                    F.when(
                        F.col("cdtr_account_id").isNotNull(),
                        F.struct(
                            F.lit("ACCOUNT").alias("entity_type"),
                            F.lit("CREDITOR").alias("entity_role"),
                            F.col("cdtr_account_id").alias("entity_id"),
                            F.col("cdtr_name").alias("entity_name"),
                        ),
                    ),
                    F.when(
                        F.col("dbtr_id").isNotNull(),
                        F.struct(
                            F.lit("COUNTERPARTY").alias("entity_type"),
                            F.lit("DEBTOR").alias("entity_role"),
                            F.col("dbtr_id").alias("entity_id"),
                            F.col("dbtr_name").alias("entity_name"),
                        ),
                    ),
                    F.when(
                        F.col("cdtr_id").isNotNull(),
                        F.struct(
                            F.lit("COUNTERPARTY").alias("entity_type"),
                            F.lit("CREDITOR").alias("entity_role"),
                            F.col("cdtr_id").alias("entity_id"),
                            F.col("cdtr_name").alias("entity_name"),
                        ),
                    ),
                ).alias("entities"),
            )
            .withColumn("entity", F.explode(F.expr("filter(entities, x -> x is not null)")))
            .drop("entities")
            .withColumn("entity_type", F.col("entity.entity_type"))
            .withColumn("entity_role", F.col("entity.entity_role"))
            .withColumn("entity_id", F.col("entity.entity_id"))
            .withColumn("entity_name", F.col("entity.entity_name"))
            .drop("entity")
        )

    def _build_events(self, df: DataFrame) -> DataFrame:
        """Add window calculations and mark as EVENT rows."""
        w_recent = Window.partitionBy("entity_type", "entity_role", "entity_id").orderBy(
            F.col("event_ts").desc()
        )
        w_cum = (
            Window.partitionBy("entity_type", "entity_role", "entity_id")
            .orderBy(F.col("event_ts").asc())
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )

        return (
            df.withColumn("recent_rank_desc", F.row_number().over(w_recent))
            .withColumn("cum_tx_count", F.count(F.lit(1)).over(w_cum))
            .withColumn(
                "cum_tx_amount",
                F.sum(F.coalesce(F.col("tx_amount"), F.lit(0.0))).over(w_cum),
            )
            .withColumn("row_type", F.lit("EVENT"))
            .withColumn("bucket_granularity", F.lit(None).cast("string"))
            .withColumn("bucket_start", F.lit(None).cast("timestamp"))
            .withColumn("bucket_tx_count", F.lit(None).cast("long"))
            .withColumn("bucket_tx_amount", F.lit(None).cast("double"))
        )

    def _build_agg(self, df: DataFrame, granularity: str) -> DataFrame:
        """Roll up entity rows to a given time granularity with deterministic PK."""
        bucket_start = F.date_trunc(granularity, F.col("event_ts"))

        return (
            df.withColumn("bucket_start", bucket_start)
        .groupBy("entity_type", "entity_role", "entity_id", "bucket_start")
        .agg(
            F.count("*").cast("long").alias("bucket_tx_count"),
            F.sum(F.coalesce(F.col("tx_amount"), F.lit(0.0)))
                .cast("double")
                .alias("bucket_tx_amount"),
            F.max("event_date").alias("event_date"),
            F.max("tenant_id").alias("tenant_id"),
        )
        .withColumn("row_type", F.lit("AGG"))
        .withColumn("bucket_granularity", F.lit(granularity))

        .withColumn(
            "transaction_id",
            F.concat_ws(
                 "||",
                F.lit("AGG"),
                F.col("entity_type"),
                F.col("entity_role"),
                F.col("entity_id"),
                F.col("bucket_granularity"),
                F.col("bucket_start").cast("string"),
            ),
        )

        # -------------------------------------------------------------
        # Remaining columns (unchanged)
        # -------------------------------------------------------------
        .withColumn("recent_rank_desc", F.lit(None).cast("int"))
        .withColumn("cum_tx_count", F.lit(None).cast("long"))
        .withColumn("cum_tx_amount", F.lit(None).cast("double"))
        .withColumn("end_to_end_id", F.lit(None).cast("string"))
        .withColumn("tx_type", F.lit(None).cast("string"))
        .withColumn("tx_msg_id", F.lit(None).cast("string"))
        .withColumn("event_ts", F.lit(None).cast("timestamp"))
        .withColumn("tx_amount", F.lit(None).cast("double"))
        .withColumn("tx_ccy", F.lit(None).cast("string"))
        .withColumn("entity_name", F.lit(None).cast("string"))
        .withColumn("is_alerted", F.lit(None).cast("int"))
        .withColumn("is_investigated", F.lit(None).cast("int"))
        .withColumn("source_file_path", F.lit(None).cast("string"))
        .withColumn("record_hash", F.lit(None).cast("string"))
    )

    def _add_pk(self, df: DataFrame) -> DataFrame:
        """Add unique record key for Hudi + ingestion timestamp.

        transaction_id is kept as tx_type||end_to_end_id from bronze.
        _record_key ensures each entity row is unique for Hudi upserts.
        """
        return (
            df.withColumn(
                "_record_key",
                F.concat_ws(
                    "||",
                    F.col("transaction_id"),
                    F.col("entity_type"),
                    F.col("entity_role"),
                    F.col("entity_id"),
                ),
            )
            .withColumn("ingested_at_ts", F.current_timestamp())
        )

    # ------------------------------------------------------------------
    # BRONZE  (main view build)
    # ------------------------------------------------------------------

    def bronze(self, source_path: str = "") -> str:
        """
        Build vw_transaction_history from bronze/transactions.

        *source_path* is ignored (reads from warehouse bronze path).
        """
        print("[TransactionHistoryViewETL] Creating Transaction History View...")

        # 1. Load bronze transactions
        tx = self.spark.read.format("hudi").load(self.transactions_bronze_path)

        # 2. Normalize column names
        rename_map = {
            "endToEndId": "end_to_end_id",
            "tenantId": "tenant_id",
            "transaction_id": "transaction_id",
        }
        for src, dst in rename_map.items():
            if src in tx.columns and dst not in tx.columns:
                tx = tx.withColumnRenamed(src, dst)

        # 3. Resolve JSON column
        tx = self._resolve_json_column(tx)

        # 4. Build base frame
        base = self._extract_base(tx)

        # 5. Join flags
        flags = self._join_flags(base)

        # 6. Expand entities
        entity_rows = self._expand_entities(flags)

        # 7. Build events + aggregates
        events = self._build_events(entity_rows)
        agg_day = self._build_agg(entity_rows, "day")
        agg_week = self._build_agg(entity_rows, "week")
        agg_month = self._build_agg(entity_rows, "month")
        agg_year = self._build_agg(entity_rows, "year")

        # 8. Union all
        view_df = (
            events.select(
                "transaction_id",
                "entity_type",
                "entity_role",
                "entity_id",
                "entity_name",
                "end_to_end_id",
                "tenant_id",
                "tx_type",
                "tx_msg_id",
                "event_ts",
                "event_date",
                "tx_amount",
                "tx_ccy",
                "is_alerted",
                "is_investigated",
                "recent_rank_desc",
                "cum_tx_count",
                "cum_tx_amount",
                "row_type",
                "bucket_granularity",
                "bucket_start",
                "bucket_tx_count",
                "bucket_tx_amount",
                "source_file_path",
                "record_hash",
            )
            .unionByName(agg_day, allowMissingColumns=True)
            .unionByName(agg_week, allowMissingColumns=True)
            .unionByName(agg_month, allowMissingColumns=True)
            .unionByName(agg_year, allowMissingColumns=True)
        )

        # 9. Ingest timestamp
        view_df = self._add_pk(view_df)

        # 10. Write Hudi view — transaction_id is the primary key
        self.write_hudi(
            view_df,
            self.view_path,
            self.hudi_opts(
                "vw_transaction_history",
                record_key="_record_key",
                precombine="ingested_at_ts",
            ),
        )
        print(f"[TransactionHistoryViewETL] View written → {self.view_path}")
        return self.view_path

    # ------------------------------------------------------------------
    # SILVER / GOLD  (no-op for view builders)
    # ------------------------------------------------------------------

    def silver(self) -> str:
        return self.view_path

    def gold(self) -> str:
        return self.view_path

    # ------------------------------------------------------------------
    # ORCHESTRATOR
    # ------------------------------------------------------------------

    def run(self, source_path: str = "") -> str:
        print("[TransactionHistoryViewETL] Starting view build")
        self.bronze(source_path)
        print("[TransactionHistoryViewETL] View build complete.")
        return self.view_path
