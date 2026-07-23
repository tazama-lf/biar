# Tazama Data Lakehouse — Data Lineage (Pilot: `event_history` + `evaluation` Tables)

> **Scope**: Field-level provenance from the Tazama Core PostgreSQL databases into the Apache Hudi Data Lakehouse Bronze, Silver, and Gold layers, as implemented in the `biar` repository (`nifi/tazama.xml` + `automation-orchestrator/lakehouse_automation_pipeline.py`).
>
> **Tables covered**: `event_history.entity`, `event_history.account_holder`, `event_history.account`, `event_history.transaction`, `evaluation.evaluation`
>
> **Source databases** (all on `10.10.80.16:15432`):
> - `event_history` — Tazama Core reference data (entity, account, account_holder, transaction)
> - `evaluation` — Tazama TMS evaluation results, including DataCache-enriched pacs records
> - `raw_history` — Raw ISO 20022 inbound messages (pacs008, pacs002); **the TMS writes DataCache back into the pacs.002 document before it is stored here** — documented separately in the PACS lineage

---

## Architecture Overview

```text
┌─────────────────────────────────────────────────────────────────────┐
│  PostgreSQL  (10.10.80.16:15432)                                    │
│                                                                     │
│  event_history  ── entity, account_holder, account, transaction     │
│  evaluation     ── evaluation  (DataCache-enriched pacs records)    │
│  raw_history    ── pacs008, pacs002  (raw ISO 20022 messages)       │
└────────────────────────────┬────────────────────────────────────────┘
                             │ NiFi: QueryDatabaseTableRecord
                             │ every 1 min · batch 500 · watermark credttm
                             ▼
                   Apache Ozone (S3-compatible)
                   s3a://{pbucket}/{table}/{iso-ts}_{uuid}.json
                             │ NiFi: InvokeHTTP → POST /checksubmit
                             ▼
              Automation Orchestrator (Python/FastAPI  :8282)
                             │ PySpark + Hudi 0.14.1
                             ▼
┌──────────────────────────────────────────────────────────┐
│  WAREHOUSE_ROOT = /opt/Tazama_Warehouse                  │
│                                                          │
│  bronze/{table}   ← raw ingestion with record hash       │
│  silver/{table}   ← typed, normalised, deduped, DQ       │
│  gold/{table}     ← analytics-ready, partitioned         │
└──────────────────────────────────────────────────────────┘
```

### NiFi Extraction Pattern (all tables in scope)

| Attribute | `event_history` tables | `evaluation.evaluation` |
|-----------|------------------------|-------------------------|
| Processor | `QueryDatabaseTableRecord` | `QueryDatabaseTableRecord` |
| JDBC URL | `jdbc:postgresql://10.10.80.16:15432/event_history` | `jdbc:postgresql://10.10.80.16:15432/evaluation` |
| JDBC connection service | `5302abbb-…` (`event_history`) | `3069101c-4e35-3e36-…` (`evaluation`) |
| Table name | `entity` / `account_holder` / `account` / `transaction` | `evaluation` |
| Columns to Return | *(all — empty field)* | *(all — empty field)* |
| Watermark column | `credttm` | `credttm` |
| Batch / fetch size | 500 rows | 500 rows |
| Schedule | Every 1 minute | Every 1 minute |
| Record writer | JSON (`3f3c10d8-…`) | JSON (`3f3c10d8-…`) |
| Ozone `table` attribute | `{tablename}` | `evaluation` |

After writing to Ozone, NiFi calls the Automation Orchestrator API (`InvokeHTTP → POST /checksubmit`), passing `bucket`, `table`, and `object_key`. The orchestrator routes by `table` name and runs the PySpark ETL.

---

## 1. `event_history.entity`

### Source → Ozone

| Step | Detail |
|------|--------|
| Source table | `event_history.entity` |
| NiFi processor | `QueryDatabaseTableRecord` (id `477ac78b-…`) |
| Watermark column | `credttm` |
| Ozone bucket | `#{pbucket}` (parameterised) |
| Ozone object key prefix | `entity/` |

### Ozone → Lakehouse

**Status: Not yet implemented.**

`run_full_etl` explicitly skips this table:

```python
elif table == "entity":
    print("Skipping unsupported table 'entity' (no ETL defined)")
    return { "result": "Skipped: unsupported table" }
```

There is no `etl_entity()` function. The raw JSON files land in Ozone but are never promoted to Bronze, Silver, or Gold Hudi tables.

### Known source columns

Only `credttm` is confirmed from the NiFi config (watermark). All other columns are fetched but not yet mapped.

---

## 2. `event_history.account_holder`

### Source → Ozone

| Step | Detail |
|------|--------|
| Source table | `event_history.account_holder` |
| NiFi processor | `QueryDatabaseTableRecord` (id `b1a…`) |
| Watermark column | `credttm` |
| Ozone path | `s3a://{bucket}/account_holder/{timestamp}_{uuid}.json` |

### Ozone → Bronze (`bronze/account_holder`)

ETL function: `etl_account_holder()`

| Source field (Ozone JSON) | Bronze column | Type | Transform |
|---------------------------|---------------|------|-----------|
| *(all source fields)* | *(all source fields, pass-through)* | *source types* | No casting at Bronze |
| — | `ingested_at_ts` | `timestamp` | `current_timestamp()` |
| — | `source_file_path` | `string` | `input_file_name()` |
| — | `_row_payload_json` | `string` | `to_json(struct(*))` — full row serialised |
| — | `record_hash` | `string` | `SHA256(_row_payload_json)` |

**Hudi options**:

| Option | Value |
|--------|-------|
| Table name | `bronze_account_holder` |
| Path | `$WAREHOUSE_ROOT/bronze/account_holder` |
| Record key | `record_hash` |
| Precombine | `ingested_at_ts` |
| Table type | `COPY_ON_WRITE` |
| Key generator | `NonpartitionedKeyGenerator` |
| Operation | `upsert` |

### Bronze → Silver (`silver/account_holder`)

| Bronze column | Silver column | Type | Transform |
|---------------|---------------|------|-----------|
| `tenantid` | `tenant_id` | `string` | rename |
| `credttm` | `event_ts` | `timestamp` | `to_timestamp(credttm)` |
| `credttm` | `event_date` | `date` | `to_date(to_timestamp(credttm))` |
| `destination` | `account_id` | `string` | rename |
| `source` | `counterparty_id` | `string` | rename |
| *(all other fields)* | *(pass-through)* | | |
| — | `pk` | `string` | `SHA256("account_holder" \|\| tenant_id \|\| counterparty_id \|\| account_id \|\| credttm)` |
| `ingested_at_ts` | `ingested_at_ts` | `timestamp` | `coalesce(ingested_at_ts, current_timestamp())` |

**Hudi options**:

| Option | Value |
|--------|-------|
| Table name | `silver_account_holder` |
| Path | `$WAREHOUSE_ROOT/silver/account_holder` |
| Record key | `pk` |
| Precombine | `ingested_at_ts` |
| Key generator | `NonpartitionedKeyGenerator` |

### Silver → Gold (`gold/account_holder`)

| Silver column | Gold column | Type | Transform |
|---------------|-------------|------|-----------|
| *(all silver columns)* | *(all silver columns)* | | pass-through |
| — | `relationship_type` | `string` | literal `"ACCOUNT_HOLDER"` |
| `_row_payload_json` | — | | **dropped** |
| `tenantid` | — | | **dropped** |
| `credttm` | — | | **dropped** (superseded by `event_ts`) |

**Hudi options**:

| Option | Value |
|--------|-------|
| Table name | `gold_account_holder` |
| Path | `$WAREHOUSE_ROOT/gold/account_holder` |
| Record key | `pk` |
| Precombine | `ingested_at_ts` |
| Key generator | `NonpartitionedKeyGenerator` |

### Field lineage summary (`account_holder`)

```text
event_history.account_holder
  tenantid       ──rename──▶  tenant_id      (Silver → Gold)
  credttm        ──parse──▶   event_ts        (Silver → Gold)
  credttm        ──parse──▶   event_date      (Silver → Gold)
  credttm        ──dropped at Gold (raw value)
  destination    ──rename──▶  account_id      (Silver → Gold)
  source         ──rename──▶  counterparty_id (Silver → Gold)
  [all others]   ──pass─────▶ [same name]     (Bronze → Silver → Gold)
  [computed]                   pk              (Silver → Gold)
  [computed]                   ingested_at_ts  (Bronze → Silver → Gold)
  [computed]                   record_hash     (Bronze only)
  [computed]                   relationship_type = "ACCOUNT_HOLDER" (Gold only)
```

---

## 3. `event_history.account`

### Source → Ozone

| Step | Detail |
|------|--------|
| Source table | `event_history.account` |
| NiFi processor | `QueryDatabaseTableRecord` (id `867…`) |
| Watermark column | `credttm` |
| Ozone path | `s3a://{bucket}/account/{timestamp}_{uuid}.json` |

### Ozone → Bronze (`bronze/account`)

ETL function: `etl_account()`

Supports both JSON and CSV input (detected by file extension):

| Source field (Ozone) | Bronze column | Type | Transform |
|----------------------|---------------|------|-----------|
| `id` | `account_id` | `string` | rename via `withColumnRenamed` |
| `tenantid` | `tenant_id` | `string` | rename via `withColumnRenamed` |
| *(all other fields)* | *(pass-through)* | *source types* | |
| — | `ingested_at_ts` | `timestamp` | `current_timestamp()` |
| — | `record_hash` | `string` | `SHA256(account_id \|\| tenant_id)` |

**Hudi options**:

| Option | Value |
|--------|-------|
| Table name | `bronze_account` |
| Path | `$WAREHOUSE_ROOT/bronze/account` |
| Record key | `record_hash` |
| Precombine | `ingested_at_ts` |
| Key generator | `NonpartitionedKeyGenerator` |

### Bronze → Silver (`silver/account`)

The silver layer for `account` is currently a **structural pass-through** with a refreshed `ingested_at_ts`:

| Bronze column | Silver column | Transform |
|---------------|---------------|-----------|
| *(all bronze columns)* | *(all bronze columns)* | pass-through |
| `ingested_at_ts` | `ingested_at_ts` | refreshed to `current_timestamp()` |

> **Note**: No additional normalisation, DQ rules, or field derivations are defined for silver `account`. This layer exists structurally to conform to the medallion pattern and leave room for future enrichment.

**Hudi options**:

| Option | Value |
|--------|-------|
| Table name | `silver_account` |
| Path | `$WAREHOUSE_ROOT/silver/account` |
| Record key | `record_hash` |
| Precombine | `ingested_at_ts` |
| Key generator | `NonpartitionedKeyGenerator` |

### Silver → Gold (`gold/account`)

| Silver column | Gold column | Type | Transform |
|---------------|-------------|------|-----------|
| *(all silver columns)* | *(all silver columns)* | | pass-through |
| — | `pk` | `string` | `SHA256("account" \|\| account_id \|\| tenant_id)` |
| `ingested_at_ts` | `ingested_at_ts` | `timestamp` | refreshed to `current_timestamp()` |

**Hudi options**:

| Option | Value |
|--------|-------|
| Table name | `gold_account` |
| Path | `$WAREHOUSE_ROOT/gold/account` |
| Record key | `pk` |
| Precombine | `ingested_at_ts` |
| Key generator | `NonpartitionedKeyGenerator` |

### Field lineage summary (`account`)

```text
event_history.account
  id             ──rename──▶  account_id     (Bronze → Silver → Gold)
  tenantid       ──rename──▶  tenant_id      (Bronze → Silver → Gold)
  credttm        ──watermark only (not written to Hudi)
  [all others]   ──pass─────▶ [same name]    (Bronze → Silver → Gold)
  [computed]                   record_hash    (Bronze → Silver)
  [computed]                   ingested_at_ts (Bronze → Silver → Gold, refreshed at each layer)
  [computed]                   pk             (Gold only)
```

---

## 4. `event_history.transaction`

### Source → Ozone

| Step | Detail |
|------|--------|
| Source table | `event_history.transaction` |
| NiFi processor | `QueryDatabaseTableRecord` (id `5e1…`) |
| Watermark column | `credttm` |
| Ozone path | `s3a://{bucket}/transaction/{timestamp}_{uuid}.json` |

### Important: Transaction ETL is PACS-triggered

The `run_full_etl` router **skips standalone transaction processing**:

```python
elif table in ("transaction", "transactions"):
    print("Skipping standalone Transactions ETL: it is triggered only after "
          "pacs008 + pacs002 reach GOLD (via etl_pacs).")
```

`etl_transactions()` is invoked **only** by `etl_pacs()` after **both** `pacs008` and `pacs002` have written to Gold (coordination via marker files in `$WAREHOUSE_ROOT/.pipeline_state/`). When triggered by the PACS pipeline the mode is `"from_pacs"`, meaning the authoritative `transactionData` content comes from the PACS Bronze tables — not from the raw `event_history.transaction` feed. The `event_history.transaction` Ozone files are used only in legacy `"join"` mode (as a source of `credttm`, `endtoendid`, `tenantid`, `txtp`).

### Ozone → Bronze (`bronze/transactions`)

ETL function: `etl_transactions()` in mode `"from_pacs"` (current default)

In `from_pacs` mode the source columns derive from **PACS Bronze**, not from the `transaction` Ozone feed. The `transaction` Ozone feed fields (`txtp`, `endtoendid`, `tenantid`, `credttm`) are used only in legacy `"join"` mode.

| Source | Bronze column | Type | Transform |
|--------|---------------|------|-----------|
| PACS008 bronze `end_to_end_id` / transaction feed `endtoendid` | `endToEndId` | `string` | |
| PACS008/002 bronze `tenant_id` / feed `tenantid` | `tenantId` | `string` | |
| PACS008/002 bronze `credttm_ts` or `ingested_at_ts` | `createdAt` | `long` | Unix epoch ms |
| PACS008/002 bronze `document_json` or `document` | `transactionData` | `string` | ISO 20022 JSON payload |
| — | `transaction_id` | `long` | deterministic: `pmod(xxhash64(source_label, endToEndId, tenantId), 900000000) + 500000` |
| — | `created_at_ts` | `timestamp` | `current_timestamp()` |
| — | `source_file_path` | `string` | `lit(source_path)` |
| — | `record_hash` | `string` | `SHA256(transaction_id \|\| endToEndId \|\| tenantId \|\| createdAt \|\| transactionData)` |
| — | `_row_payload_json` | `string` | `to_json(struct(*))` — full row serialised |

**Hudi options**:

| Option | Value |
|--------|-------|
| Table name | `transactions` |
| Path | `$WAREHOUSE_ROOT/bronze/transactions` |
| Record key | `transaction_id` |
| Precombine | `created_at_ts` |
| Key generator | `NonpartitionedKeyGenerator` |

### Bronze → Silver (`silver/transactions`)

The silver layer parses `transactionData` JSON (which can be either pacs.008 or pacs.002 format) using `get_json_object`:

| Bronze column | Silver column | Type | Source JSON path | Notes |
|---------------|---------------|------|-----------------|-------|
| `transactionData` | `tx_type` | `string` | `$.TxTp` | |
| `transactionData` | `tx_tenant_id` | `string` | `$.TenantId` | |
| `transactionData` | `tx_msg_id` | `string` | `$.FIToFICstmrCdtTrf.GrpHdr.MsgId` (pacs.008) OR `$.FIToFIPmtSts.GrpHdr.MsgId` (pacs.002) | `coalesce` |
| `transactionData` | `tx_created_ts` | `timestamp` | `$.FIToFICstmrCdtTrf.GrpHdr.CreDtTm` OR `$.FIToFIPmtSts.GrpHdr.CreDtTm` | `coalesce` → `to_timestamp` |
| `transactionData` | `tx_status` | `string` | `$.FIToFIPmtSts.TxInfAndSts.TxSts` | pacs.002 only |
| `transactionData` | `tx_accept_ts` | `timestamp` | `$.FIToFIPmtSts.TxInfAndSts.AccptncDtTm` | pacs.002 only |
| `transactionData` | `tx_amount` | `double` | `$.FIToFICstmrCdtTrf.CdtTrfTxInf.InstdAmt.Amt.Amt` | pacs.008 only |
| `transactionData` | `tx_ccy` | `string` | `$.FIToFICstmrCdtTrf.CdtTrfTxInf.InstdAmt.Amt.Ccy` | pacs.008 only |
| `transactionData` | `instg_mmb_id` | `string` | `$.FIToFICstmrCdtTrf.CdtTrfTxInf.DbtrAgt.FinInstnId.ClrSysMmbId.MmbId` OR `$.FIToFIPmtSts.TxInfAndSts.InstgAgt.FinInstnId.ClrSysMmbId.MmbId` | `coalesce` |
| `transactionData` | `instd_mmb_id` | `string` | `$.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAgt.FinInstnId.ClrSysMmbId.MmbId` OR `$.FIToFIPmtSts.TxInfAndSts.InstdAgt.FinInstnId.ClrSysMmbId.MmbId` | `coalesce` |
| `transactionData` | `charge_count` | `int` | pacs.002: `size($.FIToFIPmtSts.TxInfAndSts.ChrgsInf[])` / pacs.008: 0 or 1 | |
| `tx_created_ts` | `event_ts` | `timestamp` | — | alias |
| `event_ts` | `event_date` | `date` | — | `to_date(event_ts)` |
| *(all original bronze columns)* | *(pass-through)* | | | |

Deduplication: `row_number() OVER (PARTITION BY transaction_id ORDER BY created_at_ts DESC) = 1`

**Hudi options**:

| Option | Value |
|--------|-------|
| Table name | `silver_transactions` |
| Path | `$WAREHOUSE_ROOT/silver/transactions` |
| Record key | `transaction_id` |
| Precombine | `created_at_ts` |
| Key generator | `NonpartitionedKeyGenerator` |

### Silver → Gold (`gold/transactions`)

Gold selects only clean scalar columns (no arrays or structs — validated before write):

| Silver column | Gold column | Type | Notes |
|---------------|-------------|------|-------|
| `transaction_id` | `transaction_id` | `long` | |
| `endToEndId` | `end_to_end_id` | `string` | rename |
| `tenantId` | `tenant_id` | `string` | rename |
| `tx_type` | `tx_type` | `string` | |
| `tx_msg_id` | `tx_msg_id` | `string` | |
| `tx_status` | `tx_status` | `string` | pacs.002 only |
| `tx_amount` | `tx_amount` | `double` | pacs.008 only |
| `tx_ccy` | `tx_ccy` | `string` | pacs.008 only |
| `instg_mmb_id` | `instg_mmb_id` | `string` | |
| `instd_mmb_id` | `instd_mmb_id` | `string` | |
| `charge_count` | `charge_count` | `int` | |
| `event_ts` | `event_ts` | `timestamp` | |
| `event_date` | `event_date` | `date` | Hudi partition key |
| `created_at_ts` | `ingested_at_ts` | `timestamp` | rename |
| — | `event_to_ingest_ms` | `long` | `(ingested_at_ts.epoch - event_ts.epoch) * 1000` |
| `source_file_path` | `source_file_path` | `string` | |
| `record_hash` | `record_hash` | `string` | |

> **Important**: `gold/transactions` does **not** carry debtor/creditor account IDs. Those fields (`dc_cdtr_acct_id`, `dc_dbtr_acct_id`, etc.) are in `gold/pacs002`, not here. To resolve accounts for a transaction, join via `gold/pacs002` — see the [Cross-Table Dependencies](#cross-table-dependencies-at-gold-layer) section below.

### `DataCache` — Tazama TMS Enrichment (source of `dc_*` fields)

The `DataCache` block is **not** a standard ISO 20022 field. It is produced by the Tazama Transaction Monitoring Service (TMS/rules engine) when it evaluates a payment. The TMS writes DataCache back into the pacs.002 document before it is stored in `raw_history.pacs002`, so the DataCache block is present in both `raw_history.pacs002` and `evaluation.evaluation`. **Only records that have been processed end-to-end through the TMS will have DataCache populated; earlier or synthetic records that bypassed the TMS will have null `dc_*` fields.**

`etl_pacs002()` correctly reads DataCache from `doc.DataCache.*` in the pacs.002 Ozone files. `etl_pacs002()` projects these DataCache fields into `gold/pacs002`:

| `DataCache` JSON path | Gold `pacs002` column | Type | Description |
|-----------------------|-----------------------|------|-------------|
| `DataCache.cdtrId` | `dc_cdtr_id` | `string` | Creditor entity/party ID — matches `gold/account_holder.counterparty_id` |
| `DataCache.dbtrId` | `dc_dbtr_id` | `string` | Debtor entity/party ID — matches `gold/account_holder.counterparty_id` |
| `DataCache.cdtrAcctId` | `dc_cdtr_acct_id` | `string` | Creditor account ID — matches `gold/account.account_id` |
| `DataCache.dbtrAcctId` | `dc_dbtr_acct_id` | `string` | Debtor account ID — matches `gold/account.account_id` |
| `DataCache.creDtTm` | `dc_cre_dt_tm` | `timestamp` | Creation datetime (TMS-resolved) |
| `DataCache.instdAmt.amt` | `dc_instd_amt` | `double` | Instructed amount (TMS-resolved; may differ from pacs.008 `InstructedAmount`) |
| `DataCache.instdAmt.ccy` | `dc_instd_ccy` | `string` | Instructed currency |
| `DataCache.xchgRate` | `dc_xchg_rate` | `string` | Exchange rate |
| `DataCache.intrBkSttlmAmt.amt` | `dc_intrbk_amt` | `double` | Interbank settlement amount |
| `DataCache.intrBkSttlmAmt.ccy` | `dc_intrbk_ccy` | `string` | Interbank settlement currency |

**Hudi options**:

| Option | Value |
|--------|-------|
| Table name | `transactions` |
| Path | `$WAREHOUSE_ROOT/gold/transactions` |
| Record key | `transaction_id` |
| Precombine | `ingested_at_ts` |
| Partition field | `event_date` |
| Key generator | `SimpleKeyGenerator` |
| Hive-style partitioning | `true` |
| Payload class | `OverwriteWithLatestAvroPayload` |

### Field lineage summary (`transaction`)

```text
event_history.transaction
  txtp           ──via feed join (legacy mode only)──▶  tx_type      (Silver → Gold)
  credttm        ──watermark / legacy source of createdAt
  endtoendid     ──via feed join (legacy mode only)──▶  end_to_end_id (Gold)
  tenantid       ──via feed join (legacy mode only)──▶  tenant_id    (Gold)

PACS Bronze (primary / from_pacs mode):
  pacs008.end_to_end_id ──────────────────────────────▶ end_to_end_id
  pacs008.document_json ──JSON parse──▶ tx_type, tx_msg_id, tx_created_ts,
                                         tx_amount, tx_ccy, instg_mmb_id,
                                         instd_mmb_id, charge_count
  pacs002.document      ──JSON parse──▶ tx_status, tx_accept_ts,
                                         instg_mmb_id, instd_mmb_id,
                                         charge_count
  [computed]                             transaction_id  (deterministic hash)
  [computed]                             event_ts = tx_created_ts
  [computed]                             event_date      (partition key)
  [computed]                             event_to_ingest_ms
  [computed]                             ingested_at_ts
  [computed]                             record_hash
```

---

## 5. `evaluation.evaluation`

### Source → Ozone

| Step | Detail |
|------|--------|
| Source DB | `jdbc:postgresql://10.10.80.16:15432/evaluation` |
| Source table | `evaluation.evaluation` |
| JDBC connection service | `3069101c-4e35-3e36-0000-000000000000` (controller service name: `evaluation`) |
| NiFi processor | `QueryDatabaseTableRecord` |
| Watermark column | `credttm` |
| Columns to Return | *(all columns)* |
| Batch / fetch size | 500 rows |
| Schedule | Every 1 minute |
| Ozone path | `s3a://{pbucket}/evaluation/{iso-ts}_{uuid}.json` |
| `table` attribute | `evaluation` |

### What is stored in `evaluation.evaluation`

This is the TMS/rules-engine evaluation result table. For each payment processed by Tazama, one record is written here. The record contains:

| Field (inferred) | Description |
|-----------------|-------------|
| `id` / primary key | Evaluation identifier — corresponds to `evaluationID` in alert records (`gold/alerts.evaluation_id`) |
| `credttm` | Evaluation completion timestamp (NiFi watermark) |
| `document` | The full evaluation result payload, including the enriched pacs message with the `DataCache` block |
| `DataCache.cdtrId` | Creditor entity/party ID (resolved by TMS) |
| `DataCache.dbtrId` | Debtor entity/party ID (resolved by TMS) |
| `DataCache.cdtrAcctId` | Creditor account ID — authoritative source for `gold/pacs002.dc_cdtr_acct_id` |
| `DataCache.dbtrAcctId` | Debtor account ID — authoritative source for `gold/pacs002.dc_dbtr_acct_id` |
| `DataCache.creDtTm` | TMS-resolved creation datetime |
| `DataCache.instdAmt.amt` | TMS-resolved instructed amount |
| `DataCache.instdAmt.ccy` | TMS-resolved instructed currency |
| `DataCache.xchgRate` | Exchange rate |
| `DataCache.intrBkSttlmAmt.amt` | Interbank settlement amount |
| `DataCache.intrBkSttlmAmt.ccy` | Interbank settlement currency |

> **Note (current behavior)**: `evaluation.evaluation` is the canonical business source for DataCache. The TMS also writes DataCache back into `raw_history.pacs002` before storage, so `etl_pacs002()` correctly reads DataCache from `doc.DataCache.*` in the pacs.002 Ozone files. Only records processed end-to-end by the TMS will have populated `DataCache`; earlier or synthetic records will have null `dc_*` fields.

### Ozone → Lakehouse

**Status: Not yet implemented.**

`run_full_etl` has no handler for `table=evaluation`. When NiFi triggers the orchestrator with `table=evaluation`, the routing falls to the `else` branch and raises:

```python
raise ValueError(f"Unsupported table: {table}")
```

There is no `etl_evaluation()` function. The raw JSON files land in Ozone but are never promoted to Bronze, Silver, or Gold Hudi tables.

### Intended `dc_*` field lineage (once implemented)

Once `etl_evaluation()` is implemented, the DataCache fields should flow as:

```text
evaluation.evaluation
  DataCache.cdtrAcctId  ──ETL──▶ bronze/evaluation ──▶ silver/evaluation ──▶ gold/evaluation
  DataCache.dbtrAcctId                                                              │
  DataCache.cdtrId                                                                  │
  DataCache.dbtrId                                                                  │
  credttm (watermark)                                                               │
                                                                                    │
                       ──JOIN (endtoendid + tenantid)──▶ gold/pacs002 (dc_* columns)
                       ──JOIN (endtoendid + tenantid)──▶ gold/transactions (indirect)
                       ──JOIN (evaluationID)───────────▶ gold/alerts (evaluation_id)
```

**Current state**: `gold/pacs002` `dc_*` columns are populated by `etl_pacs002()` reading DataCache from the pacs.002 Ozone document (`doc.DataCache.*`). **Intended future state**: once `etl_evaluation()` is implemented, `dc_*` fields could alternatively be sourced by joining `gold/evaluation` to `gold/pacs002` on `end_to_end_id + tenant_id`.

### Field lineage summary (`evaluation`)

```text
evaluation.evaluation
  DataCache.cdtrAcctId  ──(intended)──▶ gold/pacs002.dc_cdtr_acct_id
                                     └──▶ gold/account.account_id (join key)
  DataCache.dbtrAcctId  ──(intended)──▶ gold/pacs002.dc_dbtr_acct_id
                                     └──▶ gold/account.account_id (join key)
  DataCache.cdtrId      ──(intended)──▶ gold/pacs002.dc_cdtr_id
                                     └──▶ gold/account_holder.counterparty_id (join key)
  DataCache.dbtrId      ──(intended)──▶ gold/pacs002.dc_dbtr_id
                                     └──▶ gold/account_holder.counterparty_id (join key)
  id (evaluationID)     ──(already)───▶ gold/alerts.evaluation_id
  credttm               ──watermark (not written to Hudi)
  [all other fields]    ──(not yet mapped)
```

---

## Summary Table

| Source table | Ozone bucket path | Bronze path | Silver path | Gold path | Status |
|---|---|---|---|---|---|
| `event_history.entity` | `{bucket}/entity/` | — | — | — | **Raw/Ozone only — no Hudi ETL** |
| `event_history.account_holder` | `{bucket}/account_holder/` | `bronze/account_holder` | `silver/account_holder` | `gold/account_holder` | ✅ Full pipeline |
| `event_history.account` | `{bucket}/account/` | `bronze/account` | `silver/account` | `gold/account` | ✅ Full pipeline (silver is structural pass-through) |
| `event_history.transaction` | `{bucket}/transaction/` | `bronze/transactions` | `silver/transactions` | `gold/transactions` | ✅ Full pipeline — **triggered by PACS pipeline**, not by transaction feed directly |
| `evaluation.evaluation` | `{bucket}/evaluation/` | — | — | — | **Raw/Ozone only — no Hudi ETL (authoritative DataCache source)** |

---

## Cross-Table Dependencies at Gold Layer

```text
gold/transactions
    ├── derives transactionData from: bronze/pacs008 (pacs.008.001.10 ISO 20022 payload)
    └── derives transactionData from: bronze/pacs002 (pacs.002.001.12 ISO 20022 payload)

evaluation.evaluation  (authoritative DataCache source — no Hudi ETL yet)
    └── DataCache.*  ──(intended join on endtoendid+tenantid)──▶  gold/pacs002 dc_* columns

gold/pacs002  (DataCache fields — currently read from raw_history.pacs002 document;
               intended source is evaluation.evaluation)
    ├── dc_cdtr_acct_id ──▶ gold/account.account_id   (creditor account, match on tenant_id)
    ├── dc_dbtr_acct_id ──▶ gold/account.account_id   (debtor account,   match on tenant_id)
    ├── dc_cdtr_id      ──▶ gold/account_holder.counterparty_id  (creditor entity)
    └── dc_dbtr_id      ──▶ gold/account_holder.counterparty_id  (debtor entity)

gold/account_holder
    └── account_id ──▶ gold/account.account_id  (account held by counterparty)

gold/account
    └── no cross-table joins (self-contained reference table)

gold/transactions ──(alert links)──▶ gold/alerts (via end_to_end_id / tx_msg_id)
```

### Resolving debtor/creditor accounts for a transaction

`gold/transactions` does **not** carry the `dc_*` account ID fields — they are only in `gold/pacs002`. To join a transaction to its account records:

```sql
SELECT
    t.transaction_id,
    t.end_to_end_id,
    t.tx_amount,
    t.tx_status,

    p.dc_cdtr_acct_id   AS cdtr_account_id,
    p.dc_cdtr_id        AS cdtr_party_id,
    p.dc_dbtr_acct_id   AS dbtr_account_id,
    p.dc_dbtr_id        AS dbtr_party_id,
    p.dc_instd_amt      AS dc_amount

FROM  gold_transactions  AS t

-- Step 1: link to pacs002 to get DataCache account IDs
INNER JOIN gold_pacs002  AS p
        ON  p.orgnl_end_to_end_id = t.end_to_end_id
        AND p.tx_tenant_id        = t.tenant_id

-- Step 2: resolve creditor account detail
LEFT  JOIN gold_account  AS cdtr_acct
        ON  cdtr_acct.account_id = p.dc_cdtr_acct_id
        AND cdtr_acct.tenant_id  = t.tenant_id

-- Step 3: resolve debtor account detail
LEFT  JOIN gold_account  AS dbtr_acct
        ON  dbtr_acct.account_id = p.dc_dbtr_acct_id
        AND dbtr_acct.tenant_id  = t.tenant_id
```

---

## Hudi Table Characteristics

All tables use `COPY_ON_WRITE` table type with `BLOOM` index (except Gold `transactions` and `alerts` which use `SimpleKeyGenerator` with date-based partitioning). Schema evolution is enabled on all tables (`hoodie.datasource.write.schema.evolution.enable=true`).

| Table | Record Key | Precombine | Partition | Key Generator |
|-------|-----------|------------|-----------|---------------|
| `bronze/account_holder` | `record_hash` | `ingested_at_ts` | none | `NonpartitionedKeyGenerator` |
| `silver/account_holder` | `pk` | `ingested_at_ts` | none | `NonpartitionedKeyGenerator` |
| `gold/account_holder` | `pk` | `ingested_at_ts` | none | `NonpartitionedKeyGenerator` |
| `bronze/account` | `record_hash` | `ingested_at_ts` | none | `NonpartitionedKeyGenerator` |
| `silver/account` | `record_hash` | `ingested_at_ts` | none | `NonpartitionedKeyGenerator` |
| `gold/account` | `pk` | `ingested_at_ts` | none | `NonpartitionedKeyGenerator` |
| `bronze/transactions` | `transaction_id` | `created_at_ts` | none | `NonpartitionedKeyGenerator` |
| `silver/transactions` | `transaction_id` | `created_at_ts` | none | `NonpartitionedKeyGenerator` |
| `gold/transactions` | `transaction_id` | `ingested_at_ts` | `event_date` | `SimpleKeyGenerator` |

---

## Known Gaps / Future Work

| Gap | Tables affected | Notes |
|-----|----------------|-------|
| `evaluation` table has no Hudi ETL | `evaluation.evaluation` | NiFi extracts to Ozone every minute but `run_full_etl` raises `ValueError` for `table=evaluation`. An `etl_evaluation()` function needs to be written and registered. This is the **highest-priority gap** because `evaluation.evaluation` is the only on-disk source of DataCache. |
| `dc_*` fields null for pre-TMS records | `gold/pacs002` | Records that were not processed end-to-end through the TMS (e.g. synthetic test data or messages ingested before the TMS pipeline was active) will have null `dc_*` fields. This is expected behaviour — only fully TMS-evaluated messages carry DataCache. |
| `entity` table has no Hudi ETL | `event_history.entity` | NiFi extracts to Ozone but `run_full_etl` explicitly skips it (`"Skipped: unsupported table"`). An `etl_entity()` function needs to be written and registered. |
| Source schema for `entity` is unknown | `event_history.entity` | The NiFi config fetches all columns but none are mapped anywhere in the pipeline. Schema must be traced from the source PostgreSQL DDL. |
| `account` silver is a structural pass-through | `event_history.account` | No normalisation, type casting, DQ rules, or enrichment is applied at silver. `credttm` watermark is not parsed to a timestamp. |
| `transaction` feed is bypassed in current mode | `event_history.transaction` | The Ozone transaction files are only consumed in legacy `"join"` mode. Current default is `"from_pacs"`, meaning `credttm` and `txtp` from `event_history.transaction` are unused. |
| No DQ / DLQ for `account`, `account_holder`, `transactions` | `account`, `account_holder`, `transaction` | Only `alerts` has a full DQ framework with reason codes and a DLQ path (`silver/alerts_dlq`). |
| No metrics layer for reference tables | `account`, `account_holder` | Reference tables do not feed into the `metrics/` layer. |
