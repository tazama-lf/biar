"""
DynamicETL.py
-------------
Generic Bronze → Silver → Gold ETL that dynamically ingests any JSON payload,
auto-flattens nested structs / arrays / maps.

Hudi keys are determined by db_name:
    raw_history   : record_key=messageid,  precombine=credttm
    event_history : record_key=_key,       precombine=credttm
    enrichment    : record_key=id,         precombine=created_at

Table naming: {db_name}_{table} for all Hudi paths and table names.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StructType, StringType
from pyspark.sql.window import Window

from .BaseETL import BaseETL


class DynamicETL(BaseETL):
    """
    Domain-agnostic ETL pipeline.

    Parameters
    ----------
    spark : SparkSession
    warehouse_root : str
    db_name : str
        Database/namespace from NiFi (raw_history, event_history, enrichment)
    table : str
        Table name from NiFi (e.g. "test6", "alerts")
    """

    def __init__(
        self,
        spark,
        warehouse_root: str,
        db_name: str,
        table: str,
    ) -> None:
        super().__init__(spark, warehouse_root)
        self.db_name = (db_name or "").lower().strip()
        self.table = table
        self.hudi_table_name = f"{self.db_name}_{self.table}"

        # Config profiles by db_name — STRICT, no fallback
        self._config = self._resolve_config(self.db_name)

    def _resolve_config(self, db_name: str) -> dict:
        """Return Hudi key config for the given db_name. Raises RuntimeError if unknown."""
        configs = {
            "raw_history": {
                "record_key": "messageid",
                "precombine": "credttm",
                "canonical_cols": {"messageid": "string", "credttm": "long"},
            },
            "event_history": {
                "record_key": "_key",
                "precombine": "credttm",
                "canonical_cols": {"_key": "string", "credttm": "long"},
            },
            "enrichment": {
                "record_key": "id",
                "precombine": "created_at",
                "canonical_cols": {"id": "string", "created_at": "timestamp"},
            },
        }

        if db_name not in configs:
            raise RuntimeError(
                f"[DynamicETL] Unidentified db: '{db_name}'. "
                f"Cannot process. Supported db_names: {list(configs.keys())}"
            )

        return configs[db_name]

    # ------------------------------------------------------------------
    # PATHS  — namespaced by db_name_table
    # ------------------------------------------------------------------

    @property
    def bronze_path(self) -> str:
        return f"{self.warehouse_root}/bronze/{self.hudi_table_name}"

    @property
    def silver_path(self) -> str:
        return f"{self.warehouse_root}/silver/{self.hudi_table_name}"

    @property
    def gold_path(self) -> str:
        return f"{self.warehouse_root}/gold/{self.hudi_table_name}"

    # ------------------------------------------------------------------
    # HUDI CONFIG
    # ------------------------------------------------------------------

    def _hudi_opts(self, layer: str) -> dict:
        return self.hudi_opts(
            table_name=f"{layer}_{self.hudi_table_name}",
            record_key=self._config["record_key"],
            precombine=self._config["precombine"],
            partition=self._config["precombine"],
        )

    # ------------------------------------------------------------------
    # BRONZE
    # ------------------------------------------------------------------

    def bronze(self, source_path: str) -> str:
        raw = self.spark.read.json(source_path)

        # Guarantee canonical columns exist for this db_name profile
        df = self.ensure_columns(raw, self._config["canonical_cols"])

        pk = self._config["record_key"]
        pc = self._config["precombine"]

        # Cast canonical columns
        pk_type = self._config["canonical_cols"][pk]
        pc_type = self._config["canonical_cols"][pc]

        df = df.withColumn(pk, F.col(pk).cast(pk_type))
        df = df.withColumn(pc, F.col(pc).cast(pc_type))

        # DEFENSIVE: precombine key cannot be null for Hudi
        if pc_type == "long":
            df = df.withColumn(
                pc,
                F.coalesce(F.col(pc), (F.unix_timestamp(F.current_timestamp()) * 1000).cast("long")),
            )
        elif pc_type == "timestamp":
            df = df.withColumn(pc, F.coalesce(F.col(pc), F.current_timestamp()))

        # Metadata
        df = (
            df.withColumn("ingested_at_ts", F.current_timestamp())
            .withColumn("source_file_path", F.input_file_name())
        )

        meta_cols = {"ingested_at_ts", "source_file_path", "record_hash"}
        hash_cols = [c for c in df.columns if c not in meta_cols]

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

        self.write_hudi(df, self.bronze_path, self._hudi_opts("bronze"))
        print(f"[DynamicETL] Bronze written → {self.bronze_path}  (table={self.hudi_table_name})")
        return self.bronze_path

    # ------------------------------------------------------------------
    # SILVER  — flatten + dedup
    # ------------------------------------------------------------------

    def silver(self) -> str:
        bronze = self.spark.read.format("hudi").load(self.bronze_path)
        bronze = self.drop_hoodie_cols(bronze)

        flat = self._flatten_df_recursive(bronze)

        pk = self._config["record_key"]
        pc = self._config["precombine"]

        # Dedup: latest precombine wins per record key
        w = Window.partitionBy(pk).orderBy(F.col(pc).desc())
        silver = (
            flat.withColumn("_rn", F.row_number().over(w))
            .filter("_rn = 1")
            .drop("_rn")
        )

        self.write_hudi(silver, self.silver_path, self._hudi_opts("silver"))
        print(f"[DynamicETL] Silver written → {self.silver_path}  (table={self.hudi_table_name})")
        return self.silver_path

    # ------------------------------------------------------------------
    # GOLD  — dedup + enrichment + scalar guarantee
    # ------------------------------------------------------------------

    def gold(self) -> str:
        silver = self.spark.read.format("hudi").load(self.silver_path)
        silver = self.drop_hoodie_cols(silver)

        pk = self._config["record_key"]
        pc = self._config["precombine"]

        w = Window.partitionBy(pk).orderBy(F.col(pc).desc())
        gold = (
            silver.withColumn("_rn", F.row_number().over(w))
            .filter("_rn = 1")
            .drop("_rn")
        )

        # Enrich: epoch ms → timestamp / date (only for credttm)
        if pc == "credttm":
            gold = (
                gold.withColumn(
                    "event_ts",
                    F.when(
                        F.col(pc).isNotNull(),
                        (F.col(pc) / 1000).cast("timestamp"),
                    ).otherwise(F.lit(None)),
                )
                .withColumn("event_date", F.to_date("event_ts"))
            )
        elif pc == "created_at":
            gold = (
                gold.withColumn("event_ts", F.col(pc))
                .withColumn("event_date", F.to_date(pc))
            )

        # Safety-net: any remaining complex types → JSON strings
        for field in gold.schema.fields:
            if isinstance(field.dataType, (ArrayType, MapType, StructType)):
                gold = gold.withColumn(field.name, F.to_json(F.col(field.name)))

        self.write_hudi(gold, self.gold_path, self._hudi_opts("gold"))
        print(f"[DynamicETL] Gold written → {self.gold_path}  (table={self.hudi_table_name})")
        return self.gold_path

    # ------------------------------------------------------------------
    # ORCHESTRATOR
    # ------------------------------------------------------------------

    def run(self, source_path: str) -> str:
        print(f"[DynamicETL] Starting Bronze → Silver → Gold for table='{self.hudi_table_name}', db='{self.db_name}'")
        self.bronze(source_path)
        self.silver()
        self.gold()
        print(f"[DynamicETL] ETL complete for table='{self.hudi_table_name}'.")
        return self.gold_path

    # ------------------------------------------------------------------
    # FLATTEN HELPERS  — recursive: structs + JSON strings
    # ------------------------------------------------------------------

    def _flatten_df_recursive(self, df: DataFrame) -> DataFrame:
        max_iterations = 50
        iteration = 0

        while iteration < max_iterations:
            iteration += 1
            changed = False

            # Step 1: Flatten direct structs
            struct_cols = [
                f.name for f in df.schema.fields if isinstance(f.dataType, StructType)
            ]
            if struct_cols:
                changed = True
                for col_name in struct_cols:
                    sub_fields = df.schema[col_name].dataType.fields
                    expanded = [
                        F.col(f"{col_name}.{sub.name}").alias(f"{col_name}_{sub.name}")
                        for sub in sub_fields
                    ]
                    other = [F.col(c) for c in df.columns if c != col_name]
                    df = df.select(other + expanded)

            # Step 2: Parse JSON strings into structs/arrays
            json_string_cols = []
            for field in df.schema.fields:
                if isinstance(field.dataType, StringType):
                    sample = (
                        df.filter(F.col(field.name).isNotNull())
                        .select(F.col(field.name))
                        .limit(1)
                        .collect()
                    )
                    if sample and len(sample) > 0:
                        val = sample[0][0]
                        if val and isinstance(val, str):
                            val_stripped = val.strip()
                            if (val_stripped.startswith('{') and val_stripped.endswith('}')) or \
                               (val_stripped.startswith('[') and val_stripped.endswith(']')):
                                json_string_cols.append(field.name)

            if json_string_cols:
                changed = True
                for col_name in json_string_cols:
                    try:
                        json_schema = self.infer_json_schema(df, col_name)
                        df = df.withColumn(col_name, F.from_json(F.col(col_name), json_schema))
                    except Exception:
                        try:
                            df = df.withColumn(col_name, F.from_json(F.col(col_name), "STRUCT<<*>"))
                        except:
                            pass

            # ------------------------------------------------------------------
            # Step 2c: Sanitize column names — strip whitespace, replace invalid chars
            # Avro/Hudi doesn't allow spaces, trailing whitespace, or special chars
            # ------------------------------------------------------------------
            rename_map = {}
            for col_name in df.columns:
                clean = col_name.strip().replace(" ", "_").replace("-", "_")
                if clean != col_name:
                    rename_map[col_name] = clean

            if rename_map:
                changed = True
                for old_name, new_name in rename_map.items():
                    df = df.withColumnRenamed(old_name, new_name)

            if not changed:
                break

        # Step 3: Serialize remaining complex types to JSON
        complex_cols = [
            f.name
            for f in df.schema.fields
            if isinstance(f.dataType, (ArrayType, MapType, StructType))
        ]
        for col_name in complex_cols:
            df = df.withColumn(col_name, F.to_json(F.col(col_name)))

        return df