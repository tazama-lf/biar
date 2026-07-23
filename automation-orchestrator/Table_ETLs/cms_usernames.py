"""
CmsUsernamesETL.py
------------------
Simple pass-through ETL for CMS usernames.
No transformations — just adds lineage metadata and writes to gold.
"""

from __future__ import annotations

from pyspark.sql import functions as F

from .BaseETL import BaseETL


class CmsUsernamesETL(BaseETL):
    """Pass-through ETL: raw JSON → gold with lineage metadata."""

    TABLE_NAME = "cms_usernames"

    @property
    def gold_path(self) -> str:
        return f"{self.warehouse_root}/gold/{self.TABLE_NAME}"

    def bronze(self, source_path: str) -> str:
        return self.gold_path  # no-op

    def silver(self) -> str:
        return self.gold_path  # no-op

    def gold(self) -> str:
        return self.gold_path  # no-op

    def run(self, source_path: str) -> str:
        print(f"[CmsUsernamesETL] Ingesting {source_path} → {self.gold_path}")

        raw = self.spark.read.json(source_path)

        df = (
            raw
            .withColumn("ingested_at_ts", F.current_timestamp())
            .withColumn("source_file_path", F.input_file_name())
            .withColumn(
                "record_hash",
                F.sha2(
                    F.concat_ws(
                        "||",
                        *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in raw.columns],
                    ),
                    256,
                ),
            )
        )

        self.write_hudi(
            df,
            self.gold_path,
            self.hudi_opts(
                table_name=self.TABLE_NAME,
                record_key="id",
                precombine="created_at",
            ),
        )

        print(f"[CmsUsernamesETL] Gold written → {self.gold_path}")
        return self.gold_path