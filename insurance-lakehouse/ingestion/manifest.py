"""Processed-file manifest and schema-drift audit, both stored as Delta tables.

This is the local stand-in for Databricks Auto Loader's ``cloudFiles``
checkpoint (ADR-0006). A file is identified by ``(path, size, modified_time)``
rather than path alone, so a partner that overwrites yesterday's file with
corrected content is correctly re-ingested instead of being silently skipped.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .config import MANIFEST_TABLE, SCHEMA_AUDIT_TABLE

MANIFEST_SCHEMA = StructType(
    [
        StructField("source_name", StringType(), False),
        StructField("file_path", StringType(), False),
        StructField("file_size", LongType(), False),
        StructField("file_modified_at", TimestampType(), False),
        StructField("batch_id", StringType(), False),
        StructField("batch_date", StringType(), True),
        StructField("row_count", LongType(), False),
        StructField("quarantined_count", LongType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("status", StringType(), False),
    ]
)

SCHEMA_AUDIT_SCHEMA = StructType(
    [
        StructField("source_name", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("added_columns", StringType(), True),
        StructField("removed_columns", StringType(), True),
        StructField("previous_column_count", LongType(), True),
        StructField("new_column_count", LongType(), True),
        StructField("detected_at", TimestampType(), False),
    ]
)


@dataclass(frozen=True)
class FileRef:
    """A landing-zone file plus the attributes that define its identity."""

    path: str
    size: int
    modified_at: datetime

    @classmethod
    def from_path(cls, path: str) -> FileRef:
        st = os.stat(path)
        return cls(
            path=os.path.abspath(path),
            size=st.st_size,
            modified_at=datetime.fromtimestamp(st.st_mtime, tz=UTC),
        )

    @property
    def key(self) -> tuple[str, int, int]:
        """Identity used for dedup: path, size and mtime truncated to seconds.

        Size alone would miss a same-length corrected resend; sub-second mtime
        would survive the Delta round-trip badly and make every re-read look
        like a new file. Whole seconds is the stable middle ground.
        """
        return (self.path, self.size, int(self.modified_at.timestamp()))


def _table_path(bronze_path: str, table: str) -> str:
    return os.path.abspath(os.path.join(bronze_path, table))


def ensure_tables(spark: SparkSession, bronze_path: str, schema: str) -> None:
    """Create the bronze database and the two control tables if absent."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    for table, struct in (
        (MANIFEST_TABLE, MANIFEST_SCHEMA),
        (SCHEMA_AUDIT_TABLE, SCHEMA_AUDIT_SCHEMA),
    ):
        path = _table_path(bronze_path, table)
        if not os.path.exists(os.path.join(path, "_delta_log")):
            spark.createDataFrame([], struct).write.format("delta").mode("overwrite").save(path)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {schema}.{table} USING DELTA LOCATION '{path}'")


def processed_keys(
    spark: SparkSession, bronze_path: str, source_name: str
) -> set[tuple[str, int, int]]:
    """Files already ingested successfully for this source, as FileRef keys."""
    path = _table_path(bronze_path, MANIFEST_TABLE)
    rows = (
        spark.read.format("delta")
        .load(path)
        .where((F.col("source_name") == source_name) & (F.col("status") == "INGESTED"))
        .select("file_path", "file_size", "file_modified_at")
        .collect()
    )
    return {(r["file_path"], r["file_size"], int(r["file_modified_at"].timestamp())) for r in rows}


def record(spark: SparkSession, bronze_path: str, rows: list[dict]) -> None:
    if not rows:
        return
    df = spark.createDataFrame(rows, MANIFEST_SCHEMA)
    df.write.format("delta").mode("append").save(_table_path(bronze_path, MANIFEST_TABLE))


def record_drift(spark: SparkSession, bronze_path: str, rows: list[dict]) -> None:
    if not rows:
        return
    df = spark.createDataFrame(rows, SCHEMA_AUDIT_SCHEMA)
    df.write.format("delta").mode("append").save(_table_path(bronze_path, SCHEMA_AUDIT_TABLE))


def manifest_df(spark: SparkSession, bronze_path: str) -> DataFrame:
    return spark.read.format("delta").load(_table_path(bronze_path, MANIFEST_TABLE))


def drift_df(spark: SparkSession, bronze_path: str) -> DataFrame:
    return spark.read.format("delta").load(_table_path(bronze_path, SCHEMA_AUDIT_TABLE))
