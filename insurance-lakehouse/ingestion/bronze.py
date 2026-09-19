"""Config-driven bronze ingestion.

Design rules, in priority order:

1. **Land raw.** Bronze does not clean, cast or reshape. It adds metadata
   columns and nothing else. Cleaning is silver's job, and keeping the two
   separate is what makes a bad silver rule recoverable without re-ingesting.
2. **Idempotent.** Re-running a batch must not duplicate rows. Enforced by the
   file manifest, not by hoping the caller behaves.
3. **Generic.** No per-source code. Everything is driven by
   ``config/sources.yml``.
4. **Quarantine narrowly.** Only records that cannot be landed at all -- an
   unparseable line, or one with no primary key to merge on later -- go to
   quarantine. Business-rule violations (negative premium, orphan FK) are
   *supposed* to reach silver so dbt tests can catch them.
"""

from __future__ import annotations

import glob
import os
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import manifest
from .config import (
    CORRUPT_COLUMN,
    META_BATCH_DATE,
    META_BATCH_ID,
    META_INGESTED_AT,
    META_RECORD_HASH,
    META_SOURCE_FILE,
    META_SOURCE_NAME,
    METADATA_COLUMNS,
    Source,
    SourceRegistry,
    expand_braces,
)
from .manifest import FileRef


@dataclass
class SourceResult:
    source: str
    status: str
    files_seen: int = 0
    files_new: int = 0
    rows_ingested: int = 0
    rows_quarantined: int = 0
    drift: dict[str, list[str]] = field(default_factory=dict)

    def as_row(self) -> str:
        bits = [f"{self.source:<26} {self.status:<14}"]
        bits.append(f"files {self.files_new}/{self.files_seen}")
        bits.append(f"rows {self.rows_ingested}")
        if self.rows_quarantined:
            bits.append(f"quarantined {self.rows_quarantined}")
        if self.drift.get("added"):
            bits.append(f"drift +{','.join(self.drift['added'])}")
        return "  ".join(bits)


def new_batch_id() -> str:
    return f"{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------- discovery
def discover_files(registry: SourceRegistry, source: Source) -> list[FileRef]:
    refs: list[FileRef] = []
    for pattern in expand_braces(os.path.join(registry.landing_root, source.path)):
        for p in glob.glob(pattern):
            if os.path.isfile(p):
                refs.append(FileRef.from_path(p))
    return sorted(refs, key=lambda r: r.path)


def _header_signature(path: str) -> str:
    """First line of a CSV, used to group files that share a schema."""
    with open(path, encoding="utf-8") as fh:
        return fh.readline().strip()


# ------------------------------------------------------------------ reading
def _read_csv_group(spark: SparkSession, source: Source, paths: list[str]) -> DataFrame:
    opts = dict(source.reader_options)
    opts.setdefault("header", "true")
    # Everything lands as string. Bronze must not guess types: a column that is
    # numeric on Monday and blank on Tuesday would flip schema under inference.
    opts["inferSchema"] = "false"
    return spark.read.options(**opts).csv(paths)


def read_source(
    spark: SparkSession, source: Source, refs: list[FileRef]
) -> tuple[DataFrame, list[str]]:
    """Read the given files, returning the frame and the union of columns.

    CSV files are grouped by header signature and unioned with
    ``allowMissingColumns``. Spark's CSV reader takes its schema from a sample
    of files, so a drifted header in a later file would otherwise be silently
    dropped -- grouping makes the drift explicit instead.
    """
    paths = [r.path for r in refs]
    if source.format == "json":
        opts = dict(source.reader_options)
        opts.setdefault("columnNameOfCorruptRecord", CORRUPT_COLUMN)
        df = spark.read.options(**opts).json(paths)
        return df, df.columns

    groups: dict[str, list[str]] = {}
    for p in paths:
        groups.setdefault(_header_signature(p), []).append(p)

    frames = [_read_csv_group(spark, source, sorted(g)) for g in groups.values()]
    df = frames[0]
    for nxt in frames[1:]:
        df = df.unionByName(nxt, allowMissingColumns=True)
    return df, df.columns


# --------------------------------------------------------------- decoration
def add_metadata(df: DataFrame, source: Source, batch_id: str, pattern: str) -> DataFrame:
    business = [c for c in df.columns if c not in METADATA_COLUMNS and c != CORRUPT_COLUMN]
    hash_input = F.concat_ws(
        "||", *[F.coalesce(F.col(f"`{c}`").cast("string"), F.lit("<null>")) for c in business]
    )
    return (
        df.withColumn(META_SOURCE_FILE, F.input_file_name())
        .withColumn(META_SOURCE_NAME, F.lit(source.name))
        .withColumn(META_BATCH_ID, F.lit(batch_id))
        .withColumn(META_INGESTED_AT, F.current_timestamp())
        # Hashed over business columns only, so the same record resent in a
        # later file hashes identically and silver can collapse it.
        .withColumn(META_RECORD_HASH, F.sha2(hash_input, 256))
        .withColumn(META_BATCH_DATE, F.regexp_extract(F.col(META_SOURCE_FILE), pattern, 1))
    )


def split_quarantine(df: DataFrame, source: Source) -> tuple[DataFrame, DataFrame]:
    """Separate records that cannot be landed from those that can."""
    reason = F.lit(None).cast("string")
    if CORRUPT_COLUMN in df.columns:
        reason = F.when(F.col(CORRUPT_COLUMN).isNotNull(), F.lit("unparseable_record"))
    present_pk = [c for c in source.primary_key if c in df.columns]
    if present_pk:
        missing_pk = None
        for col in present_pk:
            cond = F.col(f"`{col}`").isNull() | (F.trim(F.col(f"`{col}`").cast("string")) == "")
            missing_pk = cond if missing_pk is None else (missing_pk | cond)
        reason = F.coalesce(reason, F.when(missing_pk, F.lit("missing_primary_key")))

    tagged = df.withColumn("_quarantine_reason", reason)
    good = tagged.where(F.col("_quarantine_reason").isNull()).drop("_quarantine_reason")
    bad = tagged.where(F.col("_quarantine_reason").isNotNull())
    return good, bad


# ------------------------------------------------------------------ writing
def _existing_columns(spark: SparkSession, path: str) -> list[str] | None:
    if not os.path.exists(os.path.join(path, "_delta_log")):
        return None
    return spark.read.format("delta").load(path).columns


def _write(df: DataFrame, path: str, partition_by: list[str]) -> None:
    writer = df.write.format("delta").mode("append").option("mergeSchema", "true")
    usable = [c for c in partition_by if c in df.columns]
    if usable:
        writer = writer.partitionBy(*usable)
    writer.save(path)


def _register(spark: SparkSession, schema: str, table: str, path: str) -> None:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {schema}.{table} USING DELTA LOCATION '{path}'")


# -------------------------------------------------------------------- main
def ingest_source(
    spark: SparkSession, registry: SourceRegistry, source: Source, batch_id: str
) -> SourceResult:
    refs = discover_files(registry, source)
    result = SourceResult(source=source.name, status="NO_FILES", files_seen=len(refs))
    if not refs:
        return result

    already = manifest.processed_keys(spark, registry.bronze_path, source.name)
    new_refs = [r for r in refs if r.key not in already]
    result.files_new = len(new_refs)
    if not new_refs:
        result.status = "UP_TO_DATE"
        return result

    df, columns = read_source(spark, source, new_refs)
    table_path = os.path.abspath(os.path.join(registry.bronze_path, source.name))
    quarantine_path = os.path.abspath(os.path.join(registry.bronze_path, source.quarantine_table()))

    # --- schema drift -------------------------------------------------
    previous = _existing_columns(spark, table_path)
    drift_rows: list[dict] = []
    if previous is not None:
        incoming = set(columns) | set(METADATA_COLUMNS)
        added = sorted(incoming - set(previous))
        removed = sorted(set(previous) - incoming)
        if added or removed:
            result.drift = {"added": added, "removed": removed}
            drift_rows.append(
                {
                    "source_name": source.name,
                    "batch_id": batch_id,
                    "added_columns": ",".join(added) or None,
                    "removed_columns": ",".join(removed) or None,
                    "previous_column_count": len(previous),
                    "new_column_count": len(incoming | set(previous)),
                    "detected_at": datetime.now(UTC),
                }
            )

    decorated = add_metadata(df, source, batch_id, registry.batch_date_pattern)
    good, bad = split_quarantine(decorated, source)

    good = good.cache()
    result.rows_ingested = good.count()
    _write(good, table_path, source.partition_by)
    _register(spark, registry.bronze_schema, source.name, table_path)

    bad = bad.cache()
    result.rows_quarantined = bad.count()
    if result.rows_quarantined:
        _write(bad, quarantine_path, [])
        _register(spark, registry.bronze_schema, source.quarantine_table(), quarantine_path)

    manifest.record_drift(spark, registry.bronze_path, drift_rows)
    manifest.record(
        spark,
        registry.bronze_path,
        [
            {
                "source_name": source.name,
                "file_path": r.path,
                "file_size": r.size,
                "file_modified_at": r.modified_at,
                "batch_id": batch_id,
                "batch_date": _batch_date_of(r.path, registry.batch_date_pattern),
                "row_count": result.rows_ingested,
                "quarantined_count": result.rows_quarantined,
                "ingested_at": datetime.now(UTC),
                "status": "INGESTED",
            }
            for r in new_refs
        ],
    )

    good.unpersist()
    bad.unpersist()
    result.status = "INGESTED"
    return result


def _batch_date_of(path: str, pattern: str) -> str | None:
    import re

    m = re.search(pattern, path)
    return m.group(1) if m else None


def ingest_all(
    spark: SparkSession,
    registry: SourceRegistry,
    only: list[str] | None = None,
    batch_id: str | None = None,
) -> list[SourceResult]:
    batch_id = batch_id or new_batch_id()
    manifest.ensure_tables(spark, registry.bronze_path, registry.bronze_schema)
    return [ingest_source(spark, registry, source, batch_id) for source in registry.select(only)]
