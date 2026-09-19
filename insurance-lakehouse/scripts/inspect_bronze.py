#!/usr/bin/env python
"""Dump the state of bronze: tables, manifest, schema-drift audit, quarantine.

Used by `make demo` and by hand when checking what an ingestion run actually did.
"""

from __future__ import annotations

import contextlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from ingestion.config import load_registry
from ingestion.manifest import drift_df, manifest_df
from ingestion.session import get_spark


def main() -> int:
    registry = load_registry()
    spark = get_spark("bronze-inspect")

    print("\n=== BRONZE TABLES ===")
    total = 0
    for s in registry.sources:
        try:
            n = spark.read.format("delta").load(f"{registry.bronze_path}/{s.name}").count()
        except Exception:
            n = -1
        total += max(0, n)
        q = 0
        # A quarantine table only exists once something has been quarantined.
        with contextlib.suppress(Exception):
            q = (
                spark.read.format("delta")
                .load(f"{registry.bronze_path}/{s.quarantine_table()}")
                .count()
            )
        flag = f"  quarantine={q}" if q else ""
        print(f"  {s.name:<26} rows={n:<8}{flag}")
    print(f"  {'TOTAL':<26} rows={total}")

    print("\n=== MANIFEST (files processed per source per batch) ===")
    (
        manifest_df(spark, registry.bronze_path)
        .groupBy("source_name", "batch_id")
        .agg(F.count("*").alias("files"), F.sum("row_count").alias("rows"))
        .orderBy("batch_id", "source_name")
        .show(40, truncate=False)
    )

    print("=== SCHEMA DRIFT AUDIT ===")
    d = drift_df(spark, registry.bronze_path)
    if d.count() == 0:
        print("  (none recorded)\n")
    else:
        d.select(
            "source_name",
            "added_columns",
            "removed_columns",
            "previous_column_count",
            "new_column_count",
        ).show(20, truncate=False)

    print("=== QUARANTINE REASONS ===")
    found = False
    for s in registry.sources:
        try:
            qdf = spark.read.format("delta").load(f"{registry.bronze_path}/{s.quarantine_table()}")
        except Exception:
            continue
        found = True
        for r in qdf.groupBy("_quarantine_reason").count().collect():
            print(f"  {s.name:<26} {r['_quarantine_reason']:<22} {r['count']}")
    if not found:
        print("  (no quarantined records)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
