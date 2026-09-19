"""Delta maintenance: OPTIMIZE, ZORDER, clustering, VACUUM and time travel.

This module *probes* rather than asserts, because which Delta features work in
OSS is widely misreported -- and the probe corrected two assumptions made while
writing this repo:

* "Liquid clustering is Databricks-only" is wrong. OSS Delta 3.3 accepts
  ``CLUSTER BY`` and ``OPTIMIZE FULL``. What it will not do is apply clustering
  to a *partitioned* table: clustering and Hive-style partitioning are mutually
  exclusive, and the first probe fails for that reason alone, not for lack of
  the feature.
* ``OPTIMIZE FULL`` is therefore **not** Databricks-only either; it simply
  requires a table with non-empty clustering columns.

Each operation is attempted and its real outcome reported, so the capability
matrix in the docs reflects the pinned versions rather than folklore.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from pyspark.sql import SparkSession

# Genuinely proprietary: verified absent from OSS Delta 3.3 rather than assumed.
# Anything the probe can actually execute locally does not belong in this dict.
DATABRICKS_ONLY = {
    "Predictive optimization": "Databricks auto-runs OPTIMIZE/VACUUM from usage telemetry.",
    "Auto Liquid Clustering (CLUSTER BY AUTO)": "Databricks picks clustering keys for you.",
    "Managed-table auto-compaction defaults": "On by default on DBR; opt-in in OSS.",
    "Deletion vectors on by default": "Default-on for DBR managed tables.",
    "Unity Catalog governance (tags/masks/filters)": "No OSS metastore equivalent.",
}


@dataclass
class ProbeResult:
    operation: str
    supported: bool
    detail: str

    def render(self) -> str:
        mark = "OK  " if self.supported else "FAIL"
        return f"  [{mark}] {self.operation:<34} {self.detail}"


def _probe(spark: SparkSession, operation: str, sql: str) -> ProbeResult:
    try:
        spark.sql(sql)
        return ProbeResult(operation, True, "supported in OSS Delta")
    except Exception as exc:
        msg = str(exc).split("\n")[0][:110]
        return ProbeResult(operation, False, msg)


def run_maintenance(
    spark: SparkSession, table_path: str, zorder_by: list[str], vacuum_hours: int = 0
) -> list[ProbeResult]:
    """Exercise the maintenance surface against one Delta table."""
    abs_path = os.path.abspath(table_path)
    ref = f"delta.`{abs_path}`"
    results: list[ProbeResult] = []

    results.append(_probe(spark, "DESCRIBE HISTORY", f"DESCRIBE HISTORY {ref}"))
    results.append(_probe(spark, "OPTIMIZE (bin-packing)", f"OPTIMIZE {ref}"))

    if zorder_by:
        cols = ", ".join(zorder_by)
        results.append(
            _probe(spark, f"OPTIMIZE ZORDER BY ({cols})", f"OPTIMIZE {ref} ZORDER BY ({cols})")
        )

    # Clustered tables. OSS Delta 3.x DOES support CLUSTER BY -- it is the
    # storage layout underneath what Databricks markets as liquid clustering.
    # The catch is that clustering and Hive-style partitioning are mutually
    # exclusive, so this probes twice: once against the real (partitioned)
    # bronze table, once against an unpartitioned copy. Reporting only the
    # first would wrongly imply OSS Delta lacks the feature entirely.
    if zorder_by:
        results.append(
            _probe(
                spark,
                "CLUSTER BY on PARTITIONED table",
                f"ALTER TABLE {ref} CLUSTER BY ({zorder_by[0]})",
            )
        )
        results.extend(_probe_clustering_unpartitioned(spark, abs_path, zorder_by[0]))

    results.append(
        _probe(
            spark,
            f"VACUUM RETAIN {vacuum_hours} HOURS",
            f"VACUUM {ref} RETAIN {vacuum_hours} HOURS",
        )
    )

    # Time travel: read the table as at its first version.
    try:
        n0 = spark.read.format("delta").option("versionAsOf", 0).load(abs_path).count()
        n_now = spark.read.format("delta").load(abs_path).count()
        results.append(
            ProbeResult("Time travel (versionAsOf 0)", True, f"v0 rows={n0}, current rows={n_now}")
        )
    except Exception as exc:
        results.append(ProbeResult("Time travel (versionAsOf 0)", False, str(exc)[:110]))

    return results


def _probe_clustering_unpartitioned(
    spark: SparkSession, source_path: str, cluster_col: str
) -> list[ProbeResult]:
    """Re-probe clustering against an unpartitioned copy of the table.

    Bronze partitions by _batch_date, which rules clustering out. On Databricks
    the same table would use liquid clustering *instead of* partitioning, so
    this shows what that path really supports under the pinned OSS version.
    """
    tmp = os.path.join(os.path.dirname(source_path.rstrip("/")), "_cluster_probe")
    try:
        (
            spark.read.format("delta")
            .load(source_path)
            .limit(500)
            .write.format("delta")
            .mode("overwrite")
            .save(tmp)
        )
    except Exception as exc:
        return [ProbeResult("CLUSTER BY on UNPARTITIONED table", False, str(exc)[:110])]

    ref = f"delta.`{tmp}`"
    return [
        _probe(
            spark,
            "CLUSTER BY on UNPARTITIONED table",
            f"ALTER TABLE {ref} CLUSTER BY ({cluster_col})",
        ),
        _probe(spark, "OPTIMIZE on clustered table", f"OPTIMIZE {ref}"),
        _probe(spark, "OPTIMIZE FULL (re-cluster)", f"OPTIMIZE {ref} FULL"),
    ]


def history(spark: SparkSession, table_path: str, limit: int = 5):
    abs_path = os.path.abspath(table_path)
    return (
        spark.sql(f"DESCRIBE HISTORY delta.`{abs_path}`")
        .select("version", "timestamp", "operation", "operationMetrics")
        .limit(limit)
    )
