# Verified version matrix

Every row was checked by installing and running it, not from memory. Where a
constraint comes from a package's own metadata, that is stated — those are
facts you can re-derive rather than trust.

## The pins

```
Python              3.11
JDK                 17
PySpark             3.5.9
delta-spark         3.3.3
Delta JAR           io.delta:delta-spark_2.12:3.3.3
dbt-core            1.12.3
dbt-spark[session]  1.10.3
dbt-databricks      1.12.5
astronomer-cosmos   1.15.1
```

## Why each constraint exists

**Python 3.11.** PySpark 3.5 is fully tested through 3.11. It *does* run on
3.12 — verified by creating a session and writing a Delta table — which matters
when a base image forces 3.12. It does not run on 3.14.

**JDK 17.** Spark 3.5 supports Java 8/11/17; Java 21 support arrived in Spark
4.0. The failure on 21 is an obscure reflection or module-access error, never a
clear version message. Search for a 17 JDK and *prefer it over an inherited*
`JAVA_HOME` — developer machines routinely carry several JDKs, and deferring to
the environment is how you end up debugging a supported setup that isn't used.

**delta-spark 3.3.3.** Its own metadata declares `pyspark>=3.5.3,<3.6.0`, so
3.5.9 is an exact fit. Delta 4.x requires Spark 4.

**dbt-core 1.12.3 + dbt-spark 1.10.3 + dbt-databricks 1.12.5.** This is the
non-obvious one. dbt-databricks 1.12.5 declares `dbt-core<1.12.4` and
`dbt-spark<1.11.0`. Taking the newest of each adapter makes them **mutually
uninstallable**, and one project serving two targets needs both in one
environment. Check the adapter's `requires_dist` before pinning; the caps move.

## Runtime images

**Astro Runtime is pinned by Python, not by Airflow.** `astro dev init`
scaffolds the newest Runtime; 3.3 ships Python 3.14, which PySpark 3.5.9 does
not support, so any Spark task fails at import. Runtime 13.11 ships 3.12 and
works. The general rule: when the image carries Spark, the Runtime version is
chosen by the Python version Spark supports.

**Debian bookworm ships OpenJDK 17** in its own archive, so
`python:3.11-slim-bookworm` plus `openjdk-17-jdk-headless` gives both
constraints from one trusted source with no third-party PPA.

## Configuration that is easy to get wrong

**dbt-spark's session method builds its own SparkSession** with a bare
`getOrCreate()` and offers no hook for `spark.jars.packages` or
`spark.sql.extensions`. Delta config must arrive out-of-band via
`SPARK_CONF_DIR` pointing at a repo-local `spark-defaults.conf`.

**That file cannot expand environment variables**, so its paths are relative
and resolve against the working directory. Running dbt from a subdirectory
silently creates a *second, empty* metastore and every model fails with an
opaque unresolved-relation error. Always invoke dbt from the project root
(`--project-dir`), and add an `on-run-start` guard that checks the source
schema is visible and explains the three likely causes.

**Stock Spark settings assume a cluster.** On a laptop, 200 shuffle partitions
and a 10,000-partition `VACUUM` file listing dominate runtime. Set
`spark.sql.shuffle.partitions=4`, `spark.databricks.delta.snapshotPartitions=4`,
and disable the console progress bar — it writes carriage returns that corrupt
any piped output, including proof scripts.

## OSS Delta vs Databricks, probed

Against Spark 3.5.9 / Delta 3.3.3, 8 of 9 operations supported locally:

| Operation | OSS Delta 3.3 |
|---|---|
| `OPTIMIZE`, `OPTIMIZE ... ZORDER BY` | works |
| `CLUSTER BY` on an **unpartitioned** table | works |
| `OPTIMIZE ... FULL` (re-cluster) | **works** |
| `VACUUM`, `DESCRIBE HISTORY`, time travel | works |
| `CLUSTER BY` on a **partitioned** table | refused |

Two corrections to common belief, both from running the probe:
`OPTIMIZE FULL` is **not** Databricks-only, and the one failure is **not** a
missing feature — clustering and Hive-style partitioning are mutually
exclusive. On Databricks the same table uses liquid clustering *instead of*
partitioning.

Genuinely proprietary: predictive optimization, `CLUSTER BY AUTO`,
auto-compaction defaults, default-on deletion vectors, Unity Catalog governance.

## Portability gotcha

`try_to_date` exists on Databricks and **not** in OSS Spark 3.5. A macro using
it works on one target and fails on the other. `to_date` plus a range guard
works on both. Expect more of these; a regression test on each macro is what
catches them.
