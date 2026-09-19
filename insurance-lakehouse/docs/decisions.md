# Decision log

ADR-style. Each entry records the decision, why it was taken, what was rejected,
and — where the answer was determined empirically rather than from experience —
the evidence.

---

## ADR-0001 — Build under `insurance-lakehouse/` rather than the repo root

**Status:** accepted

**Context.** The brief assumed an empty repository. This repository already
contains the BUILD Hackathon "VC KPI Reporting Tool" (Reflex + FastAPI, Python
3.13) with its own `README.md`, `pyproject.toml`, `backend/`, `frontend/`,
`commons/`, `src/` and `rxconfig.py`.

**Decision.** The skeleton lives in `insurance-lakehouse/`, with the brief's
structure reproduced verbatim inside it.

**Why.** Placing `README.md`, `pyproject.toml`, `Makefile` and `CLAUDE.md` at the
root would overwrite the existing project. The two also cannot share one
`pyproject.toml`: Reflex targets Python 3.13 while PySpark 3.5 is only supported
through 3.11, so a single dependency set is not satisfiable.

**Consequences.** Commands run from `insurance-lakehouse/`. Two files must still
live at the repository root to function at all — GitHub only reads workflows
from `.github/workflows/`, and pre-commit only reads `.pre-commit-config.yaml`
from the root — so both are placed there and scoped to this subtree with path
filters.

**Rejected.** Coexisting at the root (dependency conflict, confusing Makefile);
replacing the hackathon project (destroys the user's work).

---

## ADR-0002 — Python 3.11 / JDK 17 / Spark 3.5.9 / Delta 3.3.3

**Status:** accepted

**Decision.** Pin the runtime to Python 3.11, JDK 17, PySpark 3.5.9,
delta-spark 3.3.3, dbt-core 1.12.5 and dbt-spark[session] 1.11.0.

**Why, with evidence.** Each constraint was checked rather than assumed:

- `delta-spark` 3.3.3 declares `pyspark>=3.5.3,<3.6.0` in its own metadata, so
  3.5.9 is an exact fit. Delta 4.x requires Spark 4.
- Spark 3.5 supports Java 8/11/17. Java 21 support arrived in Spark 4.0. The
  build machine had only JDK 21, so JDK 17 was installed and the entire smoke
  test was run on it.
- `dbt-spark` 1.11.0 declares `dbt-core>=1.8,<2.0` and `pyspark>=3.0,<5.0`;
  installing it with dbt-core 1.12.5 and pyspark 3.5.9 resolved with no
  conflict.
- PySpark 3.5 is only fully tested through Python 3.11, and `elementary-data`
  caps at `<3.14`. 3.11 satisfies both.

**Verified behaviour** (Spark 3.5.9 + Delta 3.3.3 on JDK 17): `MERGE`,
`OPTIMIZE ... ZORDER BY`, `VACUUM`, and `versionAsOf` time travel all succeed.
Via dbt-spark's session method: `dbt debug` passes and both a table model and an
incremental `merge` model build against Delta.

**Rejected.** Spark 4.0 / Delta 4.x — the dbt-spark and Cosmos ecosystems are
still settling on it, and Databricks LTS runtimes track Spark 3.5, so 3.5 is
also the closer mirror of the target platform.

---

## ADR-0003 — Inject Delta configuration through `SPARK_CONF_DIR`

**Status:** accepted

**Context.** dbt-spark's `session` method constructs its own SparkSession with a
bare `SparkSession.builder.getOrCreate()`. It offers no hook for
`spark.jars.packages` or `spark.sql.extensions`, so the Delta catalog and
extensions cannot be passed through the dbt profile.

**Decision.** Ship a repo-local `conf/spark-defaults.conf` and point
`SPARK_CONF_DIR` at it from the Makefile. Both the PySpark ingestion jobs and
dbt's session pick up identical configuration from one file.

**Why not the alternatives.** Writing into `site-packages/pyspark/conf/` works
but mutates an installed package and does not survive reinstall.
`PYSPARK_SUBMIT_ARGS` applies only to the driver launch path and is easy to get
wrong. A single tracked config file is the version-controllable option.

**Side benefit.** It is also where laptop tuning lives. With stock settings a
`VACUUM` on a 100-row table fanned out to a 10,000-partition job and the smoke
test took 92 seconds; `spark.sql.shuffle.partitions=4` and
`spark.databricks.delta.snapshotPartitions=4` remove that.

---

## ADR-0004 — Prefer a discovered JDK 17 over an inherited `JAVA_HOME`

**Status:** accepted

**Context.** The Makefile initially used `JAVA_HOME ?= <detection>`, which defers
to an inherited value. On the build machine `JAVA_HOME` pointed at Java 21, so
detection never ran and `make check-java` failed despite a valid JDK 17 being
installed.

**Decision.** Always search for a 17 JDK (macOS `/usr/libexec/java_home -v 17`,
Linux `/usr/lib/jvm/*17*`) and prefer it, falling back to the inherited
`JAVA_HOME` only when no 17 JDK exists.

**Why it matters beyond this machine.** Developer laptops routinely carry
several JDKs, and the failure mode when Spark 3.5 runs on Java 21 is an obscure
reflection or module-access error rather than a clear version message.

---

## ADR-0005 — Databricks Free Edition as a second, genuinely tested target

**Status:** accepted

**Context.** A Databricks Free Edition workspace is available, so the
`databricks` dbt profile need not remain an untested set of placeholders.

**Decision.** Local Spark stays the default target and the CI path. Databricks
becomes a second target that is actually exercised: the same dbt project builds
against it, and `governance/unity_catalog.sql` is executed against Unity Catalog
rather than merely generated.

**Why not Databricks-first.** Free Edition is serverless-only with usage limits.
Making it primary would (a) remove the offline, reproducible `make all` that
makes the skeleton usable on day 1 of an assignment, (b) break CI, and (c) force
the PySpark bronze layer to be rewritten as notebooks or jobs, since serverless
compute cannot be configured with custom Spark settings or JARs.

**Consequence.** Two profiles, one dbt project. What Free Edition does and does
not permit is probed empirically and recorded in `docs/databricks_port.md`
rather than assumed.

---

## ADR-0006 — A manifest table, not a checkpoint, for bronze idempotency

**Status:** accepted

**Context.** Databricks Auto Loader tracks which files it has consumed in a
RocksDB checkpoint. There is no OSS equivalent that behaves the same way, but
re-running a batch must not duplicate rows.

**Decision.** A Delta table, `bronze._ingestion_manifest`, records every file
ingested. Before each run, files matching a source's glob are listed and those
already present in the manifest are skipped.

**File identity is `(path, size, mtime-to-the-second)`, not path alone.** Path
alone would permanently skip a partner who overwrites yesterday's file with
corrected content — a common real failure. Sub-second mtime was rejected because
it does not survive the Delta round-trip cleanly and would make every re-read
look like a new file.

**Consequences.** `make bronze` is safely re-runnable; the second run reports
`UP_TO_DATE` with zero new rows, which is asserted by
`test_ingest_is_idempotent`. The manifest doubles as the run log the
observability layer reads in Phase 3.

---

## ADR-0007 — Quarantine only what cannot be landed

**Status:** accepted

**Decision.** Bronze quarantines exactly two classes of record:

| Reason | Example |
|---|---|
| `unparseable_record` | A non-JSON line in a JSONL stream |
| `missing_primary_key` | A truncated CSV row with no key to merge on later |

Everything else lands, including negative premiums, claims outside their
coverage period, and orphan foreign keys.

**Why.** Bronze's job is to be a faithful, replayable copy of what arrived.
Business-rule violations are *signal*: they are what the dbt tests in silver
exist to detect, and what the observability report is supposed to surface.
Dropping them at bronze would hide the data-quality problem instead of
measuring it — and would make the quarantine table a dumping ground whose
growth tells you nothing specific.

The two retained cases are different in kind: a record with no parseable
structure or no key cannot be deduplicated, merged or joined by *any*
downstream model, so landing it would corrupt silver rather than inform it.

**Verified by** `test_quarantine_boundary_is_exact`, which feeds bronze a
three-row file — valid, negative-premium, keyless — and asserts two land and one
is quarantined. It is built by hand rather than from the generator because the
generator injects defects probabilistically and a small sample can contain none.

---

## ADR-0008 — Group CSV files by header signature before reading

**Status:** accepted

**Context.** Spark's CSV reader derives its schema from a sample of the files in
a read, then applies it to all of them. When a partner adds a column in a later
file, the drifted header can be silently dropped depending on which files were
sampled.

**Decision.** Bronze reads the first line of each CSV, groups files by header
signature, reads each group separately, and unions them with
`unionByName(allowMissingColumns=True)`.

**Why.** It makes drift explicit and deterministic instead of
sample-dependent, and the grouping produces the drift signal for free — the
column-set difference against the existing table is written to
`bronze._schema_audit` before the widened write happens.

**Verified.** Day 2 of the generator adds one column per partner variant; the
audit table records `distribution_channel`, `AgentCode` and `kanal_distribusi`
respectively, and `mergeSchema` widens each table.

---

## ADR-0009 — OSS Delta vs Databricks: probe, don't assume

**Status:** accepted

**Context.** Two assumptions made while planning this repo turned out to be
wrong, and both would have ended up in the documentation as confident
statements.

**Decision.** `ingestion/maintenance.py` attempts each maintenance operation and
reports the real outcome. `make maintenance` prints the matrix.

**What the probe found** (Spark 3.5.9 / Delta 3.3.3, 8 of 9 supported locally):

| Operation | OSS Delta 3.3 |
|---|---|
| `OPTIMIZE` (bin-packing) | works |
| `OPTIMIZE ... ZORDER BY` | works |
| `CLUSTER BY` on an **unpartitioned** table | works |
| `OPTIMIZE ... FULL` (re-cluster) | works |
| `VACUUM`, `DESCRIBE HISTORY`, time travel | works |
| `CLUSTER BY` on a **partitioned** table | refused |

**The two corrections.** `OPTIMIZE FULL` is *not* Databricks-only. And the one
failure is not a missing feature: clustering and Hive-style partitioning are
mutually exclusive (`DELTA_ALTER_TABLE_CLUSTER_BY_ON_PARTITIONED_TABLE_NOT_ALLOWED`).
Bronze partitions by `_batch_date`, so clustering is unavailable there by
construction — on Databricks that table would use liquid clustering *instead of*
partitioning.

**Genuinely Databricks-only:** predictive optimization, `CLUSTER BY AUTO`,
auto-compaction defaults, default-on deletion vectors, Unity Catalog governance.

**Why this matters beyond accuracy.** "Which Delta features are OSS?" is exactly
the kind of question a Lead candidate gets asked, and the honest answer is more
nuanced than either marketing or folklore suggests.
