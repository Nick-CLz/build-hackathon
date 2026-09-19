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
