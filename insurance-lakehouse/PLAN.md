# PLAN — Reusable Insurance Lakehouse Skeleton

**Status:** awaiting confirmation before Phase 1.
**Goal:** a local, laptop-runnable mirror of a Databricks-on-AWS lakehouse (Delta + dbt + Airflow + governance + observability), loaded with realistic synthetic insurance data, so that on day 1 of a take-home assignment only the data and requirements need swapping.

---

## 0. Placement decision (deviation from spec)

The spec assumed an empty repo. This repo already contains the **BUILD Hackathon "VC KPI Reporting Tool"** (Reflex + FastAPI, Python 3.13, its own `README.md`, `pyproject.toml`, `backend/`, `frontend/`, `commons/`, `src/`, `rxconfig.py`).

Putting `README.md`, `pyproject.toml`, `Makefile` and `CLAUDE.md` at the repo root would collide with, and partly destroy, that project. Two unrelated Python projects (Python 3.13 + Reflex vs Python 3.11 + PySpark) also cannot share one `pyproject.toml` without dependency conflicts.

**Decision (confirmed with the user):** build the entire skeleton under **`insurance-lakehouse/`**, using the spec's structure verbatim *inside* that directory. The existing hackathon project is untouched.

Consequences:
- All commands run from `insurance-lakehouse/` (`cd insurance-lakehouse && make all`).
- The skeleton's `CLAUDE.md` lives at `insurance-lakehouse/CLAUDE.md`. Claude Code reads nested `CLAUDE.md` files when working in that subtree, so this works as intended. A short pointer stanza will be appended to the root `README.md` only (no root `CLAUDE.md`, to avoid affecting the hackathon project).
- CI (`.github/workflows/ci.yml`) **must** live at the repo root to be picked up by GitHub Actions, so it will be `.github/workflows/insurance-lakehouse-ci.yml` with `defaults.run.working-directory: insurance-lakehouse` and a `paths:` filter, leaving any existing workflows alone.
- `.pre-commit-config.yaml` likewise must be at the repo root to run; hooks will be scoped with `files: ^insurance-lakehouse/`.

This deviation is recorded as ADR-0001 in `docs/decisions.md`.

---

## 1. Version matrix — verified, not guessed

Every row below was verified **in this environment** by installing it and running it, or by reading the package's own published metadata. Evidence column says which.

| Component | Pin | Evidence |
|---|---|---|
| Python | **3.11** | PySpark 3.5 is only fully tested through 3.11; `elementary-data` requires `<3.14`. 3.11.15 present locally. |
| JDK | **17** (Temurin/OpenJDK) | Spark 3.5 supports Java 8/11/17 only — Java 21 is Spark 4.0+. Sandbox had only JDK 21; I installed OpenJDK 17.0.20 and ran the whole smoke test on it. |
| PySpark | **3.5.9** | Latest 3.5.x on PyPI. Smoke-tested. |
| delta-spark (py) | **3.3.3** | Its metadata declares `pyspark>=3.5.3,<3.6.0` → exact fit for 3.5.9. Smoke-tested. |
| Delta JAR | `io.delta:delta-spark_2.12:3.3.3` | Pulled from Maven Central (HTTP 200, 7.1 MB) and loaded successfully. |
| dbt-core | **1.12.5** | Installed alongside dbt-spark with no resolver conflict. |
| dbt-spark[session] | **1.11.0** | Declares `dbt-core>=1.8,<2.0`, `dbt-adapters>=1.24.1,<2.0`, `pyspark>=3.0,<5.0`. Resolved cleanly with the above. |
| dbt-databricks | **1.12.5** | Databricks profile only; never installed in the local default path. |
| astronomer-cosmos | **1.15.1** | Ships a `dbt-spark` extra. |
| Faker | **40.x** | Pinned `>=40,<41`. |
| ruff / sqlfluff / yamllint / pytest / pre-commit | **0.16.x / 4.3.x / 1.38.x / 9.1.x / 4.6.x** | Latest stable at time of writing. |

### What I actually ran to verify

`Spark 3.5.9 + Delta 3.3.3 on JDK 17`, all green:

```
SPARK 3.5.9 | pyspark 3.5.9
MERGE_OK rows= 100          <- needed for dbt incremental merge
OPTIMIZE_ZORDER_OK          <- OPTIMIZE ... ZORDER BY works in OSS Delta
VACUUM_OK
TIMETRAVEL_OK v0rows= 100
LIQUID_CLUSTER_OK           <- ALTER TABLE ... CLUSTER BY accepted by OSS Delta 3.3
DONE
```

`dbt-spark session method against Delta`, all green:

```
dbt debug   -> All checks passed!
dbt run     -> PASS=2  (table model + incremental model created)
dbt run -s inc (2nd time, exercising the real merge path) -> PASS=1
```

Network prerequisites confirmed reachable: Maven Central (Delta JARs), `hub.getdbt.com` (`dbt_utils` 1.4.1, `elementary` 0.26.0), `install.astronomer.io`.

### Two findings that change the design

1. **`ALTER TABLE ... CLUSTER BY` succeeds in OSS Delta 3.3** -- but only on an *unpartitioned* table. The common claim that "liquid clustering is Databricks-only" is too coarse; OSS Delta 3.x does support clustered tables. The rest of this note was wrong and is superseded by the correction below.
2. **Spark defaults matter enormously for laptop speed.** With stock settings, `VACUUM` alone fanned out to a 10,000-partition job and the smoke test took 92s. The skeleton will ship `spark.sql.shuffle.partitions=4`, `spark.databricks.delta.snapshotPartitions=4` and matching parallelism via a repo-local `SPARK_CONF_DIR`, which is also how the Delta extensions get injected into the SparkSession that dbt's session method creates (dbt-spark calls a bare `SparkSession.builder.getOrCreate()`, so config must arrive out-of-band).

### Correction to finding 1, after actually probing it (Phase 2)

Finding 1 was right that OSS Delta supports clustering and **wrong** about what stays proprietary. `ingestion/maintenance.py` attempts each operation rather than asserting it; against the pinned versions it reports 8 of 9 supported locally:

```
[OK  ] OPTIMIZE (bin-packing)             [OK  ] CLUSTER BY on UNPARTITIONED table
[OK  ] OPTIMIZE ZORDER BY (policy_no)     [OK  ] OPTIMIZE on clustered table
[OK  ] VACUUM RETAIN 0 HOURS              [OK  ] OPTIMIZE FULL (re-cluster)
[OK  ] Time travel (versionAsOf 0)        [FAIL] CLUSTER BY on PARTITIONED table
```

Two corrections:

* **`OPTIMIZE FULL` is not Databricks-only.** It runs in OSS Delta 3.3; it just requires a table with non-empty clustering columns.
* **The single failure is not a missing feature.** Clustering and Hive-style partitioning are mutually exclusive (`DELTA_ALTER_TABLE_CLUSTER_BY_ON_PARTITIONED_TABLE_NOT_ALLOWED`). Bronze partitions by `_batch_date`, so clustering is unavailable there by construction. On Databricks that table would use liquid clustering *instead of* partitioning — which is the real porting note.

Genuinely proprietary: predictive optimization, `CLUSTER BY AUTO`, auto-compaction defaults, default-on deletion vectors, Unity Catalog governance.


### Open risks, with pre-agreed fallbacks

| Risk | Fallback if it fails |
|---|---|
| **Elementary + dbt-spark session.** `elementary-data` 0.26.0 does publish a `spark` extra, but the `edr` CLI's Spark support is aimed at Thrift/Databricks connections, not `method: session`. | Spec already sanctions this: fall back to a lightweight `run_results.json` / `manifest.json` parser writing test results, row counts and freshness into a Delta `observability` schema, with `make report` rendering static HTML. Reason recorded in `docs/decisions.md`. Either way `make report` produces something viewable. |
| **Cosmos inside Astro Runtime needs Java + PySpark**, making the image heavy and possibly impractical. | Keep Cosmos for dbt task-per-model rendering but run Spark work via `BashOperator` against the repo mount; if the Astro image can't host Java at all, drop to a documented `BashOperator`-only DAG and record the trade-off. |
| **CI runtime.** Spark + dbt + Delta JAR download in GitHub Actions. | Cache pip **and** `~/.ivy2`; generate ~2k policies in CI instead of 20k; target a few minutes. |

---

## 2. Repo structure (inside `insurance-lakehouse/`)

As specced, with these documented adjustments:

```
insurance-lakehouse/
├── CLAUDE.md  README.md  PLAN.md  Makefile  pyproject.toml
├── conf/spark-defaults.conf     # ADDED: SPARK_CONF_DIR target; injects Delta into dbt's session
├── docker/                      # Dockerfile (JDK17+PySpark+Delta+dbt), docker-compose.yml
├── config/sources.yml           # source registry: format, path, PKs, PII, freshness SLA
├── data_generator/              # seeded synthetic data, day-N incremental
├── ingestion/                   # config-driven PySpark bronze + manifest + quarantine + maintenance
├── dbt/                         # silver + gold, profiles for local spark AND databricks
├── airflow/                     # Astro CLI project + Cosmos DAG
├── governance/                  # PII metadata -> unity_catalog.sql generator
├── tests/                       # pytest: generator + ingestion
├── docs/                        # primer, data_model, decisions, databricks_port, playbook
└── lakehouse/                   # ADDED: gitignored Delta warehouse + landing zone (runtime output)
```

`.github/workflows/` and `.pre-commit-config.yaml` sit at the **repo root** (they cannot function elsewhere), scoped to this subtree.

---

## 3. Phases, each with an acceptance command

A phase is done only when its command succeeds from a clean state, observed, not assumed.

| # | Phase | Acceptance |
|---|---|---|
| 1 | Domain primer + synthetic generator (10 entities, TH/ID, CSV partner drops with divergent schemas, JSONL events, CDC feed, 10 toggleable mess types, checksum-valid Thai national IDs, day-N incrementals) | `make data && make data DAY=2` produces two daily drops; `pytest tests/test_generator.py` green (determinism, checksum rate, every mess type present) |
| 2 | Docker image + config-driven bronze ingestion (metadata cols, manifest idempotency, `mergeSchema` + drift audit, `_quarantine`, Delta maintenance script) | `make bronze` twice → row counts identical; `pytest tests/test_ingestion.py` green |
| 3 | dbt silver + gold (dedup, BE→CE, partner harmonisation, FX; incremental `merge` with claims lookback; SCD2 snapshot; star schema; 3 marts; 3 macros; custom + singular tests; freshness from `sources.yml`; observability) | `make dbt-build` green with deliberate warn-level failures visible; `make report` opens |
| 4 | Governance: dbt `meta` → `governance/unity_catalog.sql` (tags, masks, row filter, grants) + PDPA notes | `make governance` regenerates valid Databricks SQL |
| 5 | Astro project + Cosmos DAG (idempotent, logical-date parameterised, retries, `max_active_runs=1`, failure callbacks) | `make airflow-up`, one DAG run completes |
| 6 | Makefile (`help`…`all`, `demo`), ruff/sqlfluff/yamllint, pre-commit, CI | `make demo` prints the five proofs; each CI step run manually and shown |
| 7 | Docs: README (mermaid architecture, Databricks/AWS production section), `databricks_port.md`, `ASSIGNMENT_PLAYBOOK.md`, `data_model.md` ERD | all exist and match the code |

`act` is **not** available here, so CI will be validated by executing each workflow step manually and showing the output, as the spec allows.

---

## 4. Key design decisions to be recorded in `docs/decisions.md`

1. **ADR-0001** Subdirectory placement (above).
2. **ADR-0002** Python 3.11 / JDK 17 / Spark 3.5.9 / Delta 3.3.3 — why not Spark 4 (dbt-spark and Cosmos ecosystems still settling; Databricks LTS runtimes track 3.5).
3. **ADR-0003** `SPARK_CONF_DIR` as the injection point for Delta config, because dbt-spark's session method builds its own SparkSession.
4. **ADR-0004** Manifest-table idempotency as the local stand-in for Auto Loader's `cloudFiles` checkpoint.
5. **ADR-0005** SCD2 via `dbt snapshot` — **check strategy** on the business columns that endorsements mutate, not timestamp, because backdated endorsements arrive with an `effective_date` earlier than rows already captured; a timestamp strategy would silently skip them.
6. **ADR-0006** Claims incremental lookback window (default 30 days, env-configurable) for late-arriving claims.
7. **ADR-0007** Elementary vs fallback observability (decided empirically in Phase 3).
8. **ADR-0008** Test severity policy: `error` for structural integrity (PKs, referential integrity, negative premium), `warn` for data-quality signals the generator deliberately injects, so `make all` stays green while still surfacing bad data.
9. **ADR-0009** PDPA: minimisation, retention, cross-border TH/ID transfer, right-to-erasure on Delta (`DELETE` + `VACUUM`, and why time travel makes this non-trivial), health data as sensitive personal data.
10. **ADR-0010** OSS Delta vs Databricks feature boundary, corrected per the finding above.

---

## 5. What I need from you

Confirm, or tell me what to change — particularly the version pins in §1 and the placement in §0. On "go" I'll build Phase 1 through Phase 7, verifying each phase by running it, and finish with the summary the spec asks for (what was built, deviations, limitations, and the 5 things to be ready to explain in interview).
