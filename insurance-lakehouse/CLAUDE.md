# CLAUDE.md — insurance lakehouse skeleton

Guidance for Claude Code sessions working in this directory. Read this before
touching anything; it will save you from re-deriving decisions that are already
settled and tested.

## What this is

A local mirror of a Databricks-on-AWS lakehouse: synthetic insurance data →
Delta bronze → dbt silver/gold → governance + observability, orchestrated by
Airflow. It exists to be **adapted quickly to a new dataset**, not admired. The
fastest path to value on a new brief is `docs/ASSIGNMENT_PLAYBOOK.md`.

## Non-negotiables

- **Run from this directory**, not the repo root. The repo also contains an
  unrelated hackathon project (see ADR-0001).
- **Java 17.** Spark 3.5 rejects Java 21. `make check-java` verifies this, and
  the Makefile deliberately *overrides* an inherited `JAVA_HOME` (ADR-0004).
- **Never hand-edit `lakehouse/`.** It is generated and gitignored.
- **Never commit `.env`,** or any real Databricks host/token.
- **Verify by running.** Every claim in the docs was produced by executing
  something. If you change behaviour, re-run the relevant `make` target and
  update the docs from the real output. Do not write "should work".

## Commands

```bash
make help                 # all targets, self-documenting
make setup                # venv + deps + Java check
make data                 # generate day 1 into lakehouse/landing
make data DAY=2 N=5000    # day 2, 5k policies
make bronze               # config-driven ingestion into Delta (idempotent)
make inspect              # bronze tables, manifest, drift audit, quarantine
make maintenance          # probe OPTIMIZE/ZORDER/CLUSTER BY/VACUUM/time travel
make test                 # pytest (use -m "not slow" to skip Spark tests)
make lint / make format   # ruff + yamllint
make docs-sync            # regenerate generated tables in docs/
```

`N=` sets policy count (default 20000). `DAY=` selects the daily drop.

## Architecture in one paragraph

`data_generator/` rebuilds a complete insurance universe from `(seed,
n_policies)` and slices it by `emit_day`; it keeps **no state between runs**, so
daily drops cannot drift apart and re-running a day is idempotent. `ingestion/`
reads `config/sources.yml` and lands every declared source into Delta with
metadata columns, a file manifest for idempotency, `mergeSchema` for drift, and
a quarantine table. `dbt/` cleans and models. `governance/` turns dbt column
`meta` into Unity Catalog SQL.

## Where things live

| Path | Purpose |
|---|---|
| `config/sources.yml` | **The** source registry. Adding a source is a YAML edit. |
| `conf/spark-defaults.conf` | Delta config + laptop tuning, for both PySpark and dbt (ADR-0003) |
| `data_generator/config.py` | `MESS_REGISTRY` — every deliberate defect |
| `ingestion/bronze.py` | The generic ingestion engine. No per-source code belongs here. |
| `ingestion/maintenance.py` | Probes Delta features instead of assuming them |
| `docs/decisions.md` | ADRs. Read before changing an architectural choice. |
| `scripts/` | Inspection and doc-sync helpers |

## Conventions

- **Python:** ruff for lint *and* format, line length 100. Run `make format`
  before committing — ruff will reflow code, so apply edits by line anchor
  rather than matching long pre-formatted strings.
- **Docstrings explain *why*.** The what is readable from the code.
- **Bronze lands raw.** No casting, cleaning or reshaping. That is silver's job.
- **Quarantine is narrow** (ADR-0007): only unparseable records and records with
  no primary key. Business-rule violations must reach silver so dbt tests catch
  them.
- **Metadata columns** are prefixed `_` so they never collide with partner
  columns: `_ingested_at`, `_source_file`, `_batch_id`, `_record_hash`,
  `_source_name`, `_batch_date`.
- **`_record_hash` covers business columns only**, so a record resent verbatim
  in a later file hashes identically and silver can collapse it.

## Adding a new source

1. Add an entry to `config/sources.yml` (name, description, format, path glob,
   `primary_key`, `timestamp_column`, `partition_by`, `freshness`,
   `pii_columns`).
2. `make bronze` — no Python changes required.
3. Add a dbt staging model in `dbt/models/silver/staging/`.
4. If it carries PII, the `pii_columns` block flows automatically into the
   Unity Catalog SQL generator.

Only `csv` and `json` are implemented. Adding a format means extending
`ingestion/bronze.py::read_source` and `_SUPPORTED_FORMATS`.

## Adapting to a different dataset

See `docs/ASSIGNMENT_PLAYBOOK.md`. In short: keep `ingestion/`, `conf/`, the
Makefile, CI and the governance generator; replace `config/sources.yml`, the
`dbt/models/` tree, and either retarget or delete `data_generator/`.

## Gotchas discovered the hard way

- `JAVA_HOME` inherited from the environment may point at an unsupported JDK;
  the Makefile searches for 17 and prefers it.
- Spark's console progress bar corrupts piped output — disabled in
  `spark-defaults.conf` so `make demo` stays readable.
- Schema drift is detected **between batches**. Ingesting day 1 and day 2 in one
  run finds no drift, because there is no prior schema to compare against.
- dbt-spark's session method builds its own SparkSession; Delta config must
  arrive via `SPARK_CONF_DIR`.
- Clustering and Hive-style partitioning are mutually exclusive in Delta.
