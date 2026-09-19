# Redshift — RESEARCHED, NOT EXECUTED

**Nothing here has been run.** Unlike the Databricks notes, every claim below
is a hypothesis derived from reading and from counting call sites in a real dbt
project. Treat each as something to verify on a live cluster, and say so in any
write-up that leans on it.

The first hour on a real cluster should be spent confirming or killing the
items marked **VERIFY FIRST**.

## Why this is a bigger jump than Databricks

Databricks *is* Spark, so models and macros port unchanged. Redshift is a
PostgreSQL-derived MPP warehouse: no Delta, no Spark, different function names
and different semantics. Expect the dbt models to need real work, the ingestion
to be a rewrite, and the governance layer to need a second emitter.

## What should port unchanged

The source registry, the grain and semantics of every table, dedup logic, the
incremental + lookback pattern, SCD2 snapshots, and the test severity design.
`dbt-redshift` sits on `dbt-core>=1.8` and supports `merge` incrementals
(Redshift gained native `MERGE` in 2023).

## SQL dialect — counted from a real project

| Function | Call sites | Redshift |
|---|---|---|
| `datediff` | 12 | **Argument order *and* unit both differ**: `DATEDIFF(day, start, end)`. Silently returns wrong numbers if ported carelessly |
| `date_format` | 11 | `TO_CHAR` |
| `regexp_extract` | 10 | **VERIFY FIRST** — see below |
| `date_sub` / `date_add` / `add_months` | 8 | `DATEADD(unit, n, date)` |
| `percentile_approx` | 4 | `APPROXIMATE PERCENTILE_DISC` |
| `explode` + `sequence` | 4 | **No equivalent.** A date dimension or numbers table to join against |
| `concat_ws`, `dayofweek` | 3 | `\|\|`, `EXTRACT(DOW ...)` |

`sha2`, `split_part`, `lpad`, `last_day`, `date_trunc`, `least`/`greatest` are
expected to exist — so a PII masking macro should survive nearly intact.

**VERIFY FIRST — capture-group extraction.** Redshift's `REGEXP_SUBSTR` may
support returning a capture group via its `parameters` argument. If it does
not, any macro built on capture groups needs rebuilding around `SPLIT_PART`
and `POSITION`. Test this before estimating anything else; it is the single
largest unknown.

## Other things to establish early

- **`VARCHAR(n)` counts bytes, not characters.** Thai and other multi-byte text
  will truncate or fail on `COPY` unless sized deliberately — a `VARCHAR(50)`
  holds roughly 16 Thai characters.
- **Ingestion is a rewrite.** Land to S3, `COPY` into staging, with Redshift
  auto-copy jobs as the nearest Auto Loader analogue.
- **Dist and sort keys** are Redshift's answer to clustering, and a real
  modelling decision rather than config noise.
- **Governance needs a second emitter**: `CREATE MASKING POLICY` /
  `ATTACH MASKING POLICY` for dynamic data masking, `CREATE RLS POLICY` for row
  filters, roles rather than groups. The metadata source stays the same, so
  this is a new output format over existing classification.
- **No local emulator.** `dbt-redshift` builds on `dbt-postgres`, so Postgres is
  a practical CI stand-in for catching dialect errors cheaply, with a real
  Serverless run before any submission.

## Architecture fork to decide before writing code

- **Spectrum over S3** — keep bronze as Parquet on S3, query via external
  tables. Closest to a lakehouse; costs per TB scanned; Delta support is via
  manifest files, so plain Parquet is more likely.
- **Native Redshift** — `COPY` everything into the cluster. Simpler, faster,
  cleaner dbt story, but bronze becomes staging tables rather than a lakehouse
  layer.

Native is the idiomatic Redshift architecture. Spectrum is the choice if one
story must span both Redshift and a lakehouse platform.
