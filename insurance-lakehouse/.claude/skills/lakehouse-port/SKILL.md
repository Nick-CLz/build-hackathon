---
name: lakehouse-port
description: Stand up or adapt a medallion-architecture data pipeline (bronze/silver/gold in Delta or a warehouse, dbt models, PII governance, orchestration), and port one between platforms — local Spark, Databricks, Redshift. Use this whenever the user mentions a data engineering take-home or assessment, a lakehouse or medallion architecture, dbt with Spark or Databricks, porting a pipeline to a new warehouse, Unity Catalog governance or column masking, or adapting an existing pipeline skeleton to new source data. Also use it when they ask about verified version pins for PySpark/delta-spark/dbt, because those combinations break in specific ways that are already documented here. Reach for it even if the user does not say "lakehouse" — "ingest these partner files and model them" is the same task.
---

# Lakehouse port and adaptation

This skill carries hard-won, *verified* facts from building a complete
insurance lakehouse skeleton and porting it from local Spark to Databricks.
Its value is that the expensive discoveries are already made: version pins that
actually resolve, and the specific ways a platform port breaks silently.

Reference repo: `insurance-lakehouse/` — a working example of everything below.

## The one discipline that matters

**Verify by running. Never write "should work".**

Every claim in that repo was produced by executing something. That single rule
caught, among others: `OPTIMIZE FULL` is *not* Databricks-only; `try_to_date`
exists on Databricks but not OSS Spark 3.5; and a parity check reporting
"17/17 tables match" while a required column was missing from every one of them.

When you state a platform capability, either you ran it or you say you didn't.

## Version pins that resolve (verified, not recalled)

Read `references/verified-versions.md` before pinning anything. The short form:

```
Python 3.11 · JDK 17 · PySpark 3.5.9 · delta-spark 3.3.3
dbt-core 1.12.3 · dbt-spark[session] 1.10.3 · dbt-databricks 1.12.5
```

Two traps worth knowing without opening the file:

- **Spark 3.5 rejects Java 21.** JDK 17 or older. The failure is an obscure
  reflection error, not a version message. Prefer a discovered JDK 17 over an
  inherited `JAVA_HOME`, which on many machines points somewhere unsupported.
- **The newest dbt-spark and dbt-databricks cannot coexist.** dbt-databricks
  1.12.5 caps `dbt-core<1.12.4` and `dbt-spark<1.11.0`. Taking the latest of
  each makes one project with two targets uninstallable.

## Porting to a new platform

The order is not negotiable, and the reason is subtle:

1. **Probe first.** Attempt every operation the design depends on and report
   the real outcome. Do not read the docs and assume — free and trial tiers
   differ from what the documentation describes, in both directions.
2. **Load, then validate, then govern.** A governed table cannot be
   time-travelled (Unity Catalog refuses: a historical version predates the
   current policy). Applying masks before validating closes the window for
   comparing a table against its previous version.
3. **Compare counts *and* schemas.** This is the lesson that cost the most.
4. **Apply governance last**, and only once its principals exist.

### The parity check is the deliverable

Write it before trusting the port. A harness that only validates what you
already thought to check is not a harness.

Row counts prove the same *number* of records arrived. They say nothing about
shape. A real port reported 17/17 matching while `_record_hash` was missing
from every table — the dbt build then failed on 12 models. Once the check
compared schemas too, it immediately found three dropped schema-drift columns.

Compare, per table: row count, every business column present locally, and
every metadata column the contract promises. Report platform-specific extras
(a rescued-data column, say) rather than failing on them.

### Five ways a port breaks silently

All five were found by running, none by reading. Expect analogues on any
platform:

| Symptom | Cause |
|---|---|
| Exactly N extra rows on the tables that have quarantine | The load path skipped the quarantine step entirely |
| Quarantine *reason* differs between platforms | The target's parser is more permissive and classifies the same defect differently |
| Empty quarantine tables everywhere | Created per source regardless of need, implying a problem where there is none |
| Models fail on an unresolved column | A metadata column the loader never supplied — **and row-count parity passes throughout** |
| A drift column silently absent | Schema *inference* sampled files. Evolving the target is a separate setting from making the reader look at every file |

## Adapting the skeleton to new source data

Roughly 2,200 lines of infrastructure transfer; about 3,300 lines of domain
modelling do not.

**Keep:** the ingestion engine (config-driven, no domain knowledge), the
governance generator (driven by metadata), the Makefile, CI, the parity
scripts, and the macros `mask_pii`, `convert_currency`, `dedupe_latest`.

**Replace:** the source registry YAML, the dbt models, the synthetic data
generator, the domain primer.

Registering a source should be a YAML edit — format, path glob, primary key,
timestamp column, freshness SLA, PII columns. **If adapting requires editing
the ingestion engine, either the YAML cannot express it or you have found a
real gap worth naming in the write-up.**

## Patterns that transfer across domains

These solve problems every pipeline has, whatever the subject:

- **Dedup with an explicit tie-break.** Business timestamp, then ingestion
  time, then a content hash — the last so two identical records resolve the
  same way on every re-run rather than depending on row ordering.
- **Incremental merge with a lookback window.** Facts arrive late. A model
  reading only "today's" rows understates every past period, permanently and
  silently. Size the window from a *measured* percentile, not a guess, and
  build the mart that measures it.
- **SCD2 with the `check` strategy where backdating is possible.** A timestamp
  strategy skips an amendment whose effective date precedes a version already
  captured.
- **Quarantine narrowly.** Only records that cannot be landed at all — no
  parse, no primary key. Business-rule violations must reach the warehouse so
  tests can *measure* them; dropping them hides the signal.
- **Currency conversion at the transaction date, by validity window.** An
  exact-date FX join silently nulls every weekend and holiday, because feeds
  publish on trading days only.

## Governance

Classification belongs in one place — dbt column `meta` — with every
enforcement artefact generated from it. Hand-maintained tags drift from models
at the third schema change and nobody notices until an audit.

Platform specifics in `references/databricks.md`. The two findings most likely
to bite anywhere:

- **A row filter whose principals do not exist hides everything, silently.**
  Every clause evaluates false, the table returns zero rows to everyone
  including the owner, and no error is raised. The fail-safe direction is
  *not applied* — an unapplied filter shows up in an audit, an over-applied one
  looks like data loss. Check principals resolve before attaching.
- **Masks bind to tables, not views.** Not a coverage gap: a mask on the base
  table applies through any view reading it. Tags *can* attach to views, so
  discovery works at every layer while enforcement binds one level down.

## Platform references

- `references/verified-versions.md` — the pin matrix and what each constraint costs
- `references/databricks.md` — probe results, `COPY INTO` loading, governance
- `references/redshift.md` — **researched, not executed.** Treat every claim as
  a hypothesis to test, and say so in any write-up that uses it
