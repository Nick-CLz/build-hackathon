# Interview prep

Every technical answer below is anchored to something in this repository. That
is the point: "how does the Delta log work" is a recitation when answered from
memory and a demonstration when you can open `lakehouse/bronze/*/_delta_log/`
from a pipeline you built and walk through the JSON.

Structure for each: **headline first**, then the mechanism, then the concrete
example, then the trap. Aim for 60–90 seconds spoken.

---

## Technical round (Head of DE & Architecture)

### 1. How the Delta transaction log works

**Headline.** Delta is Parquet files plus an ordered log of atomic commits; the
log, not the file listing, is the table.

**Mechanism.** Each commit writes `_delta_log/NNNNNNNNNNNN.json` containing
actions — `add`, `remove`, `metaData`, `commitInfo`. A reader reconstructs state
by replaying the log, so a file on disk that no commit references is invisible.
Atomicity comes from the commit being a single file creation that either lands
or does not; isolation is optimistic concurrency on the version number. Every
tenth commit is checkpointed to Parquet so replay stays bounded.

**From this repo.** Run `make bronze`, then `make bronze` again. The second run
adds no commit because the manifest skips already-ingested files.
`make maintenance` prints `DESCRIBE HISTORY`, and time travel reads version 0
while the current version has more rows:

```
[OK] Time travel (versionAsOf 0)   v0 rows=537, current rows=669
```

That gap *is* the log: two commits, both still readable.

**Trap to name.** `VACUUM` deletes files no longer referenced, which destroys
time travel beyond the retention window. `spark.databricks.delta.retentionDurationCheck.enabled`
is set to false in `conf/spark-defaults.conf` **only** so the demo can show the
effect in one run — doing that in production is how you lose the ability to
roll back.

### 2. Small-file compaction

**Headline.** Streaming and frequent micro-batches produce thousands of tiny
files; the cost is metadata and task overhead, not storage.

**Mechanism.** `OPTIMIZE` bin-packs small files into ~1GB targets and commits
the swap atomically (`add` new, `remove` old). Databricks adds auto-compaction
and optimized writes; in OSS they are opt-in.

**From this repo.** `make maintenance` runs `OPTIMIZE` and reports it supported.
Bronze is deliberately append-only per batch, which is exactly the pattern that
generates small files — sixteen sources × one file per source per day.

**Trap.** Compacting a table someone is streaming from can break the stream
unless it tolerates file rewrites. And compaction competes with ingestion for
the same cluster.

### 3. Z-order vs liquid clustering

**Headline.** Z-order is a one-shot multi-dimensional sort you must re-run;
liquid clustering is an incrementally-maintained layout you declare once.

**Mechanism.** Z-order interleaves values across chosen columns so data-skipping
statistics prune more effectively; adding data degrades it until you re-run
`OPTIMIZE ZORDER`. Clustering records clustering keys in table metadata and
maintains layout as data arrives, and the keys can be changed without rewriting
the whole table.

**From this repo — and this is the one worth telling as a story.** I assumed
liquid clustering was Databricks-only. `ingestion/maintenance.py` probes rather
than asserts, and the probe corrected me twice:

```
[OK  ] OPTIMIZE ZORDER BY (policy_no)     [OK  ] CLUSTER BY on UNPARTITIONED table
[OK  ] OPTIMIZE FULL (re-cluster)         [FAIL] CLUSTER BY on PARTITIONED table
```

`OPTIMIZE FULL` is **not** Databricks-only. And the single failure is not a
missing feature — clustering and Hive-style partitioning are mutually exclusive
(`DELTA_ALTER_TABLE_CLUSTER_BY_ON_PARTITIONED_TABLE_NOT_ALLOWED`). Bronze
partitions by `_batch_date`, so clustering is unavailable there *by
construction*. On Databricks that table would use liquid clustering **instead
of** partitioning.

What stays genuinely proprietary: predictive optimization, `CLUSTER BY AUTO`,
auto-compaction defaults, default-on deletion vectors.

### 4. Deletion vectors

**Headline.** Merge-on-read for deletes: mark rows as removed in a side file
instead of rewriting the Parquet.

**Mechanism.** A `DELETE` or `UPDATE` of a few rows would otherwise rewrite
entire files (copy-on-write). A deletion vector is a bitmap of removed row
positions; readers apply it. Writes get much cheaper, reads slightly dearer,
until compaction materialises the deletes.

**Where it bites here.** PDPA right-to-erasure. `DELETE` plus deletion vectors
does not physically remove the personal data — the bytes sit in Parquet until
compaction and `VACUUM`. An erasure request is therefore **delete → compact →
VACUUM past the retention window**, and time travel must not be able to
resurrect the record. That is written up in `docs/decisions.md`.

### 5. Spark shuffle, skew, AQE and broadcast joins

**Headline.** Shuffle is the expensive part; most tuning is about avoiding it or
making its partitions even.

**Mechanism.** Wide transformations redistribute by hash. Skew means one
partition holds far more keys, so one task runs long after the rest finish. AQE
(on by default in 3.x) coalesces small post-shuffle partitions, splits skewed
ones, and can convert a sort-merge join to broadcast once it sees real sizes.
Broadcast avoids the shuffle entirely by shipping the small side to every
executor.

**From this repo.** `conf/spark-defaults.conf` sets
`spark.sql.shuffle.partitions=4` with AQE on. The default of 200 is a cluster
assumption: on a laptop it produced 200 near-empty tasks per shuffle, and
`VACUUM` fanned out to a **10,000-partition** file listing that dominated a
92-second run. The dimension joins in gold are textbook broadcast candidates —
`stg_products` has 5 rows, `stg_partners` 6.

**Trap.** `spark.sql.shuffle.partitions` is not one-size-fits-all; the right
value scales with data and cores. And broadcasting something that is not
actually small causes driver OOM.

### 6. Partitioning mistakes

**Headline.** Over-partitioning is more common and more damaging than
under-partitioning.

**The classic errors**, with the reasoning:
- Partitioning by a high-cardinality column (`customer_id`, `policy_id`) — one
  tiny file per value, and metadata cost swamps any pruning benefit.
- Partitioning by a column nobody filters on, so you pay the cost for no gain.
- Partitioning a small table at all; under ~1GB it is pure overhead.
- Forgetting that **partitioning excludes clustering** — see topic 3, which I
  hit in this repo for real.

**From this repo.** Bronze partitions by `_batch_date` — low cardinality, and it
is genuinely the predicate used for incremental reads and backfills. That is the
test to apply: *is this column in the WHERE clause of the queries that matter?*

### 7. Unity Catalog: metastore → catalog → schema

**Headline.** Three levels: one metastore per region, catalog as the environment
or domain boundary, schema as the layer.

**Layout used here.** Catalog per environment (`insurance_dev`, `insurance_uat`,
`insurance_prod`), schema per medallion layer (`bronze`, `silver`, `gold`,
`marts`). Promotion is then a catalog swap, and the same dbt project builds
against all three — which is exactly what the two targets in `dbt/profiles.yml`
demonstrate in miniature.

**Grants, tags, masks, lineage.** Grants are inherited down the hierarchy.
Column tags drive discovery and policy. Column masks and row filters are
functions attached to a column or table, applied at query time based on group
membership. Lineage is captured automatically from query history — which is how
you answer "where did this PII column end up" without asking people.

**From this repo.** PII classification lives in dbt column `meta` and in
`config/sources.yml`, and `governance/unity_catalog.sql` is **generated** from
that metadata rather than hand-written, so the tags cannot drift from the models.

### 8. dbt incremental strategies

**Headline.** `append` is fastest and wrong for anything restatable; `merge` is
the default for facts with a stable key; `insert_overwrite` suits whole-partition
rebuilds.

**From this repo.** Claims use `merge` on `claim_id` with a **configurable
lookback window** (`claims_lookback_days`, default 30). This is the whole
late-arrival story: a claim with a 45-day reporting lag belongs to *last* month's
loss ratio, and an incremental model that only reads "today's" rows would
understate that month permanently. The window trades re-read cost for
correctness, and the right value is a business question — what is the 99th
percentile reporting lag? — which `mart_claims_lag` answers.

**Trap.** `merge` needs a genuinely unique key. If the key can duplicate within
one batch, the merge is non-deterministic — which is why dedup happens in
staging, with an explicit tie-break, before anything merges.

### 9. Snapshots vs a Delta MERGE

**Headline.** A snapshot is dbt's opinionated SCD2 implementation; a MERGE is the
primitive underneath. Use the snapshot when you want history you did not write
yourself.

**Mechanism.** `dbt snapshot` maintains `dbt_valid_from` / `dbt_valid_to` and
detects change by `timestamp` or `check` strategy. A hand-rolled MERGE gives full
control but you own the correctness.

**From this repo, and the interesting bit.** Policies use the **check** strategy,
not timestamp — deliberately. Endorsements can be **backdated**: `effective_date`
precedes `requested_date`, sometimes by weeks. A timestamp strategy keyed on the
source's updated-at would silently skip a backdated change whose timestamp is
older than a row already captured. Check compares the business columns and
catches it. `stg_endorsements` keeps both dates separate for exactly this
reason, and conflating them is the most common way earned premium goes wrong.

### 10. Airflow idempotency and backfills

**Headline.** A task must produce the same result whether it runs once or five
times, and must be parameterised by logical date rather than wall clock.

**From this repo.** Three layers of idempotency, each independently testable:
the generator holds no state and rebuilds deterministically from `(seed,
n_policies)`; bronze skips files already in the manifest — `test_ingest_is_idempotent`
asserts a second run reports `UP_TO_DATE` with identical row counts; dbt models
are `merge`-keyed. `max_active_runs=1` prevents two runs racing the same Delta
table.

**Trap.** `datetime.now()` anywhere in a DAG destroys backfill correctness. So
does an `append` incremental model, which silently doubles on re-run.

### 11. Kafka delivery semantics and CDC

**Headline.** At-least-once plus an idempotent sink is the practical
exactly-once; true end-to-end exactly-once needs transactional coordination.

**Mechanism.** At-most-once loses on failure; at-least-once duplicates on
retry; exactly-once needs idempotent producers plus transactions, or a sink
keyed so duplicates collapse. Structured Streaming's checkpoint plus Delta's
atomic commit gives exactly-once *into Delta* even from an at-least-once source.

**CDC from microservices.** Debezium reads the database log and emits
before/after images with an op (I/U/D). Order matters per key, so partition by
primary key. Deletes are the trap: a tombstone must be handled explicitly or
deleted rows live forever downstream.

**From this repo.** `cdc_policies` carries `op` and `ts`; the generator emits
inserts, updates and a delete. `stg_policies_cdc` keeps every change event —
collapsing to current state happens later — so the history stays available for
the SCD2 snapshot and for auditing what changed when.

---

## A root-cause story from this build

Have one or two told as **symptom → diagnosis → fix → prevention**. This one is
real and from this repository:

**Symptom.** Every dbt model failed with `UnresolvedRelation [bronze, ...]`
followed by a hundred lines of Spark logical plan. Bronze had just been built
successfully and `make inspect` showed sixteen populated tables.

**Diagnosis.** `conf/spark-defaults.conf` sets the Spark warehouse and Derby
metastore paths *relatively*, because that file cannot expand environment
variables. Relative paths resolve against the working directory. Running dbt from
inside `dbt/` therefore created a second, empty metastore at `dbt/lakehouse/`.
Both Spark sessions were healthy; they were looking at different catalogs.

**Fix.** dbt is now always invoked from the project root via the Makefile.

**Prevention.** An `on-run-start` hook checks the bronze schema is visible and
fails with the three likely causes and the command that fixes them, instead of
an opaque relation error. It uses `show databases` rather than
`show tables in bronze`, because the latter *raises* in precisely the case the
guard exists to explain.

The general lesson, which is the part worth saying out loud: the error message
pointed at the schema, but the fault was in path resolution two layers away.

---

## Transparent AI use

They welcome AI tools, so say this plainly and specifically rather than
minimising it:

- **What AI did.** Generated the scaffolding, the synthetic data generator, the
  staging SQL, and first drafts of the documentation.
- **What I verified myself, and how.** Every version pin was checked by
  installing the stack and running it, not taken from the model's memory —
  `delta-spark` 3.3.3's own metadata declares `pyspark>=3.5.3,<3.6.0`; Spark 3.5
  rejects Java 21, so JDK 17 was installed and everything re-run on it.
- **Where the model was wrong, and how I caught it.** It stated that
  `OPTIMIZE FULL` and liquid clustering were Databricks-only. I replaced the
  assertion with a probe (`ingestion/maintenance.py`) that attempts each
  operation and reports the real outcome — both claims were wrong, and the one
  genuine failure had a different cause than assumed. It also used
  `try_to_date`, which exists on Databricks but not in OSS Spark 3.5; the
  regression test caught it.
- **The standing rule in `CLAUDE.md`:** *"Verify by running. Do not write
  'should work'."* Every claim in these docs was produced by executing something.

That last point is the one that lands: the interesting skill is not generating
code, it is building the harness that catches the generator being confidently
wrong.

---

## Culture round (Chief Data Officer)

I can give you the structure; the specifics are yours and I am not going to
invent them. **Headline → context → what you did → result**, 60–90 seconds,
rehearsed until it runs clean in one pass.

| Story | Headline should assert | Make sure you include |
|---|---|---|
| **17-team RCSA** — leading without authority | That you got 17 teams aligned with no mandate over any of them | The specific mechanism that created buy-in; what you did when someone refused; the measurable outcome |
| **LSEG FinOps** — migration / cost win | A number, stated early | The baseline, how you measured it, what you traded away, whether the saving held |
| **ECCO costing** — messy data → trusted product | That people changed a decision because they trusted the output | What "untrusted" looked like before; the specific quality control that changed minds; who uses it now |
| **Mentoring** | A named person's trajectory, not your process | What they could not do before; what you actually did; where they are now |

Two notes from your feedback on the last process:

1. **Headline first.** Do not build to the point — a Head of Data will
   interrupt before you reach it. "We cut Databricks spend 40% in a quarter"
   *then* the context.
2. **Rehearse aloud, once through, no restarts.** These fail by rambling past
   90 seconds, not by lacking content. Time yourself.

For the risk-background angle the brief specifically rewards: the governance
work here is where it shows. PII classification lives in one place and the Unity
Catalog SQL is *generated* from it, so tags cannot drift from models — that is a
control design argument, not a data engineering one, and it is what will
separate you from candidates who bolt on `GRANT` statements at the end.
