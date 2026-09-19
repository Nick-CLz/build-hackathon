# Porting to Databricks on AWS

Component by component: what runs locally, what replaces it, and the exact
change required. Where a claim here was verified rather than assumed, it says so.

---

## Mapping

| Local | Databricks / AWS | Change required |
|---|---|---|
| Landing zone on local disk | **S3** bucket, `s3://<env>-insurance-landing/` | `LAKEHOUSE_LANDING` becomes an S3 URI; register an External Location + Storage Credential in Unity Catalog |
| `ingestion/bronze.py` file glob | **Auto Loader** (`cloudFiles`) | Replace `discover_files()` with `spark.readStream.format("cloudFiles")`; set `cloudFiles.schemaEvolutionMode=addNewColumns` and `cloudFiles.useNotifications=true` (SQS) rather than directory listing |
| `_ingestion_manifest` Delta table | Auto Loader **checkpoint** (RocksDB) | Drop the manifest. It exists only because OSS has no `cloudFiles` equivalent (ADR-0006) |
| `_schema_audit` table | `_rescued_data` + schema hints | Keep the audit table — it is application logic, and Auto Loader's rescued-data column answers a narrower question |
| `*_quarantine` tables | Unchanged | Carries over as-is; it is a pattern, not a workaround |
| Hive metastore (Derby) | **Unity Catalog** | Set `catalog:` in the dbt profile; drop `generate_schema_name` overrides if you adopt catalog-per-env |
| `dbt-spark` session method | **dbt-databricks** over a SQL warehouse | Already present: `--target databricks`. Set `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`, `DATABRICKS_CATALOG` |
| `conf/spark-defaults.conf` | Cluster policy / serverless defaults | Most settings disappear. The laptop tuning (`shuffle.partitions=4`) is actively harmful on a cluster |
| `_batch_date` partitioning | **Liquid clustering** | `CLUSTER BY (loss_date, product_code)`. Note: clustering and Hive partitioning are **mutually exclusive** — verified here by probing, not assumed |
| `make maintenance` | **Predictive optimization** | Enable at catalog level; stop scheduling OPTIMIZE/VACUUM manually |
| `mask_pii()` macro | UC **column masks** | Both, actually: the macro masks in gold, UC masks enforce at query time regardless of how the table is read |
| `governance/unity_catalog.sql` | Executed against UC | Already generated as valid Databricks SQL |
| Astro `astro dev start` | **Astronomer** or Databricks Jobs | Deploy via `astro deploy`, or convert to a Databricks Job via Asset Bundles |
| `run_results.json` parser | **Elementary** | On Databricks `edr` works, because dbt-databricks connects over a SQL warehouse (ADR-0012) |
| Makefile deploy | **Databricks Asset Bundles** | `databricks.yml` per target; `databricks bundle deploy -t prod` from CI |
| File-based CDC feed | **Debezium → MSK → Structured Streaming** | Replace `cdc_policies` source with a Kafka read; keep `stg_policies_cdc` unchanged |

---

## What does NOT change

This is the useful half of the list, because it is where the effort went:

- **`config/sources.yml`.** Paths change from local to `s3://`; nothing else.
- **Every dbt model, macro and test.** The same project builds against both
  targets. `be_to_ce_date` uses `to_date` rather than `try_to_date` specifically
  so it works on both — `try_to_date` exists on Databricks but not OSS Spark 3.5.
- **The grain and semantics of every table.**
- **The quarantine and drift-audit patterns.**
- **PII classification in dbt `meta`**, which is what generates the UC SQL.

---

## Feature availability, verified by probing

`make maintenance` attempts each operation rather than asserting support.
Against Spark 3.5.9 / Delta 3.3.3 it reports 8 of 9 supported locally:

| Operation | OSS Delta 3.3 | Databricks |
|---|---|---|
| `OPTIMIZE` (bin-packing) | yes | yes |
| `OPTIMIZE ... ZORDER BY` | yes | yes |
| `CLUSTER BY` on an **unpartitioned** table | yes | yes |
| `CLUSTER BY` on a **partitioned** table | **no** — mutually exclusive | no |
| `OPTIMIZE ... FULL` (re-cluster) | **yes** | yes |
| `VACUUM`, `DESCRIBE HISTORY`, time travel | yes | yes |
| `CLUSTER BY AUTO` | no | yes |
| Predictive optimization | no | yes |
| Deletion vectors on by default | no | yes |
| Unity Catalog tags / masks / row filters | no | yes |

Two corrections to common belief, both found by running the probe rather than
trusting documentation summaries: **`OPTIMIZE FULL` is not Databricks-only**, and
**the `CLUSTER BY` failure is not a missing feature** — it is the partitioning
conflict.

---

## Migration order

1. **Unity Catalog first.** Catalogs, schemas, groups, external locations.
   Everything else depends on the namespace existing.
2. **Bronze onto Auto Loader.** Run it alongside the manifest-based ingestion
   and compare row counts before cutting over.
3. **Point dbt at `--target databricks`.** No model changes. This is the step
   that proves the portability claim.
4. **Apply governance.** Execute `governance/unity_catalog.sql`; verify masks
   with a `restricted_health` test principal rather than assuming they bind.
5. **Move orchestration** to Databricks Jobs or Astronomer via Asset Bundles.
6. **Then optimise:** liquid clustering, predictive optimization, serverless.
   Optimising before correctness is how you end up with a fast wrong answer.

---

## Verified against a real workspace

Run `make databricks-probe`. The results below are from Databricks **Free
Edition** (serverless, DBSQL 2026.36), 18 of 19 probes succeeding.

| Capability | Free Edition | Note |
|---|---|---|
| Unity Catalog present | yes | `samples`, `system`, `workspace` |
| Create catalog / schema / Volume | yes | no admin escalation needed |
| Create Delta table, insert | yes | |
| Column `SET TAGS` | yes | what `unity_catalog.sql` emits |
| Create mask function, `SET MASK` | yes | |
| Create row-filter function, `SET ROW FILTER` | yes | |
| `GRANT` to a group | yes | |
| `OPTIMIZE` | yes | |
| **Liquid clustering (`CLUSTER BY`)** | yes | with no partitioning conflict, unlike local |
| `DESCRIBE HISTORY` | yes | |
| **Time travel on a governed table** | **no** | see below |

This corrected an assumption made before probing: account-level group grants
were expected to require admin rights Free Edition might withhold. They do not.
Everything the governance layer generates is executable on the free tier.

### The one real constraint, and why it matters

```
[FAIL] Time travel
ROW_LEVEL_SECURITY_COLUMN_MASK_FEATURE_NOT_SUPPORTED.TIME_TRAVEL
```

**A table carrying a row filter or a column mask cannot be read with
`VERSION AS OF`.** This is not a Free Edition limit; it is how Unity Catalog
governance and time travel interact, and the probe only found it because it
attached a mask and a filter *before* attempting the historical read.

The reason is sound once stated: a historical version predates the current
policy, so serving it would mean either applying today's mask to yesterday's
schema, or handing back rows the policy exists to withhold. Databricks refuses
rather than guessing.

Three consequences for this design:

1. **Governed tables are not point-in-time queryable.** `silver_policy_as_of`
   therefore earns its place: SCD2 validity windows are *data*, and they keep
   working under a mask where `VERSION AS OF` does not. A design that leaned on
   time travel for as-of reporting would fail the moment governance was applied
   — and would fail at exactly the wrong time, when someone adds the mask.
2. **Right-to-erasure gets simpler, not harder.** The PDPA concern in
   `docs/PDPA_AND_GOVERNANCE.md` is that time travel resurrects deleted personal
   data. On a masked table that path is closed for ordinary readers. It is not a
   substitute for `REORG ... PURGE` and `VACUUM`, because the files are still
   there and a privileged reader can still reach them — but it narrows exposure.
3. **Apply masks late in a migration.** Attach governance *after* validating
   history, or you lose the ability to diff a table against its previous
   version while checking the port.

**Rollout order, revised by this finding:** load bronze, validate against local
row counts using time travel, *then* apply `unity_catalog.sql`. Applying
governance first makes the validation step unavailable.

---

## The port, executed

Not a plan. Run against the Free Edition workspace, in this order:

```
make databricks-probe    # 18/19 capabilities supported
make databricks-load     # 17 sources -> UC Volume -> COPY INTO -> bronze
make databricks-parity   # counts AND schemas, both sides
make databricks-build    # the same dbt project, --target databricks
```

**Result: identical to local.**

| | Local (Spark 3.5.9 + Delta 3.3.3) | Databricks (serverless, DBSQL 2026.36) |
|---|---|---|
| `dbt build` | PASS 112, WARN 2, ERROR 0 of 114 | PASS 112, WARN 2, ERROR 0 of 114 |
| Warnings | `valid_thai_national_id`, `mart_loss_ratio` credible-cell range | the same two |
| Tables compared | — | 23 of 23 matching, 0 diverged |
| Quarantine | 4 tables, 16 rows | 4 tables, 16 rows, same reasons |

Loss ratio and claim frequency agree to four decimal places on every product:

```
product          local LR    dbx LR  local freq  dbx freq
HEALTH_ADJ         0.7251    0.7251      0.1179    0.1179
MOTOR_CMI          0.9294    0.9294      0.1048    0.1048
MOTOR_VOL_C1       0.5818    0.5818      0.0877    0.0877
MOTOR_VOL_C2       0.3968    0.3968      0.0443    0.0443
MOTOR_VOL_C3       0.6201    0.6201      0.0974    0.0974
```

**No model, macro or test was changed for Databricks.** The only
platform-specific code is the loader, because serverless has no cluster to
submit PySpark to.

### Five contract breaks the port exposed

Each was invisible until the previous one was fixed, and none was visible from
reading the code:

| # | Symptom | Cause |
|---|---|---|
| 1 | Four bronze tables carried exactly 4 rows too many | `COPY INTO` has no quarantine step; ADR-0007's rule was not applied |
| 2 | Quarantine *reason* disagreed between targets | Databricks' JSON reader emits an all-null row rather than rescuing a malformed line |
| 3 | 13 empty quarantine tables | created per source regardless of need, implying a problem where there was none |
| 4 | 12 staging models failed with `UNRESOLVED_COLUMN` | `_record_hash` was never supplied; **row-count parity passed while it was missing** |
| 5 | Three drift columns silently absent | `COPY_OPTIONS` mergeSchema evolves the *target*; `FORMAT_OPTIONS` mergeSchema is what makes the *reader* look at every file |

Number 4 is the one worth remembering. A parity check that compares only row
counts proves the same *number* of records arrived and says nothing about their
shape — it reported 17/17 while a required column was missing everywhere. The
check now compares schemas too, and found defect 5 on its first run.

Number 5 is ADR-0008 on a different engine: Spark's CSV reader samples files
and drops a drifted header; Databricks does the same thing through a different
mechanism. Both fail the same way — the feed keeps loading, the new column
never appears, and nobody notices until someone asks why it is empty.
