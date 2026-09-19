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
