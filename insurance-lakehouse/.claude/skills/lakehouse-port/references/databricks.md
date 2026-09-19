# Databricks

Probed and executed against Free Edition (serverless, DBSQL 2026.36).

## What Free Edition permits — 18 of 19 probes

Supported: Unity Catalog; create catalog, schema and Volume; Delta tables;
column `SET TAGS`; mask functions and `SET MASK`; row-filter functions and
`SET ROW FILTER`; `GRANT` to a group; `OPTIMIZE`; liquid clustering;
`DESCRIBE HISTORY`.

This corrected an assumption made before probing: account-level group grants
were expected to need admin rights the free tier might withhold. They do not.

**The one failure is not a tier limit:**

```
ROW_LEVEL_SECURITY_COLUMN_MASK_FEATURE_NOT_SUPPORTED.TIME_TRAVEL
```

A table carrying a row filter or column mask **cannot** be read with
`VERSION AS OF`. A historical version predates the current policy, so serving it
would mean applying today's mask to yesterday's schema or returning rows the
policy exists to withhold. Databricks refuses rather than guessing.

Consequences: SCD2 validity windows earn their place, because they are *data*
and keep working under a mask; right-to-erasure exposure narrows on governed
tables though `REORG ... PURGE` and `VACUUM` are still required; and governance
must be applied **after** validating history.

## Loading when there is no cluster

Serverless has nothing to submit PySpark to, so the ingestion *path* changes
while the *contract* does not. Upload to a Unity Catalog Volume, then
`COPY INTO` — idempotent per file, the same guarantee a manifest table gives
locally, and the stepping stone to Auto Loader.

Four things that cost a full reload each:

1. **Preserve relative paths on upload.** Bronze derives the batch date from
   the `dt=` path component; flattening strips it from every row.
2. **Two `mergeSchema` settings, different jobs.** `COPY_OPTIONS` lets the
   *target* gain a column. `FORMAT_OPTIONS` makes the *reader* infer across all
   files rather than a sample. Without the second, a column a partner adds on
   day 2 is silently dropped — rows still land, so row-count parity passes.
3. **`COPY INTO` has no quarantine.** Whatever the local engine holds back will
   land unless a post-load step reapplies the rule. Set `rescuedDataColumn` for
   JSON, or a malformed line arrives as a row of nulls indistinguishable from a
   sparse-but-valid record.
4. **Any metadata column requiring the full column list needs a second pass.**
   A content hash over business columns cannot be an inline expression, because
   the column list is unknown until inference completes.

## Governance, applied

Executing the generated SQL statement by statement — rather than as one
transaction — is what makes the outcome legible: a grant to a missing group
should not stop the masks binding.

Result with no secret scope and no groups: 26/26 tags, 8/9 mask functions,
21/26 bindings, row-filter bindings deliberately skipped, 0/12 grants.

**A row filter whose principals do not exist hides everything, silently.**
Every clause evaluates false; the table returns zero rows to every reader
including the owner; no error is raised. In production the first symptom is a
dashboard showing zero and the first hypothesis is a broken pipeline. Refuse to
attach a filter when none of its principals resolve.

**Masks bind to tables, not views** (`EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE`).
Not a gap — a mask on the base table applies through any view reading it. Read
materialisations from the dbt manifest and skip view-backed models; a
governance artefact that always part-fails trains people to ignore its output.

**Two prerequisites cannot be created from SQL**, and each failure cascades:

- A **secret scope** for the PII salt. The mask functions read
  `secret(scope, key)` rather than embedding it — a salt committed to a
  governance artefact is not a salt. Without the scope, `CREATE FUNCTION` fails
  and every binding referencing it then fails with `ROUTINE_NOT_FOUND`: one
  missing prerequisite, six failures, none naming the cause.
- The **groups**, created via SCIM at account level. Granting to a missing
  principal fails with `PRINCIPAL_DOES_NOT_EXIST`. The grant SQL itself is fine
  — it succeeds against any principal that exists.

## Token scopes

Scoped tokens fail with a bare 403 naming the missing scope in the body. Read
it and report it; the traceback points nowhere useful.

| Need | Scope |
|---|---|
| SQL, warehouse discovery, `COPY INTO`, all of dbt | `sql` |
| Uploading files to a UC Volume | `files` |
| Catalog/schema/volume DDL, tags, masks, grants | `unity-catalog` |
| Creating groups | `scim` |
| Creating a secret scope | `secrets` |

"BI Tools" grants `sql` only — enough for dbt, not for the Volume upload.
`all-apis` is the pragmatic choice for a short-lived token.
