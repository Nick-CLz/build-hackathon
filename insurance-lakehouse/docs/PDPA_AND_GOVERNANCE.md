# PDPA and governance notes

Thailand's Personal Data Protection Act (PDPA, B.E. 2562) is the governing
regime here, with Indonesia's PDP Law applying to the Indonesian book. The two
are close enough in structure to design once and vary by jurisdiction, and
different enough that "we comply with PDPA" is not a complete answer for a
multi-country insurer.

This document covers what the code does and why. The single source of truth for
classification is dbt column `meta` plus `config/sources.yml`, and
`governance/unity_catalog.sql` is **generated** from it — so tags cannot drift
from models.

---

## 1. Data minimisation

The principle: collect and retain the minimum needed for the stated purpose.

**What the code does.** PII stops at silver. Gold — the layer analysts and BI
read — carries masked values only, and health data is fully redacted outside
restricted models. An analyst computing loss ratio by age band never needs a
date of birth, so they get `age_band` and a `birth_year` truncated to 1 January.

**The deliberate exception.** `national_id` is salted-hashed rather than
dropped, because a deterministic hash still **joins**. Entity resolution
survives masking, which is what makes minimisation acceptable to the business
rather than a fight. An unsalted hash of a 13-digit national ID would be
trivially reversible — the keyspace is small enough to enumerate — so the salt
is what makes this a control rather than a gesture. It comes from `PII_SALT`
and must be a managed secret in production.

---

## 2. Retention

**What PDPA requires:** personal data kept no longer than necessary for the
purpose, with the retention period defined and justified.

**Insurance makes this genuinely hard.** A motor claim can be reopened years
later; a liability claim longer still. So "delete after 12 months" is not
available — the retention period is set by the claims tail and by the insurance
regulator's record-keeping rules, not by data-protection preference. The correct
posture is a **stated, defensible** period per data class:

| Data class | Retention driver |
|---|---|
| Policy and premium records | Regulatory record-keeping for the statutory period after expiry |
| Claim records | Statutory period after final settlement, not after the loss |
| Quotes that never converted | Short. This is the class most often over-retained — there is no contract, so the justification is weak |
| Health declarations | Shortest defensible; sensitive data raises the bar |
| Bronze raw files | Short. Bronze is a replay buffer, not an archive |

**Implementation note.** Retention on Delta is not a `DELETE`. See §4.

---

## 3. Cross-border transfer — the multi-country problem

Thai and Indonesian personal data in one warehouse is a transfer question, not
just a storage question.

**PDPA** restricts transfer to countries without adequate protection, with
exceptions for consent, contractual necessity and binding corporate rules.
**Indonesia's PDP Law** has a comparable restriction.

**Design consequences:**

1. **Region matters.** A single `ap-southeast-1` workspace serving both books is
   a transfer of Indonesian data to Singapore. Defensible with the right legal
   basis, but it must be a decision rather than an accident of where someone
   clicked.
2. **Row filters by country are a control, not a convenience.** The row filter
   in `governance/unity_catalog.sql` restricts each analyst group to its own
   jurisdiction by default. Cross-border access becomes an explicit grant with a
   named legal basis.
3. **Aggregates travel more easily than rows.** `mart_loss_ratio` at
   country/product/month grain carries no personal data. Where a global view is
   needed, serving the mart rather than the fact table is usually the lower-risk
   design — and it is also the faster query.
4. **Vendors are transfers too.** A BI tool or LLM API that receives unmasked
   rows is a cross-border transfer with an extra processor. Masking at the gold
   boundary means that exposure is bounded by construction.

---

## 4. Right to erasure on Delta — why `DELETE` is not enough

A data subject requests erasure. `DELETE FROM silver_customers WHERE ...`
returns success. **The personal data is still there**, for three reasons:

1. **Copy-on-write leaves old files.** Delta rewrites the affected Parquet files
   and marks the originals removed in the log. The originals remain on disk
   until vacuumed.
2. **Deletion vectors make this worse, on purpose.** With deletion vectors
   enabled — default-on for Databricks managed tables — the delete writes a
   *bitmap* and does not rewrite the Parquet at all. The row is invisible to
   readers and physically intact.
3. **Time travel is designed to undo it.** `VERSION AS OF` before the delete
   returns the record. A feature and a compliance hole simultaneously.

**The correct erasure procedure:**

```sql
-- 1. Delete from every table holding the subject's data, silver AND bronze.
DELETE FROM silver.stg_customers WHERE customer_id = :subject;
-- ... and every downstream table not rebuilt from source.

-- 2. Materialise the deletes (rewrites files, applies deletion vectors).
REORG TABLE silver.stg_customers APPLY (PURGE);

-- 3. Expire history past the retention window, then vacuum.
--    Until this runs, time travel can resurrect the record.
VACUUM silver.stg_customers RETAIN 168 HOURS;
```

**Four things that make this genuinely hard, and are worth saying aloud:**

- **Bronze holds the raw file too.** Erasure that stops at silver is incomplete.
  This is a strong argument for short bronze retention: the less raw history you
  keep, the smaller the erasure surface.
- **The vacuum retention window is a floor on compliance.** With a 7-day window,
  erasure is not complete for 7 days. That must be stated in the privacy notice,
  not discovered during an audit.
- **Gold is safe *if* it is rebuilt from source.** Hashed IDs are not personal
  data if the salt is rotated or destroyed — which makes salt rotation itself an
  erasure mechanism worth considering for bulk cases.
- **Backups and Deep Clones are separate systems** with their own retention, and
  they are the most commonly forgotten copy.

---

## 5. Health data is sensitive personal data

PDPA §26 treats health data as a special category requiring **explicit consent**
and stronger safeguards. In this model that is `pre_existing_conditions` and
`bmi_band` on health policies.

**How the code treats it differently:**

- `mask_pii(col, 'health')` returns `NULL` — full redaction, not partial. It
  deliberately does not preserve nullability, because *"this customer declared
  a condition"* is itself health information.
- A dedicated `restricted_health` group is the only principal granted the
  unmasked column, enforced by a UC column mask rather than by convention.
- Health columns never reach a mart. Aggregate health analytics use the policy
  and claim facts, not the declarations.

**Why the separation is worth the friction.** The failure mode is not a breach —
it is an ordinary analyst building an ordinary dashboard that happens to expose
declared conditions by name. Making that require an explicit grant means it
cannot happen by accident, which is the whole point of a preventive control.

---

## 6. Where a risk background shows

The design argument worth making in an interview is not about masking
functions. It is this: **classification lives in one place and the enforcement
artefacts are generated from it.** PII is declared once in dbt `meta` and
`config/sources.yml`; the `mask_pii` macro and `governance/unity_catalog.sql`
both derive from that declaration.

The alternative — tags applied by hand in the catalog, masks written separately,
a spreadsheet tracking which columns are sensitive — fails the same way every
time. Not immediately, but at the third schema change, when the spreadsheet and
the warehouse quietly diverge and nobody notices until an audit.

That is a control-design argument rather than a data-engineering one, and it is
the part most data teams get wrong.

---

## 7. Applying this to a real Unity Catalog, and what went wrong

`make databricks-governance` executes the generated SQL statement by statement
against a live workspace. Running it exposed four things that reading the SQL
would not have.

### A row filter whose groups do not exist hides everything, silently

The worst finding, and the one to remember.

`filter_by_country` grants visibility to `data_engineers`, `analysts_global`,
`analysts_th` and `analysts_id`. In a workspace where none of those groups
exist, every clause evaluates false, so the filter excludes every row — for
every reader, including the table owner.

```
gold.fct_claims             0 rows
gold.fct_written_premium    0 rows
gold.dim_customer           0 rows
```

**No error is raised.** The tables look empty. If this happened in production
the first symptom would be a dashboard showing zero, and the first hypothesis
would be a broken pipeline — not a governance change made hours earlier by
someone else.

The applier now refuses to attach a row filter when none of its principals
resolve, and says why. Attaching one before its groups exist is never correct:
the fail-safe direction for a filter is *not applied*, because an unapplied
filter is visible in an audit while an over-applied one looks like data loss.

### Unity Catalog will not mask a view

```
EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE
```

All thirteen silver staging models are views, and a column mask binds only to
a table. This is **not** a coverage gap: a mask on the base table applies to
every query that reads it, including through a view. Masking bronze therefore
protects the staging views that select from it.

Tags behave differently and *can* be applied to a view — so discovery works at
every layer even where enforcement binds one level down. The generator now
reads materialisations from the dbt manifest and skips view-backed models when
emitting masks, because a governance artefact that always part-fails trains
people to ignore its output.

### The salt must exist before the mask function does

The mask functions read the salt with `secret('insurance', 'pii_salt')` rather
than embedding it — a salt committed to a governance artefact is not a salt.
Without the scope, `CREATE FUNCTION` fails with `INVALID_SECRET_LOOKUP`, and
every binding that references it then fails with `ROUTINE_NOT_FOUND`: one
missing prerequisite, six failures, none of which name the real cause.

Both prerequisites are now stated at the top of the generated file, because
neither can be created from SQL:

```
databricks secrets create-scope insurance
databricks secrets put-secret  insurance pii_salt
```

### What did apply, and what it proves

On Databricks Free Edition, with no secret scope and no groups:

| | Result |
|---|---|
| Column tags | 26/26 applied |
| Mask functions | 8/9 (national_id needs the secret scope) |
| Column masks bound | 21/26 (the 5 gaps are the same cause) |
| Row filter function | created |
| Row filter bindings | **deliberately skipped** — principals absent |
| Grants | 0/12 — groups must pre-exist |

Masking is demonstrably live on the columns that bound:

```
phone_masked  +66****79            email_masked  s***@example.invalid
```

The masks are enforced by the engine, not by the model: that output is what a
reader gets from `SELECT *`, through any client, regardless of how the table is
queried. That is the difference between masking in a dbt model — which protects
the model — and a Unity Catalog mask, which protects the column.
