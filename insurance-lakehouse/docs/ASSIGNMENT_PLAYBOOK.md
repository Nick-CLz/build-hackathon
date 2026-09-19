# Assignment playbook

Day-1 checklist for pointing this skeleton at a new brief. The goal is that day 1
is spent on *their* data and *their* questions, not on setup.

---

## Hour 1 — before writing any code

Read the brief twice and write down, in the design doc skeleton below:

1. **What decision does the output support?** Every model should trace to a
   question someone acts on. If it does not, cut it.
2. **What is the grain of each thing they gave you?** One row per what? If the
   brief does not say, that is the first question to ask — and asking it is
   itself a signal of seniority.
3. **What is deliberately messy in their data?** There always is something.
   Finding it early is the difference between a pipeline that works on the
   sample and one that works.
4. **What would you refuse to build?** Naming a thing you *did not* do, and why,
   reads as judgment rather than omission.

---

## Hour 2 — register their sources

**This is the only step that should touch `config/sources.yml`.**

```yaml
  - name: their_source_name
    description: >
      What it is, who sends it, and anything surprising about it.
    format: csv            # or json
    path: their_path/dt=*/file_*.csv
    reader_options: {header: 'true', mode: PERMISSIVE, encoding: UTF-8}
    primary_key: [their_key]         # quarantine rule + dedup key
    timestamp_column: their_updated_at
    partition_by: [_batch_date]
    freshness: {warn_after_hours: 26, error_after_hours: 50}
    pii_columns:
      their_pii_column: {pii_type: national_id, sensitivity: high}
```

Then `make bronze`. No Python changes. If you find yourself editing
`ingestion/bronze.py`, stop — either the YAML can express it, or you have found
a genuine gap worth calling out in the write-up.

Only `csv` and `json` are implemented. Adding Parquet or Avro means extending
`read_source()` and `_SUPPORTED_FORMATS`.

---

## Hours 3–6 — silver

**Keep:** `dedupe_latest`, `clean_text`, `clean_code`, `convert_currency`,
`fx_join`, `mask_pii`, and the incremental + lookback pattern.

**Rename or delete:** everything in `dbt/models/silver/staging/` is
insurance-specific. Model their entities instead.

**Keep `be_to_ce_date` only if they have Thai dates.** If not, delete it and its
regression test — carrying unused domain logic looks like padding.

### What to preserve regardless of domain

| Pattern | Why it transfers |
|---|---|
| Dedup with an explicit, documented tie-break | Every source resends records |
| Incremental `merge` + lookback window | Every fact table has late arrivals |
| SCD2 with the `check` strategy where backdating is possible | |
| Quarantine narrow, business violations to tests | |
| Grain documented in every model's YAML | |

---

## Hours 7–10 — gold

Build the smallest star schema that answers their questions. Resist modelling
everything.

- One fact per business process, at the finest grain that is queried.
- Dimensions conformed across facts.
- **A mart per question in the brief**, named after the question.

If the brief asks for three metrics, ship three marts and say why you stopped.

---

## Day 3 — governance, CI, docs

1. `make governance` regenerates Unity Catalog SQL from the `meta` tags. If you
   classified PII while writing the models, this step is free.
2. Run `make ci` and fix whatever it finds.
3. `make demo` and paste the output into the write-up. Evidence beats claims.
4. **`make clean && make setup && make all` from scratch.** Always. It is the
   most common reason a good submission fails.

---

## Design doc template

> ### Problem
> What they asked for, in your words. One paragraph.
>
> ### Assumptions
> Everything the brief left ambiguous and what you chose. This section is
> doing more work than it looks: it shows you noticed.
>
> ### Architecture
> Diagram plus a paragraph per layer. Say what each layer is *not* responsible
> for — that is where the design actually lives.
>
> ### Data model
> ERD and the grain of every table. One sentence each.
>
> ### Quality
> The tests, their severity, and **why each severity**. Name one test that
> currently fails and explain why that is correct.
>
> ### Governance
> PII classification, masking, retention, cross-border. Where the single
> source of truth lives and how it cannot drift.
>
> ### Trade-offs
> Three things you chose *against*, with the reasoning. The strongest section
> in most submissions, and the most often missing.
>
> ### What I'd do with more time
> Ordered by value, not by effort. Shows you know what is missing.
>
> ### How I used AI tools
> Specifically: what was generated, what you verified, and where the model was
> wrong. See below.

---

## The AI-use section

Graders welcome AI use and are unimpressed by vagueness. Be concrete:

> Claude Code generated the scaffolding, the synthetic data generator, and
> first drafts of the SQL and documentation. I verified every version pin by
> installing and running the stack rather than accepting the model's memory:
> `delta-spark` 3.3.3 declares `pyspark>=3.5.3,<3.6.0`, and Spark 3.5 rejects
> Java 21, so the whole stack was re-run on JDK 17.
>
> It was confidently wrong twice, and both are worth naming. It stated that
> `OPTIMIZE FULL` and liquid clustering were Databricks-only; I replaced the
> assertion with a probe that attempts each operation, and both claims were
> false — the one real failure had a different cause (clustering and Hive
> partitioning are mutually exclusive). It also used `try_to_date`, which
> exists on Databricks but not in OSS Spark 3.5; a regression test caught it.
>
> The standing rule in the repo's `CLAUDE.md` is "verify by running; do not
> write 'should work'." The interesting skill is not generating code — it is
> building the harness that catches the generator being confidently wrong.

---

## Three-day plan

| | Morning | Afternoon |
|---|---|---|
| **Day 1** | Read brief, design doc skeleton, register sources in YAML | Bronze ingestion running; first look at real data shapes |
| **Day 2** | Silver: staging, dedup, incremental, SCD2 | Gold: star schema, marts, tests |
| **Day 3** | Governance, CI green, docs | **Clean-clone run**, demo output, write-up |

Leave day 3 afternoon genuinely free. It always overruns, and a submission that
runs from a clean clone beats a cleverer one that does not.

---

## Traps that cost people the offer

1. **A pipeline that only works on the sample.** Run it twice. Run it on day 2
   data. Idempotency is the most-tested property and the least-demonstrated.
2. **Written premium used as the loss-ratio denominator.** It flatters a growing
   book. Earned premium is the correct denominator and most candidates miss it.
3. **Late-arriving facts ignored.** An incremental model that only reads today's
   rows understates every past period, silently and permanently.
4. **PII masked in a dashboard rather than in the model.** Masking is a data
   layer concern; a second BI tool bypasses it instantly.
5. **No stated grain.** If you cannot say what one row means, neither can they.
6. **Docs that describe intent rather than behaviour.** Every claim in this repo
   was produced by running something. Do the same.
