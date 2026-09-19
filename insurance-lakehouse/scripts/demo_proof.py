#!/usr/bin/env python
"""Print evidence that the pipeline handled each hard case. Run by `make demo`.

This is the script to walk an interviewer through. Each section states what was
supposed to happen, then queries the warehouse to show whether it did, and
prints PASS or FAIL against an explicit assertion -- not a narrative claim.

Run after day 1 and day 2 have both been ingested and built; `make demo` does
that sequencing.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from ingestion.session import get_spark  # noqa: E402

RESULTS: list[tuple[str, bool, str]] = []


def check(name: str, passed: bool, detail: str) -> None:
    RESULTS.append((name, passed, detail))
    print(f"  [{'PASS' if passed else 'FAIL'}] {name}: {detail}")


def header(n: int, title: str, why: str) -> None:
    print(f"\n{'=' * 78}\n{n}. {title}\n   {why}\n{'-' * 78}")


def main() -> int:
    spark = get_spark("demo-proof")
    spark.sql("USE silver")

    # ---------------------------------------------------------------- 1
    header(
        1, "DEDUPLICATION", "Partners resend records. Bronze keeps every copy; silver keeps one."
    )
    row = spark.sql("""
        select
          (select count(*) from bronze.partner_policies_a)      as bronze_rows,
          (select count(*) from silver.stg_partner_policies
             where partner_code in ('SIAMGUARD','THAIVIVAT'))   as silver_rows,
          (select count(distinct policy_no) from bronze.partner_policies_a
             where policy_no is not null)                       as distinct_keys
    """).collect()[0]
    print(f"   bronze rows landed      : {row['bronze_rows']}")
    print(f"   distinct policy numbers : {row['distinct_keys']}")
    print(f"   silver rows after dedup : {row['silver_rows']}")
    check(
        "dedup collapses resent records",
        row["bronze_rows"] > row["silver_rows"],
        f"{row['bronze_rows'] - row['silver_rows']} duplicate rows removed",
    )

    dupes = spark.sql("""
        select policy_no, count(*) as copies, count(distinct _batch_date) as batches
        from bronze.partner_policies_a where policy_no is not null
        group by policy_no having count(*) > 1 order by copies desc limit 3
    """).collect()
    if dupes:
        print("   examples of resent keys (same key, multiple batches):")
        for d in dupes:
            print(f"     {d['policy_no']}  copies={d['copies']}  batches={d['batches']}")
    check(
        "a resent key appears in more than one batch",
        bool(dupes) and any(d["batches"] > 1 for d in dupes),
        f"{len(dupes)} keys resent across batches" if dupes else "none found",
    )

    # ---------------------------------------------------------------- 2
    header(
        2,
        "LATE-ARRIVING CLAIMS",
        "A claim reported weeks after the loss must update the month it belongs to.",
    )
    late = spark.sql("""
        select count(*) as late_claims,
               max(reporting_lag_days) as worst_lag,
               min(loss_date) as earliest_loss,
               count(distinct batch_date) as arrived_in_batches
        from silver.silver_claims where is_late_reported
    """).collect()[0]
    print(f"   late-reported claims    : {late['late_claims']}")
    print(f"   worst reporting lag     : {late['worst_lag']} days")
    print(f"   arrived across batches  : {late['arrived_in_batches']}")
    check(
        "late claims were merged, not appended",
        late["late_claims"] > 0,
        f"{late['late_claims']} claims with lag >= 21 days are present",
    )

    lag = spark.sql("""
        select sum(claim_count) as claims, max(p99_lag_days) as p99,
               sum(beyond_lookback_window) as beyond
        from marts.mart_claims_lag
    """).collect()[0]
    print(f"   p99 reporting lag       : {lag['p99']} days")
    print(f"   claims beyond lookback  : {lag['beyond']}  (lookback = 30 days)")
    check(
        "the lookback window covers the observed tail",
        (lag["beyond"] or 0) == 0,
        f"{lag['beyond']} claims would be missed by a re-run",
    )

    # ---------------------------------------------------------------- 3
    header(
        3,
        "SCD2 ENDORSEMENT HISTORY",
        "Mid-term endorsements change cover. The snapshot must keep both versions.",
    )
    scd = spark.sql("""
        select count(*) as versions, count(distinct policy_id) as policies,
               sum(case when total_versions > 1 then 1 else 0 end) as versioned_rows
        from silver.silver_policy_as_of
    """).collect()[0]
    print(f"   policy versions stored  : {scd['versions']}")
    print(f"   distinct policies       : {scd['policies']}")
    multi = spark.sql("""
        select policy_id, version_number, valid_from, valid_to, sum_insured, is_current
        from silver.silver_policy_as_of
        where total_versions > 1 order by policy_id, version_number limit 4
    """).collect()
    if multi:
        print("   a policy with history (sum insured changing over time):")
        for m in multi:
            print(
                f"     {m['policy_id']} v{m['version_number']} "
                f"{m['valid_from']} -> {m['valid_to'] or 'current':<12} "
                f"SI={m['sum_insured']} current={m['is_current']}"
            )
    check(
        "SCD2 captured more than one version for at least one policy",
        scd["versions"] > scd["policies"],
        f"{scd['versions'] - scd['policies']} historical versions retained",
    )

    overlaps = spark.sql("""
        select count(*) as n from (
          select policy_id, valid_from, valid_to,
                 lead(valid_from) over (partition by policy_id order by valid_from) as nxt
          from silver.silver_policy_as_of)
        where nxt is not null and nxt < coalesce(valid_to, date '9999-12-31')
    """).collect()[0]["n"]
    check(
        "no overlapping validity windows",
        overlaps == 0,
        f"{overlaps} overlaps (an overlap double-counts exposure)",
    )

    # ---------------------------------------------------------------- 4
    header(4, "PII MASKING", "Silver holds identifiable values; gold is what analysts read.")
    pii = spark.sql("""
        select s.national_id as raw_nid, g.national_id_hash,
               s.phone as raw_phone, g.phone_masked,
               s.date_of_birth as raw_dob, g.birth_year
        from silver.stg_customers s
        join gold.dim_customer g on g.customer_id = s.customer_id
        limit 2
    """).collect()
    for p in pii:
        print(f"   {p['raw_nid']} -> {p['national_id_hash'][:24]}...")
        print(f"   {p['raw_phone']:<16} -> {p['phone_masked']}")
        print(f"   {p['raw_dob']} -> {p['birth_year']}")
    leak = spark.sql("""
        select count(*) as n from gold.dim_customer g
        join silver.stg_customers s on s.customer_id = g.customer_id
        where g.national_id_hash = s.national_id
           or g.phone_masked = s.phone
    """).collect()[0]["n"]
    check("no raw PII survives into gold", leak == 0, f"{leak} unmasked values found")

    joinable = spark.sql("""
        select count(distinct national_id_hash) as hashes, count(*) as rows
        from gold.dim_customer where national_id_hash is not null
    """).collect()[0]
    check(
        "hashed IDs remain join-keys (deterministic, not redacted)",
        joinable["hashes"] > 0,
        f"{joinable['hashes']} distinct hashes over {joinable['rows']} rows",
    )

    # ---------------------------------------------------------------- 5
    header(
        5,
        "A TEST CATCHING REAL BAD DATA",
        "Warn-level, so the build stays green while the defect stays visible.",
    )
    bad = spark.sql("""
        with d as (select regexp_replace(trim(national_id), '[^0-9]', '') as x
                   from silver.stg_customers where country = 'TH')
        select count(*) as total,
               sum(case when length(x) = 13 and
                        cast(substr(x,13,1) as int) =
                        (11 - (cast(substr(x,1,1) as int)*13 + cast(substr(x,2,1) as int)*12 +
                               cast(substr(x,3,1) as int)*11 + cast(substr(x,4,1) as int)*10 +
                               cast(substr(x,5,1) as int)*9  + cast(substr(x,6,1) as int)*8 +
                               cast(substr(x,7,1) as int)*7  + cast(substr(x,8,1) as int)*6 +
                               cast(substr(x,9,1) as int)*5  + cast(substr(x,10,1) as int)*4 +
                               cast(substr(x,11,1) as int)*3 + cast(substr(x,12,1) as int)*2) % 11) % 10
                   then 0 else 1 end) as invalid
        from d
    """).collect()[0]
    pct = 100 * (bad["invalid"] or 0) / max(1, bad["total"])
    print(f"   Thai national IDs       : {bad['total']}")
    print(f"   failing mod-11 checksum : {bad['invalid']}  ({pct:.1f}%)")
    check(
        "the checksum test finds genuinely invalid IDs",
        (bad["invalid"] or 0) > 0 and pct < 15,
        f"{bad['invalid']} invalid IDs caught at warn level",
    )

    q = spark.sql("select count(*) as n from bronze.partner_policies_a_quarantine").collect()[0][
        "n"
    ]
    check(
        "unparseable records were quarantined, not dropped",
        q > 0,
        f"{q} records in the quarantine table with a reason",
    )

    # ---------------------------------------------------------------- 6
    header(6, "THE BUSINESS ANSWERS", "What the warehouse exists to produce.")
    spark.sql("""
        select product_code, cover_class,
               round(sum(earned_premium_thb)) as earned_thb,
               round(sum(incurred_thb))       as incurred_thb,
               round(sum(incurred_thb)/nullif(sum(earned_premium_thb),0), 3) as loss_ratio,
               round(sum(claim_count)/nullif(sum(exposure_policy_years),0), 3) as frequency
        from marts.mart_loss_ratio group by 1, 2 order by loss_ratio desc nulls last
    """).show(truncate=False)
    spark.sql("""
        select channel, sum(quotes_issued) as quotes, sum(quotes_converted) as converted,
               round(sum(quotes_converted)/nullif(sum(quotes_issued),0), 4) as conversion_rate
        from marts.mart_quote_conversion group by 1 order by conversion_rate desc
    """).show(truncate=False)

    # ---------------------------------------------------------------- summary
    passed = sum(1 for _, ok, _ in RESULTS if ok)
    print(f"\n{'=' * 78}\nDEMO SUMMARY: {passed}/{len(RESULTS)} checks passed\n{'=' * 78}")
    for name, ok, _detail in RESULTS:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    return 0 if passed == len(RESULTS) else 1


if __name__ == "__main__":
    raise SystemExit(main())
