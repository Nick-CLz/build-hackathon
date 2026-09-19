{% snapshot policies_snapshot %}
{{ config(
    target_schema='silver',
    unique_key='policy_id',
    strategy='check',
    check_cols=['sum_insured', 'written_premium', 'policy_status',
                'expiry_date', 'vehicle_id', 'product_code'],
    file_format='delta',
    invalidate_hard_deletes=True
) }}

/*
    SCD2 history of every policy. Grain: one row per (policy_id, version).

    WHY 'check' AND NOT 'timestamp' -- this is the design decision to defend.

    The timestamp strategy compares a source updated-at column against the
    latest captured version and records a change when the source is newer. It is
    cheaper and it is WRONG here, because endorsements can be backdated.

    A backdated endorsement has effective_date BEFORE requested_date, sometimes
    by weeks -- stg_endorsements keeps both dates precisely so this stays
    visible. The generator produces them at a ~35% rate because they are common
    in real motor books: a customer changes their sum insured and the paperwork
    catches up later.

    With a timestamp strategy, an amendment whose effective timestamp predates a
    version already captured looks like stale data and is skipped. The policy's
    history then silently omits a real change in cover. Nothing errors. The
    exposure used for earned premium is simply wrong, and stays wrong.

    The check strategy compares the business columns themselves, so it detects
    the change regardless of what the timestamps claim. The cost is reading and
    comparing those columns on every run, which is the right trade: a cheap
    snapshot that loses changes is worth less than an expensive one that does
    not.

    invalidate_hard_deletes closes the validity window for policies that vanish
    from the source, so a deleted policy stops appearing as currently in force.
*/

select
    policy_id,
    customer_id,
    vehicle_id,
    product_code,
    partner_code,
    country,
    currency,
    inception_date,
    expiry_date,
    policy_status,
    sum_insured,
    written_premium,
    written_premium_thb,
    sum_insured_thb,
    record_source
from {{ ref('silver_policies') }}

{% endsnapshot %}
