{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='claim_id',
    file_format='delta'
) }}

/*
    Claims, merged on claim_id. Grain: one row per claim_id.

    THE LATE-ARRIVAL PROBLEM, and why this model is shaped the way it is.

    A claim is reported after the loss occurs, sometimes by weeks. The generator
    produces reporting lags up to 60 days on purpose. That means a claim that
    belongs to January's loss ratio can arrive in March.

    A naive incremental model filters on "rows whose batch_date is newer than
    what I already have". That is correct for the ROW but wrong for the PERIOD:
    January's loss ratio was computed before the claim arrived and is never
    revisited, so January stays permanently understated. The number does not
    look broken -- it is just quietly too low forever.

    The fix is a lookback window: on each run, re-read the last N days of
    batches rather than only the newest. Claims that arrived late are re-merged,
    and because the merge is keyed on claim_id it is idempotent -- re-reading a
    claim already present updates it in place rather than duplicating it.

    Choosing N is a business decision, not a technical one: it is the reporting
    lag you are willing to absorb, and mart_claims_lag measures the actual
    distribution so the choice can be evidence-based rather than a guess.
    Default 30 days, set by the claims_lookback_days var.

    The cost is re-reading 30 days of claims on every run. That is cheap here and
    remains cheap at realistic volumes because claims are low-cardinality
    relative to policies. If it stopped being cheap, the alternative is to
    partition by loss month and rewrite only affected partitions.
*/

with internal as (
    select
        claim_id, claim_no, policy_id, loss_date, reported_date, settled_date,
        reporting_lag_days, claim_cause, claim_type, claim_status,
        gross_incurred, paid_amount, reserve_amount, currency, at_fault,
        'internal' as record_source, batch_date, ingested_at
    from {{ ref('stg_claims') }}

    {% if is_incremental() %}
    -- Re-read a window of batches, not just the newest, so late-reported
    -- claims land in the period they belong to.
    where batch_date >= date_sub(
        (select coalesce(max(batch_date), '1900-01-01') from {{ this }}),
        {{ var('claims_lookback_days') }})
    {% endif %}
),

partner as (
    select
        pc.claim_no as claim_id, pc.claim_no, pc.policy_id,
        pc.loss_date, pc.reported_date,
        cast(null as date) as settled_date,
        pc.reporting_lag_days, pc.claim_cause,
        cast(null as string) as claim_type,
        pc.claim_status, pc.gross_incurred,
        cast(null as decimal(18, 2)) as paid_amount,
        cast(null as decimal(18, 2)) as reserve_amount,
        pc.currency,
        cast(null as boolean) as at_fault,
        'partner' as record_source, pc.batch_date, pc.ingested_at
    from {{ ref('stg_partner_claims') }} pc
    where not exists (select 1 from internal i where i.claim_no = pc.claim_no)

    {% if is_incremental() %}
    and pc.batch_date >= date_sub(
        (select coalesce(max(batch_date), '1900-01-01') from {{ this }}),
        {{ var('claims_lookback_days') }})
    {% endif %}
),

combined as (
    select * from internal
    union all
    select * from partner
)

select
    c.claim_id,
    c.claim_no,
    c.policy_id,
    c.loss_date,
    c.reported_date,
    c.settled_date,
    c.reporting_lag_days,
    -- The flag mart_claims_lag reports on. 21 days is the threshold the
    -- generator uses to mark a claim late; keeping it here makes the
    -- definition visible to analysts rather than buried in a generator.
    c.reporting_lag_days >= 21 as is_late_reported,
    c.claim_cause,
    c.claim_type,
    c.claim_status,
    c.gross_incurred,
    c.paid_amount,
    c.reserve_amount,
    c.currency,
    {{ convert_currency('c.gross_incurred', 'c.currency', 'c.loss_date') }}
        as gross_incurred_thb,
    {{ convert_currency('c.paid_amount', 'c.currency', 'c.loss_date') }}
        as paid_amount_thb,
    c.at_fault,
    c.record_source,
    c.batch_date,
    current_timestamp() as _silver_loaded_at
from combined c
{{ fx_join('c.currency', 'c.loss_date') }}
