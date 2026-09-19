{{ config(materialized='view') }}
/*  Grain: one row per claim_id, from the internal claims service.

    reporting_lag_days is computed here rather than trusted from the source, so
    that mart_claims_lag measures what the dates actually say. A claim with a
    large lag is the late-arrival case the incremental model's lookback window
    exists to absorb. */
with ranked as (
    select
        {{ clean_code('claim_id') }} as claim_id,
        {{ clean_code('claim_no') }} as claim_no,
        {{ clean_code('policy_id') }} as policy_id,
        cast(loss_date as date) as loss_date,
        cast(reported_date as date) as reported_date,
        cast(settled_date as date) as settled_date,
        lower({{ clean_text('claim_cause') }}) as claim_cause,
        lower({{ clean_text('claim_type') }}) as claim_type,
        {{ clean_code('status') }} as claim_status,
        cast(gross_incurred as decimal(18, 2)) as gross_incurred,
        cast(paid_amount as decimal(18, 2)) as paid_amount,
        cast(reserve_amount as decimal(18, 2)) as reserve_amount,
        {{ clean_code('currency') }} as currency,
        cast(at_fault as boolean) as at_fault,
        _ingested_at,
        _record_hash,
        _batch_date,
        {{ dedupe_latest('claim_id', 'cast(reported_date as timestamp)') }} as row_num
    from {{ source('bronze', 'events_claims') }}
    where claim_id is not null
)

select
    claim_id,
    claim_no,
    policy_id,
    loss_date,
    reported_date,
    settled_date,
    datediff(reported_date, loss_date) as reporting_lag_days,
    claim_cause,
    claim_type,
    claim_status,
    gross_incurred,
    paid_amount,
    reserve_amount,
    currency,
    at_fault,
    _ingested_at as ingested_at,
    _batch_date as batch_date
from ranked
where row_num = 1
