{{ config(materialized='view') }}
/*  Grain: one row per endorsement_id.

    effective_date and requested_date are kept separate on purpose. effective_date
    drives exposure and earned premium; requested_date drives when we knew. A
    backdated endorsement has effective_date < requested_date, and collapsing the
    two is the most common way earned premium silently goes wrong. */
with ranked as (
    select
        {{ clean_code('endorsement_id') }}          as endorsement_id,
        {{ clean_code('policy_id') }}               as policy_id,
        {{ clean_code('endorsement_type') }}        as endorsement_type,
        cast(effective_date as date)                as effective_date,
        cast(requested_date as date)                as requested_date,
        cast(old_sum_insured as decimal(18, 2))     as old_sum_insured,
        cast(new_sum_insured as decimal(18, 2))     as new_sum_insured,
        cast(premium_delta as decimal(18, 2))       as premium_delta,
        {{ clean_code('currency') }}                as currency,
        cast(is_backdated as boolean)               as is_backdated,
        _ingested_at, _record_hash, _batch_date,
        {{ dedupe_latest('endorsement_id', 'cast(requested_date as timestamp)') }} as row_num
    from {{ source('bronze', 'events_endorsements') }}
    where endorsement_id is not null
)
select
    endorsement_id, policy_id, endorsement_type,
    effective_date, requested_date,
    datediff(requested_date, effective_date) as backdated_days,
    old_sum_insured, new_sum_insured, premium_delta, currency, is_backdated,
    _ingested_at as ingested_at, _batch_date as batch_date
from ranked
where row_num = 1
