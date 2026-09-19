{{ config(materialized='view') }}
/*  Grain: one row per (policy_id, ts) -- every change event, not the current state.

    Bronze keeps the full change feed; this model types it and exposes the op.
    Collapsing to current state happens in silver_policies, so that the history
    remains available for the SCD2 snapshot and for auditing what changed when. */
select
    {{ clean_code('policy_id') }} as policy_id,
    {{ clean_code('op') }} as change_op,
    cast(ts as timestamp) as change_ts,
    {{ clean_code('customer_id') }} as customer_id,
    {{ clean_code('product_code') }} as product_code,
    {{ clean_code('partner_code') }} as partner_code,
    {{ clean_code('country') }} as country,
    {{ clean_code('currency') }} as currency,
    {{ clean_code('vehicle_id') }} as vehicle_id,
    cast(inception_date as date) as inception_date,
    cast(expiry_date as date) as expiry_date,
    cast(sum_insured as decimal(18, 2)) as sum_insured,
    cast(written_premium as decimal(18, 2)) as written_premium,
    {{ normalise_policy_status('status') }} as policy_status,
    cast(updated_at as date) as source_updated_at,
    _ingested_at as ingested_at,
    _batch_date as batch_date
from {{ source('bronze', 'cdc_policies') }}
where policy_id is not null and ts is not null
