{{ config(materialized='view') }}
/*  Grain: one row per partner_code. */
with ranked as (
    select
        {{ clean_code('partner_code') }} as partner_code,
        {{ clean_text('partner_name') }} as partner_name,
        {{ clean_code('country') }} as country,
        {{ clean_code('schema_variant') }} as schema_variant,
        cast(commission_rate as decimal(9, 4)) as commission_rate,
        cast(onboarded_date as date) as onboarded_date,
        _ingested_at,
        _record_hash,
        {{ dedupe_latest('partner_code', '_ingested_at') }} as row_num
    from {{ source('bronze', 'ref_partner_insurers') }}
    where partner_code is not null
)

select
    partner_code,
    partner_name,
    country,
    schema_variant,
    commission_rate,
    onboarded_date,
    _ingested_at as ingested_at
from ranked
where row_num = 1
