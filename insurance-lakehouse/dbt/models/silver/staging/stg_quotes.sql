{{ config(materialized='view') }}
/*  Grain: one row per quote_id. Source: internal quote service, JSON lines.
    Internal events are already well-typed, so this is casting and dedup only. */
with ranked as (
    select
        {{ clean_code('quote_id') }}                as quote_id,
        {{ clean_code('customer_id') }}             as customer_id,
        {{ clean_code('product_code') }}            as product_code,
        {{ clean_code('partner_code') }}            as partner_code,
        {{ clean_code('country') }}                 as country,
        {{ clean_code('currency') }}                as currency,
        cast(quoted_at as date)                     as quoted_date,
        cast(quoted_premium as decimal(18, 2))      as quoted_premium,
        lower({{ clean_text('channel') }})          as channel,
        {{ clean_code('status') }}                  as quote_status,
        {{ clean_code('converted_policy_id') }}     as converted_policy_id,
        _ingested_at, _record_hash, _batch_date,
        {{ dedupe_latest('quote_id', 'cast(quoted_at as timestamp)') }} as row_num
    from {{ source('bronze', 'events_quotes') }}
    where quote_id is not null
)
select
    quote_id, customer_id, product_code, partner_code, country, currency,
    quoted_date, quoted_premium, channel, quote_status, converted_policy_id,
    converted_policy_id is not null as did_convert,
    _ingested_at as ingested_at, _batch_date as batch_date
from ranked
where row_num = 1
