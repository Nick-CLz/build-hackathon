{{ config(materialized='view') }}
/*  Grain: one row per (rate_date, from_currency, to_currency).

    This is the FX spine convert_currency() joins against. It carries an identity
    row (THB -> THB = 1.0) for every date so that domestic amounts take the same
    code path as foreign ones -- no special case to forget, and a missing rate
    shows up as NULL rather than as an unconverted number passed through. */
with ranked as (
    select
        cast(rate_date as date)                     as rate_date,
        {{ clean_code('from_currency') }}           as from_currency,
        {{ clean_code('to_currency') }}             as to_currency,
        cast(rate as decimal(18, 8))                as rate,
        _ingested_at, _record_hash,
        {{ dedupe_latest('cast(rate_date as date), from_currency, to_currency',
                         '_ingested_at') }} as row_num
    from {{ source('bronze', 'ref_fx_rates') }}
    where rate_date is not null and rate is not null
)
select rate_date, from_currency, to_currency, rate, _ingested_at as ingested_at
from ranked
where row_num = 1
