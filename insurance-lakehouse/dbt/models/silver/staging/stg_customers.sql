{{ config(materialized='view') }}
/*  Grain: one row per customer_id. SCD1 -- latest wins.

    PII is NOT masked here. Silver keeps identifiable values so that operational
    joins and right-to-erasure lookups remain possible; masking is applied at the
    gold boundary, which is what analysts and BI tools read. Access to silver is
    the control; masking in gold is the default. */
with ranked as (
    select
        {{ clean_code('customer_id') }}             as customer_id,
        {{ clean_text('first_name') }}              as first_name,
        {{ clean_text('last_name') }}               as last_name,
        {{ clean_text('national_id') }}             as national_id,
        cast(date_of_birth as date)                 as date_of_birth,
        {{ clean_text('email') }}                   as email,
        {{ clean_text('phone') }}                   as phone,
        {{ clean_text('address') }}                 as address,
        {{ clean_text('province') }}                as province,
        {{ clean_code('country') }}                 as country,
        cast(created_at as date)                    as created_date,
        _ingested_at, _record_hash, _batch_date,
        {{ dedupe_latest('customer_id', 'cast(created_at as timestamp)') }} as row_num
    from {{ source('bronze', 'ref_customers') }}
    where customer_id is not null
)
select
    customer_id, first_name, last_name, national_id, date_of_birth,
    email, phone, address, province, country, created_date,
    floor(datediff(current_date(), date_of_birth) / 365.25) as age_years,
    _ingested_at as ingested_at
from ranked
where row_num = 1
