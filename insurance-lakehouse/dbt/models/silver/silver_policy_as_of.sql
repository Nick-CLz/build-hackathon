{{ config(materialized='table') }}

/*
    Policy state as at any date, built on the SCD2 snapshot.

    Grain: one row per (policy_id, valid_from) -- i.e. per policy version, with
    an explicit validity window.

    Usage: to get the book as it stood on any date D,

        select * from silver_policy_as_of
        where D >= valid_from and D < coalesce(valid_to, '9999-12-31')

    This is what makes "what was our in-force exposure on 30 June" answerable
    after the fact, rather than only "what is it now". Earned premium needs it,
    and so does any restatement: without version history, a book that has been
    endorsed cannot be reconstructed as it actually was.

    valid_to is left NULL on the current version rather than filled with a
    sentinel, so `is_current` and an open-ended window both read naturally. The
    coalesce above is the only place the sentinel appears.
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
    record_source,
    cast(dbt_valid_from as date) as valid_from,
    cast(dbt_valid_to as date) as valid_to,
    dbt_valid_to is null as is_current,
    row_number() over (
        partition by policy_id order by dbt_valid_from
    ) as version_number,
    count(*) over (partition by policy_id) as total_versions
from {{ ref('policies_snapshot') }}
