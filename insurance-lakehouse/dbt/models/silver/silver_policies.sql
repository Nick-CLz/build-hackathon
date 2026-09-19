{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='policy_id',
    file_format='delta'
) }}

/*
    Current state of every policy. Grain: one row per policy_id.

    Two sources disagree about the same policies on purpose:
      - the CDC feed is authoritative for state (it carries op and ts)
      - the partner files are authoritative for what the partner believes

    The CDC feed wins where both exist, because it comes from our own system of
    record. Partner rows fill in policies the CDC feed has not seen yet, which
    happens when a partner reports a sale before the internal service catches up.

    Deletes are honoured: a policy whose latest CDC op is 'D' is excluded. This
    is the CDC trap worth naming -- a pipeline that only processes I and U
    leaves deleted rows alive downstream forever.
*/

with cdc_latest as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by policy_id
                order by change_ts desc, ingested_at desc
            ) as rn
        from {{ ref('stg_policies_cdc') }}
        {% if is_incremental() %}
        where
            batch_date >= date_sub(
                (select coalesce(max(batch_date), '1900-01-01') from {{ this }}),
                {{ var('claims_lookback_days') }}
            )
        {% endif %}
    )
    where rn = 1
),

from_cdc as (
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
        sum_insured,
        written_premium,
        policy_status,
        source_updated_at,
        change_ts,
        'cdc' as record_source,
        cast(null as string) as national_id,
        cast(null as string) as plate_no,
        batch_date
    from cdc_latest
    where change_op <> 'D'
),

from_partner as (
    select
        p.policy_id,
        cast(null as string) as customer_id,
        cast(null as string) as vehicle_id,
        p.product_code,
        p.partner_code,
        case when p.currency = 'THB' then 'TH' else 'ID' end as country,
        p.currency,
        p.inception_date,
        p.expiry_date,
        p.sum_insured,
        p.written_premium,
        p.policy_status,
        cast(p.source_updated_at as date) as source_updated_at,
        p.source_updated_at as change_ts,
        'partner' as record_source,
        p.national_id,
        p.plate_no,
        p.batch_date
    from {{ ref('stg_partner_policies') }} as p
    where not exists (
        select 1 from from_cdc as c
        where c.policy_id = p.policy_id
    )
),

combined as (
    select * from from_cdc
    union all
    select * from from_partner
)

select
    c.policy_id,
    c.customer_id,
    c.vehicle_id,
    c.product_code,
    c.partner_code,
    c.country,
    c.currency,
    c.national_id,
    c.plate_no,
    c.inception_date,
    c.expiry_date,
    c.policy_status,
    c.sum_insured,
    c.written_premium,
    -- Written premium in the reporting currency, at the rate on inception date.
    {{ convert_currency('c.written_premium', 'c.currency', 'c.inception_date') }}
        as written_premium_thb,
    {{ convert_currency('c.sum_insured', 'c.currency', 'c.inception_date') }}
        as sum_insured_thb,
    datediff(c.expiry_date, c.inception_date) as term_days,
    c.record_source,
    c.source_updated_at,
    c.change_ts,
    c.batch_date,
    current_timestamp() as _silver_loaded_at
from combined as c
{{ fx_join('c.currency', 'c.inception_date') }}
