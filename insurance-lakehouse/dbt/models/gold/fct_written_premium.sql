{{ config(materialized='table') }}
/*  Grain: one row per policy_id.

    Written premium is recognised in full AT INCEPTION -- the whole annual
    premium is written the day the contract is bound, even if only one day has
    elapsed. That is what makes it the wrong denominator for a loss ratio, and
    why fct_earned_premium exists alongside it. */
select
    p.policy_id,
    p.customer_id,
    p.vehicle_id,
    p.product_code,
    p.partner_code,
    p.country,
    p.currency,
    p.inception_date,
    p.expiry_date,
    cast(date_format(p.inception_date, 'yyyyMMdd') as int) as inception_date_key,
    date_format(p.inception_date, 'yyyy-MM') as written_year_month,
    p.policy_status,
    p.term_days,
    p.written_premium,
    p.written_premium_thb,
    p.sum_insured,
    p.sum_insured_thb,
    p.record_source
from {{ ref('silver_policies') }} as p
where p.inception_date is not null
