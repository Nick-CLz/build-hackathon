{{ config(materialized='table') }}
/*  Grain: one row per partner_code. schema_variant is retained deliberately --
    it lets you ask "are the feeds we find hardest to parse also the ones with
    the worst data quality", which is an operational question worth answering. */
select
    pt.partner_code,
    pt.partner_name,
    pt.country,
    pt.schema_variant,
    pt.commission_rate,
    pt.onboarded_date,
    datediff(current_date(), pt.onboarded_date) / 365.25 as tenure_years
from {{ ref('stg_partners') }} as pt
