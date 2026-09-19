{{ config(materialized='table') }}

/*
    Loss ratio by product / partner / country / month.
    Grain: one row per (earned_year_month, country, partner_code, product_code).

    Loss ratio = incurred claims / EARNED premium. Using written premium instead
    is the single most common error in insurance reporting: it makes a growing
    book look profitable (premium written today, claims arriving later) and a
    shrinking one look catastrophic. The whole reason fct_earned_premium exists
    is to make the correct denominator available.

    Claims are attributed by LOSS month, not reported month, so a late-reported
    claim lands in the period it actually belongs to. That is what makes the
    lookback window in silver_claims matter: without it, an old month's claims
    would never be re-read, and its loss ratio would stay permanently understated.

    loss_ratio is NULL, not zero, when earned premium is zero. A zero would
    average into portfolio rollups as though it were a real, excellent result.
*/

with earned as (
    select
        earned_year_month, month_start, country, partner_code, product_code,
        sum(earned_premium_thb)     as earned_premium_thb,
        sum(exposure_policy_years)  as exposure_policy_years,
        count(distinct policy_id)   as policies_on_risk
    from {{ ref('fct_earned_premium') }}
    group by 1, 2, 3, 4, 5
),

claims as (
    select
        loss_year_month, country, partner_code, product_code,
        count(*)                                        as claim_count,
        sum(gross_incurred_thb)                         as incurred_thb,
        sum(paid_amount_thb)                            as paid_thb,
        sum(case when is_late_reported then 1 else 0 end) as late_reported_claims
    from {{ ref('fct_claims') }}
    where not is_orphan
    group by 1, 2, 3, 4
)

select
    e.earned_year_month                                 as year_month,
    e.month_start,
    e.country,
    e.partner_code,
    e.product_code,
    pr.product_name_en,
    pr.cover_class,
    pr.line_of_business,
    e.policies_on_risk,
    e.earned_premium_thb,
    e.exposure_policy_years,
    coalesce(c.claim_count, 0)                          as claim_count,
    coalesce(c.incurred_thb, 0)                         as incurred_thb,
    coalesce(c.paid_thb, 0)                             as paid_thb,
    coalesce(c.late_reported_claims, 0)                 as late_reported_claims,
    case when e.earned_premium_thb > 0
         then round(coalesce(c.incurred_thb, 0) / e.earned_premium_thb, 4)
    end                                                 as loss_ratio,
    -- Claim frequency per policy-year of EXPOSURE, not per policy. A policy on
    -- risk for one month is not comparable to one on risk for a year.
    case when e.exposure_policy_years > 0
         then round(coalesce(c.claim_count, 0) / e.exposure_policy_years, 4)
    end                                                 as claim_frequency,
    case when coalesce(c.claim_count, 0) > 0
         then round(c.incurred_thb / c.claim_count, 2)
    end                                                 as claim_severity_thb,
    -- CREDIBILITY. A monthly cell with a fraction of a policy-year of exposure
    -- and one claim produces a loss ratio of 30+, which is arithmetically
    -- correct and actuarially meaningless. Flagging low-credibility cells is
    -- standard practice: they should be excluded from decisions and from
    -- range tests, not quietly averaged into a portfolio view.
    (e.exposure_policy_years >= 5 and coalesce(c.claim_count, 0) >= 3)
                                                        as is_credible,
    case
        when e.exposure_policy_years < 1 then 'very_low'
        when e.exposure_policy_years < 5 then 'low'
        when e.exposure_policy_years < 25 then 'partial'
        else 'full'
    end                                                 as credibility_band
from earned e
left join claims c
    on  c.loss_year_month = e.earned_year_month
    and c.country         = e.country
    and c.partner_code    = e.partner_code
    and c.product_code    = e.product_code
left join {{ ref('dim_product') }} pr on pr.product_code = e.product_code
