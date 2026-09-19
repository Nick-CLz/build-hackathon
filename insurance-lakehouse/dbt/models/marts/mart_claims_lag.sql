{{ config(materialized='table') }}

/*
    Reporting-delay distribution. Grain: one row per
    (loss_year_month, country, product_code).

    This mart exists to make late arrival VISIBLE and to turn the lookback
    window from a guess into a measured decision.

    The percentiles are the operative output. If p99 of reporting lag is 45 days
    and claims_lookback_days is 30, roughly 1% of claims are landing outside the
    window and permanently understating their loss month. The number to set the
    var to is right here, and it should be revisited as the book changes rather
    than fixed once.

    Also surfaces claims_still_developing: months where claims are still
    arriving are not yet fully developed, and their loss ratio will rise. Reading
    a recent month's loss ratio as final is a classic misreading, and flagging
    it in the mart is cheaper than explaining it repeatedly.
*/

with lagged as (
    select
        loss_year_month,
        loss_date,
        country,
        product_code,
        reporting_lag_days,
        is_late_reported,
        gross_incurred_thb
    from {{ ref('fct_claims') }}
    where not is_orphan and reporting_lag_days is not null
)

select
    loss_year_month,
    country,
    product_code,
    count(*)                                            as claim_count,
    round(avg(reporting_lag_days), 1)                   as avg_lag_days,
    min(reporting_lag_days)                             as min_lag_days,
    max(reporting_lag_days)                             as max_lag_days,
    percentile_approx(reporting_lag_days, 0.50)         as p50_lag_days,
    percentile_approx(reporting_lag_days, 0.90)         as p90_lag_days,
    percentile_approx(reporting_lag_days, 0.95)         as p95_lag_days,
    percentile_approx(reporting_lag_days, 0.99)         as p99_lag_days,
    sum(case when is_late_reported then 1 else 0 end)   as late_reported_count,
    round(sum(case when is_late_reported then 1 else 0 end) / count(*), 4)
                                                        as late_reported_pct,
    -- Claims that would fall outside the configured lookback window, i.e. the
    -- ones a re-run would NOT pick up. This should be close to zero.
    sum(case when reporting_lag_days > {{ var('claims_lookback_days') }}
             then 1 else 0 end)                         as beyond_lookback_window,
    sum(case when is_late_reported then gross_incurred_thb else 0 end)
                                                        as late_reported_incurred_thb,
    -- A loss month is still developing if the newest claim for it arrived
    -- recently relative to the lookback window.
    max(loss_date) + max(reporting_lag_days) > date_sub(current_date(),
        {{ var('claims_lookback_days') }})              as claims_still_developing
from lagged
group by 1, 2, 3
