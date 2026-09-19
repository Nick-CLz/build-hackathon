{{ config(materialized='table') }}
/*  Grain: one row per claim_id.

    Dated by LOSS date for loss-ratio purposes and by REPORTED date for
    operational reporting -- both keys are exposed because the two answer
    different questions, and silently picking one is how a claims report and a
    finance report stop agreeing.

    Orphan claims (policy_id not present in silver_policies) are kept, not
    dropped, with an is_orphan flag. Dropping them would hide a referential
    integrity problem that the relationships test is supposed to surface. */
select
    c.claim_id,
    c.claim_no,
    c.policy_id,
    p.product_code,
    p.partner_code,
    p.country,
    c.loss_date,
    c.reported_date,
    c.settled_date,
    cast(date_format(c.loss_date, 'yyyyMMdd') as int) as loss_date_key,
    cast(date_format(c.reported_date, 'yyyyMMdd') as int) as reported_date_key,
    date_format(c.loss_date, 'yyyy-MM') as loss_year_month,
    date_format(c.reported_date, 'yyyy-MM') as reported_year_month,
    c.reporting_lag_days,
    c.is_late_reported,
    c.claim_cause,
    c.claim_type,
    c.claim_status,
    c.currency,
    c.gross_incurred,
    c.gross_incurred_thb,
    c.paid_amount_thb,
    c.at_fault,
    c.record_source,
    p.policy_id is null as is_orphan,
    -- A loss outside the coverage period is a data-quality signal, not a
    -- reason to drop the row. The singular test reports on it.
    (
        p.policy_id is not null
        and (c.loss_date < p.inception_date or c.loss_date > p.expiry_date))
        as is_outside_coverage
from {{ ref('silver_claims') }} as c
left join {{ ref('silver_policies') }} as p on c.policy_id = p.policy_id
