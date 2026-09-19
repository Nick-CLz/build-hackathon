{{ config(materialized='table') }}

/*
    Earned premium by policy-month. Grain: one row per (policy_id, month_start).

    THE CORE INSURANCE CALCULATION, and the one most often got wrong.

    Written premium lands entirely at inception. Earned premium accrues across
    the coverage period: a THB 12,000 annual policy incepting 1 March has
    written 12,000 in March but EARNED only about 1,000 by 31 March. Judging
    March's loss ratio against the 12,000 makes a healthy book look catastrophic
    in its first month and unreasonably profitable later.

    Method: pro-rata by days of exposure in each calendar month. For each month
    the policy is on risk, earned = written * (days on risk in month / term days).

    Three details that separate a correct implementation from a plausible one:

      1. CANCELLATION TRUNCATES EXPOSURE. A policy cancelled mid-term stops
         earning on the cancellation date, not at expiry. Missing this
         overstates earned premium on every cancelled policy, which flatters
         loss ratio precisely where the book is worst.

      2. EXPOSURE STOPS AT TODAY. A policy running to next year has not yet
         earned next year's premium. Aggregating future months would book
         revenue that has not happened.

      3. THE INTERVAL MUST BE HALF-OPEN, [start, end). The first version of
         this model used an inclusive day count (datediff + 1) per month. Each
         month boundary was then counted twice, so a 365-day policy spanning 13
         calendar months earned 378 days of premium -- 3.6% more than was ever
         written. It produced earned > written on 1196 of 1546 policies, and
         the portfolio total looked unremarkable, which is precisely why the
         singular test earned_premium <= written_premium exists.

         With half-open intervals the months tile exactly: the sum of
         datediff(period_end_exclusive, period_start) over all months equals
         datediff(exposure_end, inception), by construction.

    Exposure in policy-years is emitted alongside, because claim FREQUENCY needs
    an exposure denominator rather than a policy count -- a policy on risk for
    one month is not comparable to one on risk for a year.
*/

with policies as (
    select
        p.policy_id, p.product_code, p.partner_code, p.country, p.currency,
        p.inception_date, p.expiry_date, p.policy_status,
        p.written_premium_thb, p.term_days,
        -- Exposure ends at the earliest of expiry, cancellation, and today.
        least(
            p.expiry_date,
            current_date(),
            case when p.policy_status = 'CANCELLED'
                 -- Cancellation date is not carried on the policy record, so
                 -- fall back to the refund payment that records it.
                 then coalesce(
                     (select min(pay.effective_date)
                      from {{ ref('silver_premium_payments') }} pay
                      where pay.policy_id = p.policy_id and pay.is_refund),
                     p.expiry_date)
                 else p.expiry_date
            end
        ) as exposure_end_date
    from {{ ref('silver_policies') }} p
    where p.inception_date is not null
      and p.expiry_date is not null
      and p.term_days > 0
),

months as (
    select
        pol.*,
        explode(sequence(
            date_trunc('month', pol.inception_date),
            date_trunc('month', pol.exposure_end_date),
            interval 1 month
        )) as month_start
    from policies pol
    where pol.exposure_end_date >= pol.inception_date
),

exposure as (
    select
        m.*,
        -- Half-open [period_start, period_end_excl): consecutive months abut
        -- without overlapping, so no day is counted twice.
        greatest(cast(m.month_start as date), m.inception_date) as period_start,
        least(
            add_months(cast(m.month_start as date), 1),
            m.exposure_end_date
        ) as period_end_excl
    from months m
)

select
    e.policy_id,
    e.product_code,
    e.partner_code,
    e.country,
    e.currency,
    cast(e.month_start as date)                     as month_start,
    date_format(e.month_start, 'yyyy-MM')           as earned_year_month,
    e.inception_date,
    e.expiry_date,
    e.exposure_end_date,
    e.policy_status,
    e.written_premium_thb,
    e.term_days,
    datediff(e.period_end_excl, e.period_start)     as days_on_risk,
    round(
        e.written_premium_thb
        * datediff(e.period_end_excl, e.period_start) / e.term_days
    , 2)                                            as earned_premium_thb,
    -- Exposure in policy-years: the correct denominator for claim frequency.
    round(datediff(e.period_end_excl, e.period_start) / 365.25, 6)
                                                    as exposure_policy_years
from exposure e
where e.period_end_excl > e.period_start
