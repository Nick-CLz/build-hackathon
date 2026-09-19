{{ config(severity='error') }}
/*
    A policy can never earn more premium than was written for it.

    ERROR severity: this is an accounting identity, not a data-quality signal.
    If it breaks, the earned-premium model is wrong and every loss ratio built
    on it is wrong. An earlier version of fct_earned_premium used an inclusive
    day count per month, double-counting each month boundary; it produced
    earned > written on 1196 of 1546 policies while the portfolio total looked
    unremarkable. This test is what makes that class of error impossible to
    ship unnoticed.

    TOLERANCE, and why it is not zero. earned_premium_thb is rounded to 2dp per
    policy-month, because it is a monetary column. Summing ~13 rounded months
    can drift by a few thousandths of a baht, which on a THB 645 compulsory
    motor premium is a relative error of ~0.0001. The tolerance is therefore the
    greater of 1 THB and 0.1% -- comfortably above accumulated rounding, far
    below any real modelling error, which shows up as percent-level excess.
*/
with per_policy as (
    select
        policy_id,
        max(written_premium_thb) as written_premium_thb,
        sum(earned_premium_thb)  as earned_premium_thb
    from {{ ref('fct_earned_premium') }}
    group by policy_id
)
select
    policy_id,
    written_premium_thb,
    earned_premium_thb,
    earned_premium_thb - written_premium_thb as excess_thb
from per_policy
where earned_premium_thb >
      written_premium_thb + greatest(1.0, abs(written_premium_thb) * 0.001)
