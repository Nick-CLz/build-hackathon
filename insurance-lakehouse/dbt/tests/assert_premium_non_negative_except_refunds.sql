{{ config(severity='error') }}
/*
    Premium amounts must be non-negative unless the row is a refund.

    ERROR severity because the carve-out makes it unambiguous. Negative amounts
    ARE legitimate here -- a mid-term cancellation produces a pro-rata refund --
    so the test does not flag negativity itself. It flags a negative amount that
    is NOT marked as a refund, which means either the refund flag is wrong or a
    genuine negative premium has appeared. Both are structural faults worth
    failing on.

    This is the difference between a test that measures quality and one that
    cries wolf: the naive "amount >= 0" would fire on every legitimate refund,
    get muted within a week, and then catch nothing.
*/
select
    payment_id,
    policy_id,
    amount,
    amount_thb,
    is_refund,
    payment_status
from {{ ref('silver_premium_payments') }}
where amount < 0 and not is_refund
