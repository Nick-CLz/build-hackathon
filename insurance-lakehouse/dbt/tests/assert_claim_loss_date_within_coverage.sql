{{ config(severity='warn') }}
/*
    A claim's loss date should fall inside the policy's coverage period.

    WARN, not error, and the distinction is the point. The generator injects
    out-of-cover losses deliberately, because they genuinely occur: a partner
    mis-keys a date, or a claim is filed against the wrong policy version. These
    are real-world data-quality defects that the warehouse should MEASURE, not
    refuse to build over.

    Erroring here would mean one bad partner row blocks the entire nightly
    build, including the 99.9% of the book that is fine. That trade is almost
    never worth it: a stale warehouse is usually more damaging than a warehouse
    with a known, quantified defect rate.

    The count is what matters. If it trends up, a partner feed has broken.
*/
select
    claim_id,
    claim_no,
    policy_id,
    loss_date,
    claim_status
from {{ ref('fct_claims') }}
where is_outside_coverage
