{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='payment_id',
    file_format='delta'
) }}

/*
    Premium instalments and refunds. Grain: one row per payment_id.

    Uses the same lookback treatment as claims: a payment can be recorded after
    its due date, and a cancellation refund is always emitted in a later batch
    than the policy it refunds.

    Negative amounts are legitimate -- they are pro-rata cancellation refunds.
    The premium >= 0 test carves them out via is_refund rather than flagging
    them as bad data, which is the difference between a test that measures
    quality and a test that cries wolf.
*/

select
    p.payment_id,
    p.policy_id,
    p.installment_no,
    p.due_date,
    p.paid_date,
    p.effective_date,
    p.amount,
    p.currency,
    {{ convert_currency('p.amount', 'p.currency', 'p.effective_date') }} as amount_thb,
    p.payment_method,
    p.payment_status,
    p.is_refund,
    p.batch_date,
    current_timestamp() as _silver_loaded_at
from {{ ref('stg_premium_payments') }} p
{{ fx_join('p.currency', 'p.effective_date') }}

{% if is_incremental() %}
where p.batch_date >= date_sub(
    (select coalesce(max(batch_date), '1900-01-01') from {{ this }}),
    {{ var('claims_lookback_days') }})
{% endif %}
