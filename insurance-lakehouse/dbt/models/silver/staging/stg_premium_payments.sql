{{ config(materialized='view') }}
/*  Grain: one row per payment_id. Negative amounts are pro-rata cancellation
    refunds and are legitimate -- the premium >= 0 test carves them out via
    is_refund rather than treating them as bad data. */
with ranked as (
    select
        {{ clean_code('payment_id') }} as payment_id,
        {{ clean_code('policy_id') }} as policy_id,
        cast(installment_no as int) as installment_no,
        cast(due_date as date) as due_date,
        cast(paid_date as date) as paid_date,
        cast(amount as decimal(18, 2)) as amount,
        {{ clean_code('currency') }} as currency,
        lower({{ clean_text('payment_method') }}) as payment_method,
        {{ clean_code('status') }} as payment_status,
        cast(is_refund as boolean) as is_refund,
        _ingested_at,
        _record_hash,
        _batch_date,
        {{ dedupe_latest('payment_id', 'cast(paid_date as timestamp)') }} as row_num
    from {{ source('bronze', 'events_premium_payments') }}
    where payment_id is not null
)

select
    payment_id,
    policy_id,
    installment_no,
    due_date,
    paid_date,
    amount,
    currency,
    payment_method,
    payment_status,
    is_refund,
    coalesce(paid_date, due_date) as effective_date,
    _ingested_at as ingested_at,
    _batch_date as batch_date
from ranked
where row_num = 1
