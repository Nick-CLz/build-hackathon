{{ config(materialized='view') }}

/*
    Harmonise the three partner claim feeds.

    Grain: one row per claim_no (latest version).

    Note that partner claim files carry no explicit timestamp column, so dedup
    orders by reported_date and falls back to ingestion order. That is weaker
    than the policy feed's ordering and is called out in the model docs, because
    a reader should know which dedup decisions are well-founded and which are
    best-effort.
*/

with variant_a as (
    select
        {{ clean_code('claim_no') }} as claim_no,
        {{ clean_code('policy_no') }} as policy_id,
        {{ be_to_ce_date('loss_date') }} as loss_date,
        {{ be_to_ce_date('reported_date') }} as reported_date,
        cast(incurred_amt as decimal(18, 2)) as gross_incurred,
        {{ clean_code('currency_cd') }} as currency,
        lower({{ clean_text('cause_of_loss') }}) as claim_cause,
        {{ clean_code('claim_status') }} as claim_status,
        _source_file,
        _ingested_at,
        _record_hash,
        _batch_date
    from {{ source('bronze', 'partner_claims_a') }}
),

variant_b as (
    select
        {{ clean_code('ClaimNumber') }} as claim_no,
        {{ clean_code('PolicyNumber') }} as policy_id,
        {{ be_to_ce_date('LossDate') }} as loss_date,
        {{ be_to_ce_date('NotifiedDate') }} as reported_date,
        cast(incurredamount as decimal(18, 2)) as gross_incurred,
        {{ clean_code('Curr') }} as currency,
        lower({{ clean_text('CauseOfLoss') }}) as claim_cause,
        {{ clean_code('ClaimStatus') }} as claim_status,
        _source_file,
        _ingested_at,
        _record_hash,
        _batch_date
    from {{ source('bronze', 'partner_claims_b') }}
),

variant_c as (
    select
        {{ clean_code('nomor_klaim') }} as claim_no,
        {{ clean_code('nomor_polis') }} as policy_id,
        {{ be_to_ce_date('tanggal_kejadian') }} as loss_date,
        {{ be_to_ce_date('tanggal_lapor') }} as reported_date,
        cast(jumlah_klaim as decimal(18, 2)) as gross_incurred,
        {{ clean_code('mata_uang') }} as currency,
        lower({{ clean_text('penyebab') }}) as claim_cause,
        {{ clean_code('status_klaim') }} as claim_status,
        _source_file,
        _ingested_at,
        _record_hash,
        _batch_date
    from {{ source('bronze', 'partner_claims_c') }}
),

unioned as (
    select * from variant_a
    union all
    select * from variant_b
    union all
    select * from variant_c
),

ranked as (
    select
        *,
        {{ partner_code_from_source_file() }} as partner_code,
        {{ dedupe_latest('claim_no', 'reported_date') }} as row_num
    from unioned
)

select
    claim_no,
    policy_id,
    partner_code,
    loss_date,
    reported_date,
    datediff(reported_date, loss_date) as reporting_lag_days,
    gross_incurred,
    currency,
    claim_cause,
    claim_status,
    _ingested_at as ingested_at,
    _batch_date as batch_date
from ranked
where row_num = 1
