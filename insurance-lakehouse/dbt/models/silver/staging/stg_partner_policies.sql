{{ config(materialized='view') }}

/*
    Harmonise three incompatible partner policy feeds into one shape.

    Grain: one row per (partner_code, policy_id) -- the latest version seen.

    The three feeds disagree on column names, language, date convention and
    currency. They are registered as separate bronze sources precisely because
    no single schema fits them; this model is where they become one table.

    Dedup happens here rather than downstream because a partner resending a
    record is a transport artefact, not a business event. The tie-break is
    documented in dedupe_latest().
*/

with variant_a as (
    select
        {{ clean_code('policy_no') }}                     as policy_id,
        {{ clean_text('cust_national_id') }}              as national_id,
        {{ clean_code('product_cd') }}                    as product_code,
        {{ be_to_ce_date('start_date') }}                 as inception_date,
        {{ be_to_ce_date('end_date') }}                   as expiry_date,
        cast(premium_amt as decimal(18, 2))               as written_premium,
        cast(sum_insured as decimal(18, 2))               as sum_insured,
        {{ clean_code('currency_cd') }}                   as currency,
        {{ clean_text('plate_no') }}                      as plate_no,
        {{ normalise_policy_status('policy_status') }}    as policy_status,
        cast({{ clean_text('last_updated_at') }} as timestamp) as source_updated_at,
        _source_file, _ingested_at, _record_hash, _batch_date
    from {{ source('bronze', 'partner_policies_a') }}
),

variant_b as (
    select
        {{ clean_code('PolicyNumber') }}                  as policy_id,
        {{ clean_text('IDCard') }}                        as national_id,
        {{ clean_code('ProductCode') }}                   as product_code,
        {{ be_to_ce_date('EffectiveDate') }}              as inception_date,
        {{ be_to_ce_date('ExpiryDate') }}                 as expiry_date,
        cast(GrossPremium as decimal(18, 2))              as written_premium,
        cast(SumInsured as decimal(18, 2))                as sum_insured,
        {{ clean_code('Curr') }}                          as currency,
        {{ clean_text('VehiclePlate') }}                  as plate_no,
        {{ normalise_policy_status('Status') }}           as policy_status,
        cast({{ clean_text('UpdatedTimestamp') }} as timestamp) as source_updated_at,
        _source_file, _ingested_at, _record_hash, _batch_date
    from {{ source('bronze', 'partner_policies_b') }}
),

variant_c as (
    select
        {{ clean_code('nomor_polis') }}                   as policy_id,
        {{ clean_text('no_ktp') }}                        as national_id,
        {{ clean_code('kode_produk') }}                   as product_code,
        {{ be_to_ce_date('tanggal_mulai') }}              as inception_date,
        {{ be_to_ce_date('tanggal_akhir') }}              as expiry_date,
        cast(premi as decimal(18, 2))                     as written_premium,
        cast(nilai_pertanggungan as decimal(18, 2))       as sum_insured,
        {{ clean_code('mata_uang') }}                     as currency,
        {{ clean_text('nomor_plat') }}                    as plate_no,
        {{ normalise_policy_status('status_polis') }}     as policy_status,
        cast({{ clean_text('waktu_pembaruan') }} as timestamp) as source_updated_at,
        _source_file, _ingested_at, _record_hash, _batch_date
    from {{ source('bronze', 'partner_policies_c') }}
),

unioned as (
    select * from variant_a
    union all select * from variant_b
    union all select * from variant_c
),

ranked as (
    select
        *,
        {{ partner_code_from_source_file() }} as partner_code,
        {{ dedupe_latest('policy_id', 'source_updated_at') }} as row_num
    from unioned
)

select
    policy_id,
    partner_code,
    national_id,
    product_code,
    inception_date,
    expiry_date,
    written_premium,
    sum_insured,
    currency,
    plate_no,
    policy_status,
    source_updated_at,
    _source_file  as source_file,
    _ingested_at  as ingested_at,
    _record_hash  as record_hash,
    _batch_date   as batch_date
from ranked
where row_num = 1
