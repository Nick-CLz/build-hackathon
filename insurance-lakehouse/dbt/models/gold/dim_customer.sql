{{ config(materialized='table') }}
/*  Grain: one row per customer_id.

    PII IS MASKED HERE. Silver keeps identifiable values so operational joins and
    right-to-erasure lookups work; gold is what analysts and BI tools read, so
    masking is the default at this boundary and access to silver is the control.

    national_id is salted-hashed rather than dropped: the hash is deterministic,
    so masked data still JOINs and entity resolution survives masking. That is
    the property that makes hashing preferable to redaction for a key. */
select
    c.customer_id,
    {{ mask_pii('c.first_name', 'name') }} as first_name_masked,
    {{ mask_pii('c.last_name', 'name') }} as last_name_masked,
    {{ mask_pii('c.national_id', 'national_id') }} as national_id_hash,
    {{ mask_pii('c.email', 'email') }} as email_masked,
    {{ mask_pii('c.phone', 'phone') }} as phone_masked,
    {{ mask_pii('c.address', 'address') }} as address_masked,
    {{ mask_pii('c.date_of_birth', 'dob') }} as birth_year,
    c.province,
    c.country,
    c.age_years,
    case
        when c.age_years < 25 then '18-24'
        when c.age_years < 35 then '25-34'
        when c.age_years < 45 then '35-44'
        when c.age_years < 55 then '45-54'
        when c.age_years < 65 then '55-64'
        else '65+'
    end as age_band,
    c.created_date
from {{ ref('stg_customers') }} as c
