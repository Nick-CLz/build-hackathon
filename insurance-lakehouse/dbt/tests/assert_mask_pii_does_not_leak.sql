-- Regression test for mask_pii().
--
-- Asserts the properties that actually matter for a privacy control, rather
-- than exact output strings (which would just restate the implementation):
--
--   * the masked value never equals the raw value
--   * the raw value is not a substring of the masked value
--   * national IDs hash deterministically (so masked data still joins)
--   * health data is fully redacted, not partially
--   * NULL stays NULL, so masking cannot manufacture a value
--
-- Returns offending rows, so an empty result is a pass.

with raw as (
    select
        '1101700151361'            as national_id,
        '081-234-5678'             as phone,
        '1กข 1234'                 as plate,
        'somchai.5@example.invalid' as email,
        'hypertension'             as condition
),

masked as (
    select
        national_id,
        phone,
        plate,
        email,
        condition,
        {{ mask_pii('national_id', 'national_id') }} as m_nid,
        {{ mask_pii('national_id', 'national_id') }} as m_nid_again,
        {{ mask_pii('phone', 'phone') }}             as m_phone,
        {{ mask_pii('plate', 'plate') }}             as m_plate,
        {{ mask_pii('email', 'email') }}             as m_email,
        {{ mask_pii('condition', 'health') }}        as m_condition,
        {{ mask_pii('cast(null as string)', 'phone') }} as m_null
    from raw
),

violations as (
    select 'national_id is unmasked' as failure from masked where m_nid = national_id
    union all
    select 'national_id hash is not deterministic' from masked where m_nid <> m_nid_again
    union all
    select 'phone leaks the full number' from masked where m_phone like '%' || phone || '%'
    union all
    select 'plate leaks the full plate' from masked where m_plate like '%' || plate || '%'
    union all
    select 'email leaks the local part' from masked where m_email like '%somchai%'
    union all
    select 'health data is not fully redacted' from masked where m_condition is not null
    union all
    select 'masking manufactured a value from null' from masked where m_null is not null
)

select * from violations
