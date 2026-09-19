{{ config(materialized='table') }}
/*  Grain: one row per policy_id with a health declaration.

    Every sensitive column is FULLY REDACTED here, not partially masked.
    mask_pii(col, 'health') returns NULL deliberately: a partial mask would
    still reveal that a condition was declared, and "this customer declared a
    condition" is itself health information under PDPA s.26.

    What remains is the non-sensitive shape of the book -- how many health
    policies exist and when they were underwritten -- which is enough for
    portfolio analytics without exposing anybody's medical history. Unmasked
    access is granted to the restricted_health group via a Unity Catalog
    column mask, never by querying a different table. */
select
    h.policy_id,
    h.customer_id,
    h.declared_date,
    {{ mask_pii('h.pre_existing_conditions', 'health') }} as pre_existing_conditions,
    {{ mask_pii('h.bmi_band', 'health') }} as bmi_band,
    cast(null as boolean) as has_declared_condition
from {{ ref('stg_health_declarations') }} as h
