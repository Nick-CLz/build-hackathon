{{ config(materialized='view') }}
/*  Grain: one row per declaration_id.

    SENSITIVE PERSONAL DATA (PDPA s.26). Values are kept intact here because
    silver is the restricted layer; redaction happens at the gold boundary,
    which is what analysts and BI read. See docs/PDPA_AND_GOVERNANCE.md. */
with ranked as (
    select
        {{ clean_code('declaration_id') }} as declaration_id,
        {{ clean_code('policy_id') }} as policy_id,
        {{ clean_code('customer_id') }} as customer_id,
        cast(declared_at as date) as declared_date,
        {{ clean_text('pre_existing_conditions') }} as pre_existing_conditions,
        {{ clean_text('bmi_band') }} as bmi_band,
        _ingested_at,
        _record_hash,
        _batch_date,
        {{ dedupe_latest('declaration_id', 'cast(declared_at as timestamp)') }} as row_num
    from {{ source('bronze', 'events_health_declarations') }}
    where declaration_id is not null
)

select
    declaration_id,
    policy_id,
    customer_id,
    declared_date,
    pre_existing_conditions,
    bmi_band,
    pre_existing_conditions != 'none' as has_declared_condition,
    _ingested_at as ingested_at,
    _batch_date as batch_date
from ranked
where row_num = 1
