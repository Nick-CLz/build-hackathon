{{ config(materialized='view') }}
/*  Grain: one row per vehicle_id. */
with ranked as (
    select
        {{ clean_code('vehicle_id') }}              as vehicle_id,
        {{ clean_code('customer_id') }}             as customer_id,
        {{ clean_text('plate_no') }}                as plate_no,
        {{ clean_text('plate_province') }}          as plate_province,
        {{ clean_text('make') }}                    as make,
        {{ clean_text('model') }}                   as model,
        cast(manufacture_year as int)               as manufacture_year,
        cast(engine_cc as int)                      as engine_cc,
        {{ clean_text('chassis_no') }}              as chassis_no,
        cast(vehicle_value_local as decimal(18, 2)) as vehicle_value_local,
        {{ clean_code('country') }}                 as country,
        _ingested_at, _record_hash, _batch_date,
        {{ dedupe_latest('vehicle_id', '_ingested_at') }} as row_num
    from {{ source('bronze', 'ref_vehicles') }}
    where vehicle_id is not null
)
select
    vehicle_id, customer_id, plate_no, plate_province, make, model,
    manufacture_year, engine_cc, chassis_no, vehicle_value_local, country,
    year(current_date()) - manufacture_year as vehicle_age_years,
    _ingested_at as ingested_at
from ranked
where row_num = 1
