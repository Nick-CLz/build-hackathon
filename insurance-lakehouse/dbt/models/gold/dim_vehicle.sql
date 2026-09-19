{{ config(materialized='table') }}
/*  Grain: one row per vehicle_id. Plate and chassis are partially masked -- a
    plate is personally identifying in practice, but the province is retained
    because geographic exposure analysis depends on it. */
select
    v.vehicle_id,
    v.customer_id,
    {{ mask_pii('v.plate_no', 'plate') }} as plate_masked,
    v.plate_province,
    {{ mask_pii('v.chassis_no', 'chassis') }} as chassis_masked,
    v.make,
    v.model,
    concat(v.make, ' ', v.model) as make_model,
    v.manufacture_year,
    v.vehicle_age_years,
    case
        when v.vehicle_age_years <= 3 then '0-3'
        when v.vehicle_age_years <= 7 then '4-7'
        when v.vehicle_age_years <= 12 then '8-12'
        else '13+'
    end as vehicle_age_band,
    v.engine_cc,
    case
        when v.engine_cc <= 1500 then 'small'
        when v.engine_cc <= 2000 then 'medium'
        else 'large'
    end as engine_class,
    v.vehicle_value_local,
    v.country
from {{ ref('stg_vehicles') }} as v
