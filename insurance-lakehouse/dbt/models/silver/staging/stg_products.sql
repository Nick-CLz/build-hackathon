{{ config(materialized='view') }}
/*  Grain: one row per product_code. Static dimension.
    cover_class exposes the Thai voluntary motor class (1/2/3) as its own
    attribute, because comparing loss ratios across classes without it is
    meaningless -- class 1 carries own-damage cover and class 3 does not. */
with ranked as (
    select
        {{ clean_code('product_code') }}            as product_code,
        {{ clean_text('product_name_en') }}         as product_name_en,
        {{ clean_text('product_name_th') }}         as product_name_th,
        lower({{ clean_text('line_of_business') }}) as line_of_business,
        lower({{ clean_text('cover_type') }})       as cover_type,
        cast(base_premium_local as decimal(18, 2))  as base_premium_local,
        cast(default_sum_insured as decimal(18, 2)) as default_sum_insured,
        _ingested_at, _record_hash,
        {{ dedupe_latest('product_code', '_ingested_at') }} as row_num
    from {{ source('bronze', 'ref_products') }}
    where product_code is not null
)
select
    product_code, product_name_en, product_name_th,
    line_of_business, cover_type,
    case
        when product_code = 'MOTOR_CMI' then 'compulsory'
        when product_code = 'MOTOR_VOL_C1' then 'class_1'
        when product_code = 'MOTOR_VOL_C2' then 'class_2'
        when product_code = 'MOTOR_VOL_C3' then 'class_3'
        when product_code = 'HEALTH_ADJ' then 'health'
    end as cover_class,
    product_code = 'MOTOR_CMI' as is_compulsory,
    base_premium_local, default_sum_insured,
    _ingested_at as ingested_at
from ranked
where row_num = 1
