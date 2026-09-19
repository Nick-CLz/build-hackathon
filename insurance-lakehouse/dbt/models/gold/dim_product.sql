{{ config(materialized='table') }}
/*  Grain: one row per product_code.

    cover_class matters more than it looks: comparing loss ratios across Thai
    voluntary motor classes without it is meaningless, because class 1 carries
    own-damage cover and class 3 does not. Any product-level aggregate that
    ignores class will show class 1 as "worse" purely by construction. */
select
    p.product_code,
    p.product_name_en,
    p.product_name_th,
    p.line_of_business,
    p.cover_type,
    p.cover_class,
    p.is_compulsory,
    case
        when p.product_code = 'MOTOR_CMI'
            then 'Thai compulsory motor (por ror bor): bodily injury only, statutory caps'
        when p.product_code = 'MOTOR_VOL_C1'
            then 'Class 1: own damage, theft and fire, third party'
        when p.product_code = 'MOTOR_VOL_C2'
            then 'Class 2: theft and fire, third party; no own damage'
        when p.product_code = 'MOTOR_VOL_C3'
            then 'Class 3: third party only'
        when p.product_code = 'HEALTH_ADJ'
            then 'Adjustable health: sum insured re-rated between periods'
    end as cover_description,
    p.base_premium_local,
    p.default_sum_insured
from {{ ref('stg_products') }} p
