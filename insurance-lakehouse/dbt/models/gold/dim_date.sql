{{ config(materialized='table') }}
/*  Grain: one row per calendar date.

    Built from the actual span of the data plus headroom, rather than a fixed
    range, so it cannot silently fail to cover a backfill. Includes the Buddhist
    Era year, because Thai-facing reports are expected to show it. */
with bounds as (
    select
        date_sub(least(
            (select coalesce(min(inception_date), current_date()) from {{ ref('silver_policies') }}),
            (select coalesce(min(loss_date), current_date()) from {{ ref('silver_claims') }})
        ), 30) as start_date,
        date_add(greatest(
            (select coalesce(max(expiry_date), current_date()) from {{ ref('silver_policies') }}),
            current_date()
        ), 400) as end_date
),

spine as (
    select explode(sequence(start_date, end_date, interval 1 day)) as date_day
    from bounds
)

select
    date_day,
    cast(date_format(date_day, 'yyyyMMdd') as int) as date_key,
    year(date_day) as calendar_year,
    year(date_day) + 543 as buddhist_year,
    quarter(date_day) as calendar_quarter,
    month(date_day) as calendar_month,
    date_format(date_day, 'MMM') as month_name,
    date_trunc('month', date_day) as month_start,
    last_day(date_day) as month_end,
    date_format(date_day, 'yyyy-MM') as year_month,
    dayofweek(date_day) as day_of_week,
    dayofweek(date_day) in (1, 7) as is_weekend,
    date_day = last_day(date_day) as is_month_end
from spine
