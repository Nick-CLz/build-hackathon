{{ config(materialized='table') }}

/*
    Quote-to-policy conversion. Grain: one row per
    (quote_year_month, country, partner_code, product_code, channel).

    Attributed to the QUOTE's month, not the policy's. A quote issued in January
    that converts in February is January's conversion -- attributing it to
    February would credit the wrong period's marketing spend and make the funnel
    un-analysable.

    Average premium is computed over converted quotes only, so it answers "what
    does a sale look like" rather than being diluted by quotes that never became
    anything.
*/

select
    date_format(q.quoted_date, 'yyyy-MM') as quote_year_month,
    date_trunc('month', q.quoted_date) as month_start,
    q.country,
    q.partner_code,
    q.product_code,
    pr.product_name_en,
    pr.cover_class,
    q.channel,
    count(*) as quotes_issued,
    sum(case when q.did_convert then 1 else 0 end) as quotes_converted,
    round(
        sum(case when q.did_convert then 1 else 0 end) / count(*),
        4
    ) as conversion_rate,
    round(avg(q.quoted_premium), 2) as avg_quoted_premium,
    round(avg(case when q.did_convert then q.quoted_premium end), 2)
        as avg_converted_premium,
    sum(case when q.quote_status = 'REJECTED' then 1 else 0 end) as quotes_rejected,
    sum(case when q.quote_status = 'EXPIRED' then 1 else 0 end) as quotes_expired
from {{ ref('stg_quotes') }} as q
left join {{ ref('dim_product') }} as pr on q.product_code = pr.product_code
where q.quoted_date is not null
group by 1, 2, 3, 4, 5, 6, 7, 8
