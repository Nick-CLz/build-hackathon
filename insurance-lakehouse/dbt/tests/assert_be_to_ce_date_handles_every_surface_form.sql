-- Regression test for be_to_ce_date().
--
-- Every row below is a real surface form seen in the partner drops. All of them
-- denote 2026-01-05; the macro must return exactly that, and must return NULL
-- (not a wrong date) for input it cannot parse.
--
-- This is a singular test rather than a scratch model so that `dbt build` keeps
-- it honest. Buddhist Era handling is the single highest-consequence transform
-- in the project: a BE year parsed as CE lands 543 years in the future and
-- corrupts every date comparison downstream without raising anything.
--
-- Returns offending rows, so an empty result is a pass.

with cases as (
    select
        '2026-01-05' as raw_date,
        date '2026-01-05' as expected,
        'iso, common era' as description
    union all
    select
        '2569-01-05' as raw_date,
        date '2026-01-05' as expected,
        'iso, buddhist era' as description
    union all
    select
        '05/01/2026' as raw_date,
        date '2026-01-05' as expected,
        'dd/mm/yyyy, common era' as description
    union all
    select
        '05/01/2569' as raw_date,
        date '2026-01-05' as expected,
        'dd/mm/yyyy, buddhist era' as description
    union all
    select
        '5 Jan 2026' as raw_date,
        date '2026-01-05' as expected,
        'english month abbreviation' as description
    union all
    select
        '5 ม.ค. 2569' as raw_date,
        date '2026-01-05' as expected,
        'thai month abbreviation, BE' as description
    union all
    select
        '  2569-01-05 ' as raw_date,
        date '2026-01-05' as expected,
        'buddhist era with whitespace' as description
    union all
    select
        'garbage' as raw_date,
        cast(null as date) as expected,
        'unparseable yields null' as description
    union all
    select
        '' as raw_date,
        cast(null as date) as expected,
        'empty yields null' as description
    union all
    select
        '2026-13-45' as raw_date,
        cast(null as date) as expected,
        'impossible month/day yields null' as description
    union all
    select
        cast(null as string) as raw_date,
        cast(null as date) as expected,
        'null input yields null' as description
)

select
    description,
    raw_date,
    expected,
    {{ be_to_ce_date('raw_date') }} as actual
from cases
where not ({{ be_to_ce_date('raw_date') }} <=> expected)
