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
    select * from values
        ('2026-01-05',    date '2026-01-05', 'iso, common era'),
        ('2569-01-05',    date '2026-01-05', 'iso, buddhist era'),
        ('05/01/2026',    date '2026-01-05', 'dd/mm/yyyy, common era'),
        ('05/01/2569',    date '2026-01-05', 'dd/mm/yyyy, buddhist era'),
        ('5 Jan 2026',    date '2026-01-05', 'english month abbreviation'),
        ('5 ม.ค. 2569',   date '2026-01-05', 'thai month abbreviation, BE'),
        ('  2569-01-05 ', date '2026-01-05', 'buddhist era with whitespace'),
        ('garbage',       cast(null as date), 'unparseable yields null'),
        ('',              cast(null as date), 'empty yields null'),
        ('2026-13-45',    cast(null as date), 'impossible month/day yields null'),
        (cast(null as string), cast(null as date), 'null input yields null')
    as t(raw_date, expected, description)
)

select
    description,
    raw_date,
    expected,
    {{ be_to_ce_date('raw_date') }} as actual
from cases
where not ({{ be_to_ce_date('raw_date') }} <=> expected)
