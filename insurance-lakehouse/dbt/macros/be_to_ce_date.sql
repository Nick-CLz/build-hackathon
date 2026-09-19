{#
    Parse a partner date string into a Common Era date.

    Thailand uses the Buddhist Era: BE = CE + 543, so CE 2026 is BE 2569. Thai
    partner systems emit BE years inside otherwise ordinary date formats. A BE
    year parsed as CE lands 543 years in the future and silently poisons every
    date comparison downstream -- no error, just wrong numbers.

    Conversion is by DETECTION (year > 2400), not by partner, because partners
    are inconsistent even within one file: BKKSURE emits all of `2568-08-25`,
    `25/08/2568` and `25 ส.ค. 2568`, plus the occasional Common Era year.

    Surface forms handled:
        YYYY-MM-DD       2026-01-05   /  2569-01-05
        DD/MM/YYYY       05/01/2026   /  05/01/2569
        D Mon YYYY       5 Jan 2026   (English abbreviation)
        D <thai> YYYY    5 ม.ค. 2569  (Thai abbreviation)

    Anything else yields NULL, which staging surfaces as a not_null test failure
    rather than a silent epoch or a wrong date.

    Structure note: the year, month and day are each coalesced across the
    patterns FIRST, then a single date is constructed. The obvious alternative
    -- a CASE arm per format, each building its own date -- compiled to ~3.7KB
    per call. With twelve calls in stg_partner_policies that produced 44KB of
    SQL, which overflows the 32,700-character limit Derby imposes on a view's
    stored text. This shape is about four times smaller and easier to read.
#}

{% macro be_to_ce_date(col) -%}
{%- set s = "trim(cast(" ~ col ~ " as string))" -%}
{%- set iso = "'^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$'" -%}
{%- set dmy = "'^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'" -%}
{%- set dmon = "'^[0-9]{1,2} [^ ]+ [0-9]{4}$'" -%}
{{ be_make_date(
    "coalesce("
      ~ "cast(nullif(regexp_extract(" ~ s ~ ", '^([0-9]{4})-[0-9]{1,2}-[0-9]{1,2}$', 1), '') as int),"
      ~ "cast(nullif(regexp_extract(" ~ s ~ ", '^[0-9]{1,2}/[0-9]{1,2}/([0-9]{4})$', 1), '') as int),"
      ~ "cast(nullif(regexp_extract(" ~ s ~ ", '^[0-9]{1,2} [^ ]+ ([0-9]{4})$', 1), '') as int))",
    "coalesce("
      ~ "cast(nullif(regexp_extract(" ~ s ~ ", '^[0-9]{4}-([0-9]{1,2})-[0-9]{1,2}$', 1), '') as int),"
      ~ "cast(nullif(regexp_extract(" ~ s ~ ", '^[0-9]{1,2}/([0-9]{1,2})/[0-9]{4}$', 1), '') as int),"
      ~ be_month_from_name("regexp_extract(" ~ s ~ ", '^[0-9]{1,2} ([^ ]+) [0-9]{4}$', 1)") ~ ")",
    "coalesce("
      ~ "cast(nullif(regexp_extract(" ~ s ~ ", '^[0-9]{4}-[0-9]{1,2}-([0-9]{1,2})$', 1), '') as int),"
      ~ "cast(nullif(regexp_extract(" ~ s ~ ", '^([0-9]{1,2})/[0-9]{1,2}/[0-9]{4}$', 1), '') as int),"
      ~ "cast(nullif(regexp_extract(" ~ s ~ ", '^([0-9]{1,2}) [^ ]+ [0-9]{4}$', 1), '') as int))"
) }}
{%- endmacro %}


{#  Assemble a date from year/month/day expressions, converting a Buddhist Era
    year to Common Era.

    Uses `to_date`, not `try_to_date`: the latter exists on Databricks but NOT
    in OSS Spark 3.5, and a macro that only works on one of the two targets
    defeats the point of having two. The range guard does the job `try_` would
    have -- an impossible month or day yields NULL instead of raising. #}
{% macro be_make_date(year_expr, month_expr, day_expr) -%}
(
    case when ({{ month_expr }}) between 1 and 12
          and ({{ day_expr }}) between 1 and 31
    then to_date(
        concat_ws('-',
            lpad(cast(case when ({{ year_expr }}) > 2400
                           then ({{ year_expr }}) - {{ var('be_offset', 543) }}
                           else ({{ year_expr }}) end as string), 4, '0'),
            lpad(cast(({{ month_expr }}) as string), 2, '0'),
            lpad(cast(({{ day_expr }}) as string), 2, '0')),
        'yyyy-MM-dd')
    end
)
{%- endmacro %}


{#  Month number from an English or Thai abbreviation. #}
{% macro be_month_from_name(name_expr) -%}
(case lower(trim({{ name_expr }}))
 when 'jan' then 1 when 'feb' then 2 when 'mar' then 3 when 'apr' then 4
 when 'may' then 5 when 'jun' then 6 when 'jul' then 7 when 'aug' then 8
 when 'sep' then 9 when 'oct' then 10 when 'nov' then 11 when 'dec' then 12
 when 'ม.ค.' then 1 when 'ก.พ.' then 2 when 'มี.ค.' then 3 when 'เม.ย.' then 4
 when 'พ.ค.' then 5 when 'มิ.ย.' then 6 when 'ก.ค.' then 7 when 'ส.ค.' then 8
 when 'ก.ย.' then 9 when 'ต.ค.' then 10 when 'พ.ย.' then 11 when 'ธ.ค.' then 12
 end)
{%- endmacro %}
