{#
    Parse a partner date string into a CE date, handling Buddhist Era years.

    Thailand uses the Buddhist Era: BE = CE + 543, so CE 2026 is BE 2569. Thai
    partner systems emit BE years inside otherwise ordinary date formats, and a
    BE year parsed as CE lands ~543 years in the future, silently poisoning
    every date comparison downstream.

    Conversion is by DETECTION (year > 2400), not by partner, because partners
    are inconsistent even within a single file -- BKKSURE emits all three of
    `2568-08-25`, `25/08/2568` and `25 ส.ค. 2568`, and occasionally a CE year.

    Supported surface forms:
      YYYY-MM-DD      2026-01-05   or BE 2569-01-05
      DD/MM/YYYY      05/01/2026   or BE 05/01/2569
      D Mon YYYY      5 Jan 2026   (English abbreviation)
      D <thai> YYYY   5 ม.ค. 2569  (Thai abbreviation)

    Returns NULL for anything unrecognised, which the staging models surface as
    a not_null test failure rather than a silent zero date.
#}

{% macro be_to_ce_date(col) -%}
{%- set c = col -%}
(
    case
        -- ISO: YYYY-MM-DD
        when {{ c }} rlike '^\\s*[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}\\s*$'
            then {{ dbt_lakehouse_make_date(
                    "cast(regexp_extract(trim(" ~ c ~ "), '^([0-9]{4})-', 1) as int)",
                    "cast(regexp_extract(trim(" ~ c ~ "), '^[0-9]{4}-([0-9]{1,2})-', 1) as int)",
                    "cast(regexp_extract(trim(" ~ c ~ "), '-([0-9]{1,2})$', 1) as int)") }}

        -- DD/MM/YYYY
        when {{ c }} rlike '^\\s*[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}\\s*$'
            then {{ dbt_lakehouse_make_date(
                    "cast(regexp_extract(trim(" ~ c ~ "), '/([0-9]{4})$', 1) as int)",
                    "cast(regexp_extract(trim(" ~ c ~ "), '^[0-9]{1,2}/([0-9]{1,2})/', 1) as int)",
                    "cast(regexp_extract(trim(" ~ c ~ "), '^([0-9]{1,2})/', 1) as int)") }}

        -- D Mon YYYY (English)
        when {{ c }} rlike '^\\s*[0-9]{1,2} [A-Za-z]{3} [0-9]{4}\\s*$'
            then {{ dbt_lakehouse_make_date(
                    "cast(regexp_extract(trim(" ~ c ~ "), ' ([0-9]{4})$', 1) as int)",
                    dbt_lakehouse_month_from_name(
                        "regexp_extract(trim(" ~ c ~ "), '^[0-9]{1,2} ([A-Za-z]{3}) ', 1)"),
                    "cast(regexp_extract(trim(" ~ c ~ "), '^([0-9]{1,2}) ', 1) as int)") }}

        -- D <thai month> YYYY
        when {{ c }} rlike '^\\s*[0-9]{1,2} [^ ]+ [0-9]{4}\\s*$'
            then {{ dbt_lakehouse_make_date(
                    "cast(regexp_extract(trim(" ~ c ~ "), ' ([0-9]{4})$', 1) as int)",
                    dbt_lakehouse_month_from_name(
                        "regexp_extract(trim(" ~ c ~ "), '^[0-9]{1,2} ([^ ]+) ', 1)"),
                    "cast(regexp_extract(trim(" ~ c ~ "), '^([0-9]{1,2}) ', 1) as int)") }}

        else null
    end
)
{%- endmacro %}


{#  Build a date from y/m/d expressions, converting a Buddhist Era year to CE.

    Uses `to_date`, not `try_to_date`: the latter exists on Databricks but NOT
    in OSS Spark 3.5, and a macro that only runs on one of the two targets
    defeats the point of having two targets. The month/day range guard does the
    job `try_` would have done -- a mangled row yields NULL instead of raising. #}
{% macro dbt_lakehouse_make_date(year_expr, month_expr, day_expr) -%}
(
    case
        when ({{ month_expr }}) between 1 and 12 and ({{ day_expr }}) between 1 and 31
        then to_date(
            lpad(cast(
                case when ({{ year_expr }}) > 2400
                     then ({{ year_expr }}) - {{ var('be_offset', 543) }}
                     else ({{ year_expr }})
                end as string), 4, '0')
            || '-' || lpad(cast(({{ month_expr }}) as string), 2, '0')
            || '-' || lpad(cast(({{ day_expr }}) as string), 2, '0'),
            'yyyy-MM-dd')
    end
)
{%- endmacro %}


{#  Month number from an English or Thai abbreviation. #}
{% macro dbt_lakehouse_month_from_name(name_expr) -%}
(
    case lower(trim({{ name_expr }}))
        when 'jan' then 1 when 'feb' then 2  when 'mar' then 3
        when 'apr' then 4 when 'may' then 5  when 'jun' then 6
        when 'jul' then 7 when 'aug' then 8  when 'sep' then 9
        when 'oct' then 10 when 'nov' then 11 when 'dec' then 12
        when 'ม.ค.' then 1  when 'ก.พ.' then 2  when 'มี.ค.' then 3
        when 'เม.ย.' then 4  when 'พ.ค.' then 5  when 'มิ.ย.' then 6
        when 'ก.ค.' then 7  when 'ส.ค.' then 8  when 'ก.ย.' then 9
        when 'ต.ค.' then 10 when 'พ.ย.' then 11 when 'ธ.ค.' then 12
    end
)
{%- endmacro %}
