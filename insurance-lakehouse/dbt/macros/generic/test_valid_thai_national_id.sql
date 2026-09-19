{% test valid_thai_national_id(model, column_name) %}
{#
    Thai national IDs carry a mod-11 check digit. This test implements the real
    algorithm rather than a length check, so a transposed or mistyped digit is
    caught rather than passing as "13 characters, looks fine".

    Algorithm: multiply digit i (0-indexed, first 12) by (13 - i), sum, take
    mod 11, subtract from 11, take mod 10. That must equal the 13th digit.

    The same algorithm lives in data_generator/thai.py so the generator can
    produce a controlled proportion of invalid IDs (~4%) for this test to find.
    Two implementations of one rule is a deliberate cross-check: if they ever
    disagree, one of them is wrong and the suite says so.

    Returns offending rows, so an empty result is a pass.
#}

with candidates as (
    select
        {{ column_name }} as national_id,
        regexp_replace(trim(cast({{ column_name }} as string)), '[^0-9]', '') as digits
    from {{ model }}
    where {{ column_name }} is not null
),

checked as (
    select
        national_id,
        digits,
        (
            11 - (
{%- for i in range(12) %}
                cast(substr(digits, {{ i + 1 }}, 1) as int) * {{ 13 - i }}
                {%- if not loop.last %} + {% endif %}
                {%- endfor %}
            ) % 11
        ) % 10 as expected_check_digit,
        cast(substr(digits, 13, 1) as int) as actual_check_digit
    from candidates
    where length(digits) = 13
)

select national_id, digits, expected_check_digit, actual_check_digit
from checked
where expected_check_digit <> actual_check_digit

{% endtest %}
