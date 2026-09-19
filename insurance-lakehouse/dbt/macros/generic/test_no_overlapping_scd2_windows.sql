{% test no_overlapping_scd2_windows(model, key_column, valid_from_column, valid_to_column) %}
{#
    SCD2 validity windows for one key must not overlap.

    Overlap means a point-in-time query returns TWO rows for one entity, which
    silently double-counts exposure and premium in any as-of-date aggregate.
    Nothing errors; the numbers are just quietly doubled for the affected keys,
    which is close to the worst possible failure mode.

    Windows are treated as half-open [valid_from, valid_to), matching how
    silver_policy_as_of is queried. An open window (valid_to IS NULL) is the
    current version and extends to the far future.

    Also catches inverted windows, where valid_to precedes valid_from.

    Returns offending rows, so an empty result is a pass.
#}

with windows as (
    select
        {{ key_column }} as entity_key,
        cast({{ valid_from_column }} as timestamp) as valid_from,
        coalesce(cast({{ valid_to_column }} as timestamp),
                 cast('9999-12-31' as timestamp)) as valid_to
    from {{ model }}
),

adjacent as (
    select
        entity_key,
        valid_from,
        valid_to,
        lead(valid_from) over (
            partition by entity_key order by valid_from
        ) as next_valid_from
    from windows
)

select entity_key, valid_from, valid_to, next_valid_from, 'overlap' as failure_reason
from adjacent
where next_valid_from is not null and next_valid_from < valid_to

union all

select entity_key, valid_from, valid_to, null, 'inverted_window'
from adjacent
where valid_to < valid_from

{% endtest %}
