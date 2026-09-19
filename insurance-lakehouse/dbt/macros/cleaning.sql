{#
    Text-cleaning helpers shared by every staging model.

    These exist as macros rather than repeated SQL because the partner feeds
    disagree in the same ways over and over, and a fix applied in eleven places
    is a fix applied in ten.
#}

{#  Trim, collapse internal whitespace, and null out empty strings.

    Empty-string-vs-NULL is the quiet killer here: a partner sending '' rather
    than NULL passes a not_null test while breaking every join. #}
{% macro clean_text(col) -%}
nullif(regexp_replace(trim(cast({{ col }} as string)), '\\s+', ' '), '')
{%- endmacro %}


{#  Clean, then uppercase: for codes and keys, where 'active' and 'ACTIVE ' are
    the same value wearing different clothes. #}
{% macro clean_code(col) -%}
upper({{ clean_text(col) }})
{%- endmacro %}


{#  Normalise a policy status to a canonical English code.

    Partner B emits Thai statuses in about half its rows, mixed with English in
    the other half -- inside the same column of the same file. Mapping by value
    rather than by partner is the only thing that survives that. #}
{% macro normalise_policy_status(col) -%}
(
    case {{ clean_code(col) }}
        when 'ACTIVE' then 'ACTIVE'
        when 'CANCELLED' then 'CANCELLED'
        when 'CANCELED' then 'CANCELLED'
        when 'EXPIRED' then 'EXPIRED'
        when 'มีผลบังคับ' then 'ACTIVE'
        when 'ยกเลิก' then 'CANCELLED'
        when 'หมดอายุ' then 'EXPIRED'
        else 'UNKNOWN'
    end
)
{%- endmacro %}


{#  The partner code is not a column in the partner files -- it is implied by
    the directory the file landed in. Recovering it from _source_file is what
    lets three differently-shaped feeds become one table. #}
{% macro partner_code_from_source_file(col='_source_file') -%}
upper(regexp_extract({{ col }}, 'partner_drops/([^/]+)/', 1))
{%- endmacro %}


{#  Deduplicate to one row per key.

    Tie-break order, and why each step is needed:
      1. the business timestamp  -- the partner's own view of recency
      2. _ingested_at            -- when two records claim the same timestamp,
                                    the batch that arrived later wins
      3. _record_hash            -- a deterministic final tie-break, so the
                                    result is stable across re-runs rather than
                                    depending on Spark's row ordering

    Without step 3 a re-run can legitimately produce a different winner between
    two genuinely identical records, which makes downstream diffs meaningless
    and snapshot history churn for no reason.
#}
{% macro dedupe_latest(partition_by, order_by, relation_alias='') -%}
row_number() over (
    partition by {{ partition_by }}
    order by {{ order_by }} desc nulls last,
             {{ relation_alias }}_ingested_at desc nulls last,
             {{ relation_alias }}_record_hash asc
)
{%- endmacro %}
