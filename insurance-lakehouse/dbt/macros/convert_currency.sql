{#
    Convert an amount into the reporting currency using the rate AS AT the
    transaction date, not today's rate.

    This matters more than it looks. Converting history at the current rate
    makes last quarter's loss ratio move every time the baht does, so a
    restated report never reconciles with the one issued at the time.

    Contract: the model must join the FX spine, which `fx_join()` emits. The
    join is explicit rather than hidden in a correlated subquery because Spark
    executes a correlated scalar subquery per row, which is ruinous on a fact
    table of any size.

    fx_rates carries an identity row (THB -> THB = 1.0) for every date, so
    domestic amounts flow through the same path as foreign ones and there is no
    special case to forget. A missing rate yields NULL rather than silently
    passing the unconverted amount through, so a gap in the FX feed shows up as
    a test failure instead of a quietly wrong number.
#}

{% macro convert_currency(amount_col, currency_col, date_col, target=none, fx_alias='fx') -%}
{%- set tgt = target or var('reporting_currency') -%}
round(
    cast({{ amount_col }} as decimal(20, 4))
    * case
        when {{ currency_col }} = '{{ tgt }}' then 1.0
        else {{ fx_alias }}.rate
      end
, 2)
{%- endmacro %}


{#  The join that convert_currency() depends on.

    Range join, not an equality join. Each rate is valid from its own date until
    the next supersedes it, so this resolves to the most recent rate ON OR
    BEFORE the transaction date -- last-observation-carried-forward, which is
    what every treasury system does.

    An equality join on rate_date looks correct and fails silently on weekends
    and holidays (FX feeds publish on trading days only), on any date past the
    last published rate, and on any gap in the feed. Each of those produces a
    NULL converted amount for a reason unrelated to the data being wrong.

    Left join: an amount with genuinely no applicable rate -- a transaction
    predating the first rate ever published -- must still survive into the model
    as a NULL converted value so a test can catch it, rather than being dropped
    and quietly shrinking the fact table. #}
{% macro fx_join(currency_col, date_col, target=none, fx_alias='fx', fx_relation=none) -%}
{%- set tgt = target or var('reporting_currency') -%}
left join {{ fx_relation or ref('stg_fx_rates') }} as {{ fx_alias }}
    on {{ fx_alias }}.from_currency = {{ currency_col }}
   and {{ fx_alias }}.to_currency = '{{ tgt }}'
   and {{ date_col }} >= {{ fx_alias }}.valid_from
   and ({{ date_col }} < {{ fx_alias }}.valid_to or {{ fx_alias }}.valid_to is null)
{%- endmacro %}
