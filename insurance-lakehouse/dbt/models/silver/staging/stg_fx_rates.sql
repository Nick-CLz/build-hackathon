{{ config(materialized='view') }}

/*
    The FX spine that convert_currency() joins against.

    Grain: one row per (from_currency, to_currency, valid_from) -- a rate with
    an explicit validity window rather than a single date.

    WHY WINDOWS AND NOT AN EXACT-DATE JOIN.

    The first version joined on rate_date = transaction_date. That looks correct
    and fails in three ways, all silently:

      1. WEEKENDS AND HOLIDAYS. Real FX feeds publish on trading days only. A
         policy incepting on a Saturday finds no rate, and its converted premium
         comes out NULL -- roughly 2/7 of the book, for a reason that has
         nothing to do with the data being wrong.
      2. FUTURE DATES. A policy's inception date can sit past the last published
         rate. Here that is literal: the reference feed only emits rates up to
         the batch date, so policies incepting after it had no match.
      3. FEED GAPS. One missed publication nulls out a whole day.

    The correct semantics, and what every treasury system does, is
    last-observation-carried-forward: use the most recent rate on or before the
    transaction date. Each rate is therefore valid from its own date until the
    next one supersedes it, and the newest rate has an open-ended window.

    This still fails loudly for a transaction BEFORE the first rate ever
    published, which is a genuine gap worth surfacing rather than papering over.
*/

with deduped as (
    select *
    from (
        select
            cast(rate_date as date) as rate_date,
            {{ clean_code('from_currency') }} as from_currency,
            {{ clean_code('to_currency') }} as to_currency,
            cast(rate as decimal(18, 8)) as rate,
            _ingested_at,
            {{ dedupe_latest('cast(rate_date as date), from_currency, to_currency',
                             '_ingested_at') }} as row_num
        from {{ source('bronze', 'ref_fx_rates') }}
        where rate_date is not null and rate is not null
    )
    where row_num = 1
)

select
    rate_date,
    from_currency,
    to_currency,
    rate,
    rate_date as valid_from,
    -- Open-ended on the newest rate: it stays in force until superseded.
    lead(rate_date) over (
        partition by from_currency, to_currency order by rate_date
    ) as valid_to,
    _ingested_at as ingested_at
from deduped
