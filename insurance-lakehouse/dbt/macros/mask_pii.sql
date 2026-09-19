{#
    Mask a PII column according to its declared type.

    The classification lives in dbt column `meta` (pii_type / sensitivity) and
    is mirrored in config/sources.yml for bronze. This macro is the single
    implementation both the gold models and the generated Unity Catalog column
    masks agree on, so local output and Databricks output mask identically.

    Strategies, and why each one:

      national_id  sha2(salt || value)  -- irreversible, but still JOINable,
                                           so entity resolution survives masking
      phone        first 3 + last 2     -- enough to confirm a number with a
                                           caller without disclosing it
      plate        province + last 2    -- keeps geographic analysis possible
      email        first char + domain  -- domain is useful for channel analysis
      dob          year only            -- age banding without identifying
      name         first char + '.'
      address      province token only
      chassis      last 4
      health       full redaction       -- sensitive personal data under PDPA;
                                           never exposed outside restricted models

    The salt comes from the PII_SALT environment variable. An unsalted hash of a
    13-digit national ID is trivially reversible by brute force -- the keyspace
    is small enough to enumerate -- so the salt is what makes this a real
    control rather than a gesture.
#}

{% macro mask_pii(col, pii_type) -%}
{%- set salt = var('pii_salt') -%}
{%- if pii_type == 'national_id' -%}
    case when {{ col }} is null then null
         else sha2(concat('{{ salt }}', trim(cast({{ col }} as string))), 256) end

{%- elif pii_type == 'phone' -%}
    case when {{ col }} is null then null
         when length(trim({{ col }})) < 6 then '***'
         else concat(substr(trim({{ col }}), 1, 3), '****',
                     substr(trim({{ col }}), -2, 2)) end

{%- elif pii_type == 'plate' -%}
    case when {{ col }} is null then null
         when length(trim({{ col }})) < 4 then '***'
         else concat('***', substr(trim({{ col }}), -2, 2)) end

{%- elif pii_type == 'email' -%}
    case when {{ col }} is null then null
         when {{ col }} not like '%@%' then '***'
         else concat(substr(trim({{ col }}), 1, 1), '***@',
                     split_part(trim({{ col }}), '@', 2)) end

{%- elif pii_type == 'dob' -%}
    case when {{ col }} is null then null
         else concat(cast(year(cast({{ col }} as date)) as string), '-01-01') end

{%- elif pii_type == 'name' -%}
    case when {{ col }} is null then null
         else concat(substr(trim({{ col }}), 1, 1), '.') end

{%- elif pii_type == 'address' -%}
    case when {{ col }} is null then null else '[redacted]' end

{%- elif pii_type == 'chassis' -%}
    case when {{ col }} is null then null
         when length(trim({{ col }})) < 4 then '***'
         else concat('***', substr(trim({{ col }}), -4, 4)) end

{%- elif pii_type in ('health', 'health_condition', 'bmi') -%}
    {#- Full redaction. Deliberately does not preserve nullability information,
        because "this customer declared a condition" is itself health data. -#}
    cast(null as string)

{%- else -%}
    {{ exceptions.raise_compiler_error(
        "mask_pii: unknown pii_type '" ~ pii_type ~ "'. Add a strategy in macros/mask_pii.sql.") }}
{%- endif -%}
{%- endmacro %}
