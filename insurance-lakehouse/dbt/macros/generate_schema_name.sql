{#
    Use the configured schema name verbatim instead of dbt's default
    `<target_schema>_<custom_schema>` concatenation.

    The default exists to keep developers from colliding in a shared warehouse.
    Here the layers ARE the schemas (silver, gold, marts), and a local run has
    no other tenants, so the concatenation only produces names like
    `silver_gold`. On Databricks the isolation comes from the catalog
    (catalog-per-environment), not from a schema prefix.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
{%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
{{ custom_schema_name | trim }}
{%- endif -%}
{%- endmacro %}
