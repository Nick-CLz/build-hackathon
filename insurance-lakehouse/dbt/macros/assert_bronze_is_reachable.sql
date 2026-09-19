{#
    Fail fast, with an explanation, when dbt cannot see the bronze schema.

    Why this exists: conf/spark-defaults.conf sets the Spark warehouse and Derby
    metastore paths RELATIVELY, because spark-defaults.conf cannot expand
    environment variables. Two things therefore break dbt in ways whose native
    error messages point nowhere useful:

      1. Running dbt from inside dbt/ resolves those relative paths against the
         wrong directory and silently creates a SECOND, empty metastore at
         dbt/lakehouse/. Every model then fails with an opaque
         `UnresolvedRelation [bronze, ...]` and a hundred lines of logical plan.

      2. Running dbt without SPARK_CONF_DIR set means Delta is never configured
         and Spark uses a default warehouse, so the schema simply is not there.

    Uses `show databases` rather than `show tables in bronze` deliberately: the
    latter RAISES when the schema is absent, which is the very case this guard
    exists to explain, and the raw exception would pre-empt the message below.
#}
{% macro assert_bronze_is_reachable() %}
    {% if execute and target.type == 'spark' %}
        {% set bronze = var('bronze_schema', 'bronze') %}
        {% set databases = run_query('show databases') %}
        {% set names = databases.columns[0].values() | map('lower') | list %}

        {% if bronze | lower not in names %}
            {{ exceptions.raise_compiler_error(
                "dbt cannot see the '" ~ bronze ~ "' schema, so no model can build.\n"
                ~ "Spark reports these schemas instead: " ~ (names | join(', ')) ~ "\n\n"
                ~ "Most likely one of:\n"
                ~ "  1. dbt was run from the wrong directory. The Spark warehouse and\n"
                ~ "     metastore paths in conf/spark-defaults.conf are RELATIVE, so running\n"
                ~ "     dbt from inside dbt/ creates a second, empty metastore.\n"
                ~ "  2. SPARK_CONF_DIR is not set, so Delta was never configured.\n"
                ~ "  3. Bronze has genuinely not been built yet.\n\n"
                ~ "Use the Makefile, which handles 1 and 2:\n"
                ~ "    make dbt-build       (or make data && make bronze first)") }}
        {% endif %}
    {% endif %}
{% endmacro %}
