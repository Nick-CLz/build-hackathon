# PINNED DELIBERATELY, and the constraint is Python, not Airflow.
#
# Astro Runtime 3.3 ships Python 3.14; PySpark 3.5.9 does not support it, so a
# Spark task in that image fails at import. Runtime 13.11 ships Python 3.12,
# which was verified here to run PySpark 3.5.9 + delta-spark 3.3.3 and write a
# Delta table successfully.
#
# The general rule worth remembering: when a pipeline image carries Spark, the
# Runtime version is chosen by the Python version Spark supports. Taking the
# newest Runtime is how you end up debugging an import error at 2am.
FROM quay.io/astronomer/astro-runtime:13.11.0

# Spark 3.5 requires Java 8/11/17. Astro Runtime ships no JDK, so PySpark in a
# task would fail with "Java gateway process exited" -- the same opaque error
# the main docker-compose.yml hit when JAVA_HOME pointed at the wrong path.
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# The repo is bind-mounted here by docker-compose.override.yml.
ENV LAKEHOUSE_REPO=/usr/local/airflow/lakehouse_repo
ENV SPARK_CONF_DIR=${LAKEHOUSE_REPO}/conf

USER astro
