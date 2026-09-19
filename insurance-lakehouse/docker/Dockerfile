# Spark 3.5 + Delta 3.3 + dbt-spark (session), pinned to match pyproject.toml.
#
# Base choice: python:3.11-slim-bookworm, because Debian bookworm ships
# OpenJDK 17 in its own archive. Spark 3.5 supports Java 8/11/17 only, and
# Python 3.11 is the newest PySpark 3.5 is fully tested against -- bookworm
# satisfies both from one trusted source, with no third-party PPA.
FROM python:3.11-slim-bookworm

# Optional extra CA bundle, for networks that terminate TLS (corporate proxies,
# sandboxed CI). Default is empty and changes nothing. Build with:
#   docker compose build --build-arg EXTRA_CA_BUNDLE=ca-bundle.crt
# where the file sits in the build context.
ARG EXTRA_CA_BUNDLE=""

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_CONF_DIR=/workspace/conf \
    LAKEHOUSE_IN_DOCKER=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      openjdk-17-jdk-headless \
      build-essential git make procps ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && "$JAVA_HOME/bin/java" -version

# Trust the extra CA, if one was supplied. This has to be done in two places:
# the system store (for apt, curl and Python) and the JDK's own truststore (for
# Spark resolving Delta JARs from Maven Central). Java does not read the system
# CA bundle, so skipping the keytool step leaves JAR resolution failing with an
# opaque SSL error long after pip has started working.
COPY docker/ca/ /tmp/extra-ca/
RUN if [ -n "$EXTRA_CA_BUNDLE" ] && [ -f "/tmp/extra-ca/$EXTRA_CA_BUNDLE" ]; then \
      cp "/tmp/extra-ca/$EXTRA_CA_BUNDLE" /usr/local/share/ca-certificates/extra.crt \
      && update-ca-certificates \
      && csplit -z -f /tmp/cacert- -b '%02d.pem' \
           /usr/local/share/ca-certificates/extra.crt '/BEGIN CERTIFICATE/' '{*}' >/dev/null \
      && for c in /tmp/cacert-*.pem; do \
           "$JAVA_HOME/bin/keytool" -importcert -noprompt -trustcacerts \
             -alias "extra-$(basename "$c" .pem)" -file "$c" \
             -keystore "$JAVA_HOME/lib/security/cacerts" -storepass changeit >/dev/null || true; \
         done \
      && echo "trusted extra CA in system + JDK stores: $EXTRA_CA_BUNDLE"; \
    fi; rm -rf /tmp/extra-ca /tmp/cacert-*.pem
ENV PIP_CERT=/etc/ssl/certs/ca-certificates.crt \
    REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

ENV PATH="$JAVA_HOME/bin:$PATH"
WORKDIR /workspace

# Dependencies first so this layer caches across source edits. The package
# directories must exist for the editable install to resolve.
COPY pyproject.toml /workspace/pyproject.toml
RUN mkdir -p data_generator ingestion governance \
 && touch data_generator/__init__.py ingestion/__init__.py governance/__init__.py \
 && python -m pip install --upgrade pip setuptools wheel \
 && python -m pip install -e ".[dev]"

# Pre-fetch the Delta JARs into the image so containers start without network.
# Without this, every `docker compose run` re-resolves them from Maven Central.
COPY conf /workspace/conf
RUN python -c "\
from pyspark.sql import SparkSession; \
s = SparkSession.builder.appName('warm-ivy-cache').master('local[1]') \
 .config('spark.jars.packages', 'io.delta:delta-spark_2.12:3.3.3') \
 .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
 .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
 .getOrCreate(); \
s.range(1).count(); s.stop(); print('delta jars cached')"

COPY . /workspace
CMD ["bash"]
