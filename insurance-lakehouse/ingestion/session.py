"""SparkSession construction for the ingestion jobs.

Delta configuration deliberately lives in ``conf/spark-defaults.conf`` rather
than here, so that dbt-spark's session method -- which builds its own
SparkSession and accepts no such settings -- picks up an identical config.
See docs/decisions.md ADR-0003.
"""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession

REPO_ROOT = Path(__file__).resolve().parents[1]


def _ensure_spark_conf_dir() -> None:
    """Point SPARK_CONF_DIR at the repo's conf/ if the caller has not."""
    if not os.getenv("SPARK_CONF_DIR"):
        os.environ["SPARK_CONF_DIR"] = str(REPO_ROOT / "conf")
    # The agent proxy injects JAVA_TOOL_OPTIONS, which spark-submit mis-parses.
    os.environ.pop("JAVA_TOOL_OPTIONS", None)


def get_spark(app_name: str = "insurance-lakehouse-bronze") -> SparkSession:
    _ensure_spark_conf_dir()
    spark = SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("LAKEHOUSE_LOG_LEVEL", "ERROR"))
    if "io.delta" not in (spark.conf.get("spark.sql.extensions", "") or ""):
        raise RuntimeError(
            "Delta extensions are not loaded. SPARK_CONF_DIR must point at the "
            f"repo's conf/ directory (currently {os.getenv('SPARK_CONF_DIR')!r})."
        )
    return spark
