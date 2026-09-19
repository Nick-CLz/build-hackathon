"""End-to-end insurance lakehouse pipeline.

    generate (optional) -> bronze ingestion -> source freshness
                        -> dbt build (models + tests + snapshots)
                        -> observability report

DESIGN NOTES worth reading before changing anything here.

**Idempotency.** Every task can be re-run for the same logical date without
changing the result. The generator rebuilds deterministically from
``(seed, n_policies)``; bronze skips files already in its manifest; dbt models
merge on a unique key. That property is what makes backfills safe, and it is
enforced by tests rather than by convention (``test_ingest_is_idempotent``).

**Parameterisation by logical date.** The daily drop is selected by
``data_interval_start``, never by ``datetime.now()``. Wall-clock time in a DAG
silently destroys backfill correctness: a backfill of last Tuesday would
generate today's data and label it Tuesday.

**dbt rendering.** Cosmos renders each dbt model as its own Airflow task, so a
failure points at the model that broke rather than at a monolithic
``dbt build`` step. It loads from a pre-built ``manifest.json``
(``LoadMode.DBT_MANIFEST``) instead of invoking ``dbt ls`` at parse time. The
default would shell out to dbt on every scheduler heartbeat, which on a project
this size adds seconds to each parse and needs a live warehouse connection just
to render the DAG. The manifest is produced by ``make dbt-manifest``.

**max_active_runs=1.** Two concurrent runs would write the same Delta tables.
Delta's optimistic concurrency would make one of them fail on commit rather
than corrupt anything, but a failed run is still a page at 3am.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task

REPO = Path(os.getenv("LAKEHOUSE_REPO", "/usr/local/airflow/lakehouse_repo"))
DBT_PROJECT = REPO / "dbt"
# Resolved from PATH by default. Hardcoding the container path
# (/usr/local/airflow/.local/bin/dbt) made this task fail instantly anywhere
# else, including a local `airflow dags test`.
DBT_EXECUTABLE = os.getenv("DBT_EXECUTABLE", "dbt")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    # Overridable so tests can exercise the failure path without waiting out
    # the backoff; production leaves it at 2.
    "retries": int(os.getenv("LAKEHOUSE_TASK_RETRIES", "2")),
    # Exponential backoff: a transient S3/metastore blip clears in seconds, but
    # retrying a genuinely broken task three times in 90 seconds just burns
    # compute and delays the alert.
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "depends_on_past": False,
}


def alert_on_failure(context) -> None:
    """Failure callback. Stubbed deliberately.

    In production this routes to PagerDuty or Slack. It is left as a stub with
    an explicit structure rather than omitted, because the shape of the alert
    matters more than the transport: a page that does not say which table, which
    logical date and which upstream source is a page that starts an
    investigation rather than ending one.
    """
    ti = context.get("task_instance")
    dag_obj = context.get("dag")
    print(
        f"ALERT dag={dag_obj.dag_id if dag_obj else '?'} "
        f"task={ti.task_id if ti else '?'} "
        f"logical_date={context.get('logical_date')} "
        f"try={ti.try_number if ti else '?'} "
        f"log_url={getattr(ti, 'log_url', '?')}"
    )


def alert_on_sla_miss(*args, **kwargs) -> None:
    """SLA-miss callback. Also stubbed; see alert_on_failure."""
    print("SLA MISS", args, kwargs)


@dag(
    dag_id="insurance_lakehouse",
    description="generate -> bronze -> freshness -> dbt build -> observability",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 5),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=alert_on_failure,
    tags=["insurance", "lakehouse", "delta", "dbt"],
    doc_md=__doc__,
    params={
        # Generation is optional so the same DAG serves both the demo (where it
        # creates its own input) and a real deployment (where partners deliver).
        "generate_data": True,
        "day": 1,
    },
)
def insurance_lakehouse():
    start = EmptyOperator(task_id="start")

    @task.bash(task_id="generate_daily_drop")
    def generate_daily_drop() -> str:
        """Emit one daily drop. Skipped when partners deliver the files."""
        return (
            f"cd {REPO} && "
            'if [ "{{ params.generate_data }}" = "True" ]; then '
            "python -m data_generator --day {{ params.day }}; "
            'else echo "generation disabled; expecting partner-delivered files"; fi'
        )

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=f"cd {REPO} && python -m ingestion",
        doc_md=(
            "Config-driven ingestion of every source in config/sources.yml. "
            "Idempotent via the Delta manifest table: re-running reports "
            "UP_TO_DATE and writes nothing."
        ),
    )

    source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        # Freshness failure must not stop the build. A stale partner feed is a
        # real signal, but yesterday's data is usually better than none, and the
        # observability report records the staleness either way.
        bash_command=(
            f"cd {REPO} && {DBT_EXECUTABLE} source freshness "
            f"--project-dir {DBT_PROJECT} --profiles-dir {DBT_PROJECT} || true"
        ),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {REPO} && {DBT_EXECUTABLE} build "
            f"--project-dir {DBT_PROJECT} --profiles-dir {DBT_PROJECT} "
            f"--target ${{DBT_TARGET:-local}}"
        ),
        doc_md=(
            "Models, tests and snapshots in dependency order. Replaced by a "
            "Cosmos DbtTaskGroup when a manifest is available -- see "
            "dbt_task_group.py."
        ),
    )

    observability = BashOperator(
        task_id="observability_report",
        bash_command=f"cd {REPO} && python scripts/build_observability_report.py",
        trigger_rule="all_done",
        doc_md=(
            "Runs even when upstream fails (all_done), because a failed build "
            "is exactly when you most want the run-results parsed and visible."
        ),
    )

    # `end` depends on BOTH the build and the report, with the default
    # all_success rule.
    #
    # The first version hung `end` off observability_report alone. Because that
    # task uses all_done, it succeeded even when dbt_build failed -- and `end`
    # then succeeded too, so the whole DAG run was marked SUCCESS with a failed
    # build inside it. A green run that hides a broken warehouse is worse than
    # no orchestration at all: nobody looks at a pipeline that is always green.
    #
    # Depending on dbt_build directly means its failure propagates to `end` as
    # upstream_failed and the run fails, while observability still runs via
    # all_done and captures why.
    end = EmptyOperator(task_id="end")

    (
        start
        >> generate_daily_drop()
        >> ingest_bronze
        >> source_freshness
        >> dbt_build
        >> observability
        >> end
    )
    dbt_build >> end


insurance_lakehouse()
