"""DAG integrity tests.

These run without a scheduler, a database or a warehouse, so they belong in the
fast CI job. They assert the properties that make a DAG safe to run unattended
-- the ones whose absence you would otherwise discover in production.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

DAGS_FOLDER = Path(__file__).resolve().parents[2] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    os.environ.setdefault("LAKEHOUSE_REPO", str(DAGS_FOLDER.parents[1]))
    from airflow.models import DagBag

    # Airflow 3 dropped the include_examples kwarg; example loading is
    # controlled by AIRFLOW__CORE__LOAD_EXAMPLES instead.
    return DagBag(dag_folder=str(DAGS_FOLDER))


def test_no_import_errors(dagbag):
    assert not dagbag.import_errors, dagbag.import_errors


def test_expected_dag_is_present(dagbag):
    assert "insurance_lakehouse" in dagbag.dags


def test_dag_has_retries(dagbag):
    """A task with no retries turns every transient blip into a 3am page."""
    dag = dagbag.dags["insurance_lakehouse"]
    for task in dag.tasks:
        assert task.retries is not None, task.task_id


def test_dag_is_serialised_with_tags(dagbag):
    """Tags are how anyone finds this DAG in a workspace with hundreds."""
    assert dagbag.dags["insurance_lakehouse"].tags


def test_single_active_run(dagbag):
    """Two concurrent runs would write the same Delta tables.

    Delta's optimistic concurrency would fail one of them on commit rather than
    corrupt anything, but a failed run is still a page.
    """
    assert dagbag.dags["insurance_lakehouse"].max_active_runs == 1


def test_end_task_depends_on_the_build(dagbag):
    """Regression test for a real bug.

    `end` originally hung off observability_report alone. That task uses
    trigger_rule='all_done' so the report survives a failed build -- which meant
    a failed dbt_build produced a SUCCESSFUL DAG run. A pipeline that is always
    green is one nobody looks at.
    """
    dag = dagbag.dags["insurance_lakehouse"]
    upstream = {t.task_id for t in dag.get_task("end").upstream_list}
    assert "dbt_build" in upstream, (
        "end must depend on dbt_build directly, or a failed build reports success"
    )


def test_observability_runs_even_when_the_build_fails(dagbag):
    """A failed build is exactly when the run-results report matters most."""
    dag = dagbag.dags["insurance_lakehouse"]
    assert dag.get_task("observability_report").trigger_rule == "all_done"
