#!/usr/bin/env python
"""Parse dbt artifacts into a Delta observability schema and render an HTML summary.

WHY THIS EXISTS RATHER THAN ELEMENTARY.

elementary-data publishes a `spark` extra, so Elementary is not categorically
incompatible with dbt-spark. But its `edr` CLI connects through the dbt profile,
and the profile here uses `method: session` -- an in-process SparkSession with
no thrift endpoint for an external process to attach to. Two separate Python
processes cannot share one local SparkSession, so `edr` has nothing to connect
to. Running Elementary would mean standing up a Thrift server purely to satisfy
the reporting tool, which is a lot of moving parts for a laptop skeleton.

The brief sanctions the fallback explicitly, so this is it: parse the artifacts
dbt already writes (`run_results.json`, `manifest.json`, `sources.json`), land
them in a Delta `observability` schema so history accumulates run over run, and
render a static HTML summary.

What is genuinely lost versus Elementary: anomaly detection on metrics over
time, and the hosted UI. What is kept: test outcomes with severity, model
timings, row counts, freshness state, and run-over-run history -- which is the
part that answers "is the warehouse healthy" at 9am.

On Databricks the recommendation flips: dbt-databricks connects over SQL
warehouses, `edr` works normally, and Elementary is worth adopting.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from html import escape
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

TARGET = REPO_ROOT / "dbt" / "target"
REPORTS = REPO_ROOT / "reports"
OBSERVABILITY_PATH = REPO_ROOT / "lakehouse" / "observability"


def _load(name: str) -> dict | None:
    path = TARGET / name
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def collect_test_results(run_results: dict, manifest: dict) -> list[dict]:
    """Flatten run_results into one row per node, carrying configured severity."""
    nodes = manifest.get("nodes", {}) if manifest else {}
    rows: list[dict] = []
    for r in run_results.get("results", []):
        uid = r.get("unique_id", "")
        node = nodes.get(uid, {})
        rows.append(
            {
                "run_started_at": run_results.get("metadata", {}).get("generated_at"),
                "unique_id": uid,
                "node_type": uid.split(".")[0] if uid else "unknown",
                "name": node.get("name") or uid.split(".")[-1],
                "resource_path": node.get("original_file_path"),
                "status": r.get("status"),
                "severity": (node.get("config") or {}).get("severity", "error"),
                "failures": r.get("failures"),
                "rows_affected": (r.get("adapter_response") or {}).get("rows_affected"),
                "execution_time_s": round(r.get("execution_time") or 0.0, 3),
                "message": (r.get("message") or "")[:500],
            }
        )
    return rows


def collect_freshness(sources: dict | None) -> list[dict]:
    if not sources:
        return []
    return [
        {
            "unique_id": r.get("unique_id"),
            "status": r.get("status"),
            "max_loaded_at": (r.get("criteria") or {}) and r.get("max_loaded_at"),
            "age_seconds": round((r.get("age") or 0.0), 1),
        }
        for r in sources.get("results", [])
    ]


def write_delta(test_rows: list[dict], freshness_rows: list[dict]) -> bool:
    """Append this run's results to the Delta observability schema.

    Best-effort: the HTML report is the primary deliverable and must render even
    when Spark is unavailable (for instance in a CI job that only parses
    artifacts). A failure here is reported, not fatal.
    """
    try:
        from pyspark.sql.types import StringType, StructField, StructType

        from ingestion.session import get_spark
    except Exception as exc:
        print(f"  (skipping Delta write: {type(exc).__name__}: {exc})")
        return False

    try:
        spark = get_spark("observability")
        spark.sql("CREATE DATABASE IF NOT EXISTS observability")
        for table, rows in (("test_results", test_rows), ("source_freshness", freshness_rows)):
            if not rows:
                continue
            path = str(OBSERVABILITY_PATH / table)
            # Explicit all-string schema. Inference raises CANNOT_DETERMINE_TYPE
            # when a column is None in every row of a run -- `failures` is null
            # for every model result, for instance -- and the artifact shape
            # varies with what dbt actually ran. Strings keep the table
            # append-compatible; anything needing arithmetic is cast at query time.
            columns = list(rows[0].keys())
            schema = StructType([StructField(c, StringType(), True) for c in columns])
            df = spark.createDataFrame(
                [
                    tuple(str(r.get(c)) if r.get(c) is not None else None for c in columns)
                    for r in rows
                ],
                schema,
            )
            (df.write.format("delta").mode("append").option("mergeSchema", "true").save(path))
            spark.sql(
                f"CREATE TABLE IF NOT EXISTS observability.{table} USING DELTA LOCATION '{path}'"
            )
        return True
    except Exception as exc:
        print(f"  (Delta write failed: {type(exc).__name__}: {exc})")
        return False


def _status_class(status: str, severity: str) -> str:
    if status in {"pass", "success"}:
        return "ok"
    if status == "warn" or (status in {"fail", "error"} and severity == "warn"):
        return "warn"
    return "fail"


def render_html(test_rows: list[dict], freshness_rows: list[dict], delta_written: bool) -> str:
    total = len(test_rows)
    tests = [r for r in test_rows if r["node_type"] == "test"]
    models = [r for r in test_rows if r["node_type"] in {"model", "snapshot", "seed"}]
    passed = sum(1 for r in tests if r["status"] in {"pass", "success"})
    warned = sum(1 for r in tests if r["status"] == "warn")
    failed = sum(1 for r in tests if r["status"] in {"fail", "error"} and r["severity"] != "warn")
    slowest = sorted(models, key=lambda r: r["execution_time_s"], reverse=True)[:10]

    def rows_html(rows, cols):
        out = []
        for r in rows:
            cls = _status_class(str(r.get("status")), str(r.get("severity", "error")))
            cells = "".join(
                f"<td>{escape(str(r.get(c) if r.get(c) is not None else '-'))}</td>" for c in cols
            )
            out.append(f'<tr class="{cls}">{cells}</tr>')
        return "\n".join(out)

    attention = [r for r in tests if r["status"] not in {"pass", "success"}]

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>Lakehouse observability</title>
<style>
 body{{font:14px/1.5 -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
      margin:0;padding:2rem;background:#f6f7f9;color:#1a1d21}}
 h1{{margin:0 0 .25rem}} .sub{{color:#666;margin-bottom:1.5rem}}
 .cards{{display:flex;gap:1rem;flex-wrap:wrap;margin-bottom:2rem}}
 .card{{background:#fff;border:1px solid #e3e6ea;border-radius:8px;
        padding:1rem 1.25rem;min-width:130px}}
 .card .n{{font-size:1.9rem;font-weight:600}}
 .card .l{{color:#666;font-size:.8rem;text-transform:uppercase;letter-spacing:.04em}}
 .ok .n{{color:#137333}} .warn .n{{color:#b06000}} .fail .n{{color:#c5221f}}
 table{{width:100%;border-collapse:collapse;background:#fff;
        border:1px solid #e3e6ea;border-radius:8px;overflow:hidden;margin-bottom:2rem}}
 th,td{{padding:.5rem .75rem;text-align:left;border-bottom:1px solid #eef0f2;
        font-size:.85rem}}
 th{{background:#fafbfc;font-weight:600}}
 tr.warn td:first-child{{box-shadow:inset 3px 0 0 #f9ab00}}
 tr.fail td:first-child{{box-shadow:inset 3px 0 0 #c5221f}}
 tr.ok td:first-child{{box-shadow:inset 3px 0 0 #34a853}}
 h2{{font-size:1rem;margin:0 0 .5rem}} code{{background:#f1f3f4;padding:0 .25rem}}
</style></head><body>
<h1>Insurance lakehouse &mdash; observability</h1>
<div class="sub">Generated {datetime.now(UTC):%Y-%m-%d %H:%M UTC} &middot;
 {total} nodes &middot; Delta history: {"written" if delta_written else "not written"}</div>

<div class="cards">
  <div class="card ok"><div class="n">{passed}</div><div class="l">tests passed</div></div>
  <div class="card warn"><div class="n">{warned}</div><div class="l">warnings</div></div>
  <div class="card fail"><div class="n">{failed}</div><div class="l">failures</div></div>
  <div class="card"><div class="n">{len(models)}</div><div class="l">models built</div></div>
  <div class="card"><div class="n">{
        len(freshness_rows)
    }</div><div class="l">sources checked</div></div>
</div>

<h2>Needs attention</h2>
<table><tr><th>Test</th><th>Status</th><th>Severity</th><th>Failures</th><th>Path</th></tr>
{
        rows_html(attention, ["name", "status", "severity", "failures", "resource_path"])
        or '<tr class="ok"><td colspan="5">Nothing failing or warning.</td></tr>'
    }
</table>

<h2>Slowest models</h2>
<table><tr><th>Model</th><th>Status</th><th>Seconds</th><th>Rows</th></tr>
{
        rows_html(slowest, ["name", "status", "execution_time_s", "rows_affected"])
        or '<tr><td colspan="4">No model results.</td></tr>'
    }
</table>

<h2>Source freshness</h2>
<table><tr><th>Source</th><th>Status</th><th>Age (s)</th></tr>
{
        rows_html(freshness_rows, ["unique_id", "status", "age_seconds"])
        or '<tr><td colspan="3">Not run. Use <code>make freshness</code>.</td></tr>'
    }
</table>
</body></html>"""


def main() -> int:
    run_results = _load("run_results.json")
    if not run_results:
        print(f"No run_results.json in {TARGET}. Run `make dbt-build` first.")
        return 1
    manifest = _load("manifest.json") or {}
    sources = _load("sources.json")

    test_rows = collect_test_results(run_results, manifest)
    freshness_rows = collect_freshness(sources)
    delta_written = write_delta(test_rows, freshness_rows)

    REPORTS.mkdir(parents=True, exist_ok=True)
    out = REPORTS / "observability.html"
    out.write_text(render_html(test_rows, freshness_rows, delta_written), encoding="utf-8")

    tests = [r for r in test_rows if r["node_type"] == "test"]
    warned = sum(1 for r in tests if r["status"] == "warn")
    failed = sum(1 for r in tests if r["status"] in {"fail", "error"} and r["severity"] != "warn")
    print(f"observability report -> {out.relative_to(REPO_ROOT)}")
    print(
        f"  {len(tests)} tests: {len(tests) - warned - failed} passed, "
        f"{warned} warned, {failed} failed"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
