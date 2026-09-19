#!/usr/bin/env python
"""Probe what a Databricks workspace actually permits, before relying on it.

Databricks Free Edition is serverless-only with usage limits, and which
governance features it exposes is not something to assume from documentation
aimed at paid tiers. This follows the same discipline as
`ingestion/maintenance.py`: attempt each operation and report the real outcome,
so `docs/databricks_port.md` reflects the workspace rather than folklore.

Reads DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN from the
environment (a gitignored .env is loaded if present). Nothing is written except
inside the probe catalog, which is dropped at the end unless --keep is passed.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

REQUIRED = ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")


@dataclass
class Probe:
    name: str
    ok: bool
    detail: str

    def render(self) -> str:
        return f"  [{'OK  ' if self.ok else 'FAIL'}] {self.name:<44} {self.detail}"


def load_env() -> None:
    env_file = REPO_ROOT / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def connect():
    from databricks import sql

    host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
    return sql.connect(
        server_hostname=host,
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def run(cursor, name: str, statement: str, fetch: bool = False) -> Probe:
    try:
        cursor.execute(statement)
        detail = "supported"
        if fetch:
            rows = cursor.fetchall()
            detail = f"{len(rows)} row(s): " + ", ".join(str(r[0]) for r in rows[:6])
        return Probe(name, True, detail[:100])
    except Exception as exc:
        return Probe(name, False, f"{type(exc).__name__}: {str(exc).splitlines()[0][:80]}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default=os.getenv("DATABRICKS_CATALOG", "insurance_dev"))
    ap.add_argument("--keep", action="store_true", help="Do not drop the probe catalog")
    args = ap.parse_args()

    load_env()
    missing = [k for k in REQUIRED if not os.getenv(k)]
    if missing:
        print("Missing environment variables: " + ", ".join(missing))
        print("\nCopy .env.example to .env and fill in:")
        print("  DATABRICKS_HOST        dbc-xxxxxxxx-xxxx.cloud.databricks.com")
        print(
            "  DATABRICKS_HTTP_PATH   /sql/1.0/warehouses/<id>   (SQL Warehouses -> "
            "your warehouse -> Connection details)"
        )
        print("  DATABRICKS_TOKEN       Settings -> Developer -> Access tokens")
        return 1

    probe_cat = f"{args.catalog}_probe"
    results: list[Probe] = []

    print(f"Connecting to {os.environ['DATABRICKS_HOST']} ...")
    with connect() as conn, conn.cursor() as cur:
        results.append(run(cur, "Connect and run SQL", "SELECT 1"))
        results.append(run(cur, "Server version", "SELECT current_version()", fetch=True))
        results.append(run(cur, "Unity Catalog present", "SHOW CATALOGS", fetch=True))
        results.append(run(cur, "Create a catalog", f"CREATE CATALOG IF NOT EXISTS {probe_cat}"))
        results.append(
            run(cur, "Create a schema", f"CREATE SCHEMA IF NOT EXISTS {probe_cat}.probe")
        )
        results.append(
            run(
                cur,
                "Create a managed Volume",
                f"CREATE VOLUME IF NOT EXISTS {probe_cat}.probe.landing",
            )
        )
        results.append(
            run(
                cur,
                "Create a Delta table",
                f"CREATE TABLE IF NOT EXISTS {probe_cat}.probe.t "
                "(id BIGINT, country STRING, secret STRING) USING DELTA",
            )
        )
        results.append(
            run(
                cur,
                "Insert rows",
                f"INSERT INTO {probe_cat}.probe.t VALUES (1,'TH','abc'),(2,'ID','def')",
            )
        )

        # --- the features the governance layer depends on -----------------
        results.append(
            run(
                cur,
                "Column TAGS",
                f"ALTER TABLE {probe_cat}.probe.t ALTER COLUMN secret SET TAGS ('pii' = 'true')",
            )
        )
        results.append(
            run(
                cur,
                "Create a mask function",
                f"CREATE OR REPLACE FUNCTION {probe_cat}.probe.m(c STRING) "
                "RETURNS STRING RETURN CASE "
                "WHEN is_account_group_member('data_engineers') THEN c "
                "ELSE '***' END",
            )
        )
        results.append(
            run(
                cur,
                "Attach a COLUMN MASK",
                f"ALTER TABLE {probe_cat}.probe.t ALTER COLUMN secret SET MASK {probe_cat}.probe.m",
            )
        )
        results.append(
            run(
                cur,
                "Create a row-filter function",
                f"CREATE OR REPLACE FUNCTION {probe_cat}.probe.f(c STRING) "
                "RETURNS BOOLEAN RETURN c = 'TH' "
                "OR is_account_group_member('data_engineers')",
            )
        )
        results.append(
            run(
                cur,
                "Attach a ROW FILTER",
                f"ALTER TABLE {probe_cat}.probe.t SET ROW FILTER {probe_cat}.probe.f ON (country)",
            )
        )
        results.append(
            run(
                cur,
                "GRANT to a group",
                f"GRANT SELECT ON TABLE {probe_cat}.probe.t TO `account users`",
            )
        )

        # --- Delta features, mirroring make maintenance -------------------
        results.append(run(cur, "OPTIMIZE", f"OPTIMIZE {probe_cat}.probe.t"))
        results.append(
            run(
                cur,
                "Liquid clustering (CLUSTER BY)",
                f"ALTER TABLE {probe_cat}.probe.t CLUSTER BY (country)",
            )
        )
        results.append(
            run(cur, "Time travel", f"SELECT * FROM {probe_cat}.probe.t VERSION AS OF 0")
        )
        results.append(run(cur, "DESCRIBE HISTORY", f"DESCRIBE HISTORY {probe_cat}.probe.t"))
        results.append(
            run(
                cur,
                "Serverless compute in use",
                "SELECT current_catalog(), current_user()",
                fetch=True,
            )
        )

        if not args.keep:
            run(cur, "cleanup", f"DROP CATALOG IF EXISTS {probe_cat} CASCADE")

    print("\n=== WORKSPACE CAPABILITIES ===")
    for r in results:
        print(r.render())
    ok = sum(1 for r in results if r.ok)
    print(f"\n{ok}/{len(results)} probes succeeded.")
    print("\nAnything marked FAIL above is a real constraint of this workspace tier.")
    print("Record it in docs/databricks_port.md rather than working around it silently.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
