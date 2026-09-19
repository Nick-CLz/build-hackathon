#!/usr/bin/env python
"""Compare local Delta tables against their Databricks counterparts.

A port is only real if the numbers agree. This reads row counts from both
sides and reports any divergence, per layer.

Run it BEFORE applying governance/unity_catalog.sql. A table carrying a column
mask or row filter cannot be read with VERSION AS OF, and a row filter changes
what a count returns depending on who is asking -- so the window for an honest
comparison closes once policies are attached. See docs/databricks_port.md.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from ingestion.config import load_registry  # noqa: E402
from scripts.databricks_probe import connect, load_env  # noqa: E402

# (local schema.table, databricks schema.table). Bronze comes from the registry.
MODEL_PAIRS = [
    ("silver.stg_partner_policies", "silver.stg_partner_policies"),
    ("silver.silver_policies", "silver.silver_policies"),
    ("silver.silver_claims", "silver.silver_claims"),
    ("gold.fct_written_premium", "gold.fct_written_premium"),
    ("gold.fct_claims", "gold.fct_claims"),
    ("marts.mart_loss_ratio", "marts.mart_loss_ratio"),
]


def local_counts(names: list[str]) -> dict[str, int | None]:
    from ingestion.session import get_spark

    spark = get_spark("parity")
    out: dict[str, int | None] = {}
    for name in names:
        try:
            out[name] = spark.table(name).count()
        except Exception:
            out[name] = None
    return out


def remote_counts(cur, catalog: str, names: list[str]) -> dict[str, int | None]:
    out: dict[str, int | None] = {}
    for name in names:
        try:
            cur.execute(f"SELECT count(*) FROM {catalog}.{name}")
            out[name] = cur.fetchone()[0]
        except Exception:
            out[name] = None
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default=os.getenv("DATABRICKS_CATALOG", "insurance_dev"))
    ap.add_argument(
        "--bronze-only",
        action="store_true",
        help="Compare bronze only; use before dbt has run on Databricks.",
    )
    args = ap.parse_args()

    load_env()
    registry = load_registry()

    names = [f"{registry.bronze_schema}.{s.name}" for s in registry.sources]
    if not args.bronze_only:
        names += [m for m, _ in MODEL_PAIRS]

    print("reading local counts ...")
    local = local_counts(names)

    print("reading Databricks counts ...")
    with connect() as conn, conn.cursor() as cur:
        remote = remote_counts(cur, args.catalog, names)

    print(f"\n{'table':<34}{'local':>10}{'databricks':>13}   verdict")
    print("-" * 74)
    matched = diverged = skipped = 0
    for name in names:
        lo, re_ = local.get(name), remote.get(name)
        if lo is None and re_ is None:
            verdict, skipped = "absent both sides", skipped + 1
        elif lo is None or re_ is None:
            verdict, diverged = "MISSING ON ONE SIDE", diverged + 1
        elif lo == re_:
            verdict, matched = "match", matched + 1
        else:
            verdict, diverged = f"DIFFERS by {re_ - lo:+d}", diverged + 1
        print(
            f"{name:<34}{'-' if lo is None else lo:>10}"
            f"{'-' if re_ is None else re_:>13}   {verdict}"
        )

    print("-" * 74)
    print(f"{matched} match, {diverged} diverged, {skipped} absent both sides")
    if diverged:
        print("\nA divergence is usually one of:")
        print("  * COPY INTO skipped files it had already loaded (re-run is a no-op)")
        print("  * the local run used a different LAKEHOUSE_N_POLICIES")
        print("  * quarantined rows differ because bronze parsed the file differently")
    return 0 if diverged == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
