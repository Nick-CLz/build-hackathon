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


def compare_schemas(cur, catalog: str, registry) -> int:
    """Check every bronze table carries the metadata columns the contract promises.

    Row counts agreeing proves the same NUMBER of records arrived; it says
    nothing about their shape. A missing _record_hash produced identical counts
    and twelve broken staging models, so schema is checked explicitly.
    """
    from ingestion.config import METADATA_COLUMNS
    from ingestion.session import get_spark

    spark = get_spark("parity-schema")
    problems = 0
    print(f"\n{'table':<34}  metadata columns")
    print("-" * 74)
    for source in registry.sources:
        name = f"{registry.bronze_schema}.{source.name}"
        try:
            cur.execute(f"DESCRIBE TABLE {catalog}.{name}")
            remote_cols = {r[0] for r in cur.fetchall() if r[0] and not r[0].startswith("#")}
        except Exception:
            print(f"{name:<34}  absent on Databricks")
            problems += 1
            continue
        try:
            local_cols = set(spark.table(name).columns)
        except Exception:
            local_cols = set()

        missing_meta = [c for c in METADATA_COLUMNS if c not in remote_cols]
        missing_business = sorted(
            c for c in local_cols if not c.startswith("_") and c not in remote_cols
        )
        if missing_meta or missing_business:
            problems += 1
            detail = []
            if missing_meta:
                detail.append("missing metadata: " + ", ".join(missing_meta))
            if missing_business:
                detail.append("missing business: " + ", ".join(missing_business[:4]))
            print(f"{name:<34}  {'; '.join(detail)}")
        else:
            extra = sorted(c for c in remote_cols - local_cols if c.startswith("_"))
            note = f"ok (platform extras: {', '.join(extra)})" if extra else "ok"
            print(f"{name:<34}  {note}")
    print("-" * 74)
    return problems


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
    schema_problems = 0
    with connect() as conn, conn.cursor() as cur:
        remote = remote_counts(cur, args.catalog, names)
        schema_problems = compare_schemas(cur, args.catalog, registry)

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
    if schema_problems:
        print(f"{schema_problems} table(s) with a schema mismatch -- see above.")
    return 0 if (diverged == 0 and schema_problems == 0) else 1


if __name__ == "__main__":
    raise SystemExit(main())
