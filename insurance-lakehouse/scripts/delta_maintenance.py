#!/usr/bin/env python
"""Run and report Delta maintenance against a bronze table. `make maintenance`."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.config import load_registry
from ingestion.maintenance import DATABRICKS_ONLY, history, run_maintenance
from ingestion.session import get_spark


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="partner_policies_a")
    ap.add_argument("--zorder", default=None, help="Comma-separated ZORDER columns")
    args = ap.parse_args()

    registry = load_registry()
    source = registry.get(args.source)
    path = f"{registry.bronze_path}/{source.name}"
    zorder = (
        args.zorder.split(",")
        if args.zorder
        else [source.primary_key[0]]
        if source.primary_key
        else []
    )

    spark = get_spark("delta-maintenance")

    print(f"\n=== TABLE HISTORY: {source.name} (time travel is what makes this possible) ===")
    history(spark, path).show(truncate=60)

    print(f"=== MAINTENANCE PROBE: {source.name}, ZORDER BY {zorder} ===")
    results = run_maintenance(spark, path, zorder)
    for r in results:
        print(r.render())

    print("\n=== DATABRICKS-ONLY (no local equivalent; see docs/databricks_port.md) ===")
    for name, why in DATABRICKS_ONLY.items():
        print(f"  [N/A ] {name:<42} {why}")

    failed = [r for r in results if not r.supported]
    print(f"\n{len(results) - len(failed)}/{len(results)} operations supported locally.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
