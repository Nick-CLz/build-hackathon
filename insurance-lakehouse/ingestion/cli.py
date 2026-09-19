"""``python -m ingestion`` -- run bronze ingestion."""

from __future__ import annotations

import argparse
import sys

from .bronze import ingest_all, new_batch_id
from .config import load_registry
from .session import get_spark


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="bronze", description="Config-driven bronze ingestion.")
    p.add_argument(
        "--source",
        action="append",
        dest="sources",
        help="Ingest only this source (repeatable). Default: all.",
    )
    p.add_argument("--config", default=None, help="Path to sources.yml")
    p.add_argument("--batch-id", default=None)
    p.add_argument("--list-sources", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    registry = load_registry(args.config)

    if args.list_sources:
        for s in registry.sources:
            pk = ",".join(s.primary_key) or "-"
            print(f"{s.name:<26} {s.format:<5} pk={pk:<28} {s.path}")
        return 0

    spark = get_spark()
    batch_id = args.batch_id or new_batch_id()
    print(f"batch_id={batch_id}")
    results = ingest_all(spark, registry, only=args.sources, batch_id=batch_id)

    for r in results:
        print("  " + r.as_row())
    ingested = sum(r.rows_ingested for r in results)
    quarantined = sum(r.rows_quarantined for r in results)
    new_files = sum(r.files_new for r in results)
    print(f"\ntotal: {new_files} new files, {ingested} rows, {quarantined} quarantined")
    return 0


if __name__ == "__main__":
    sys.exit(main())
