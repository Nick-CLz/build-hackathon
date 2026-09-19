"""``python -m data_generator`` / ``generate-data`` entry point."""

from __future__ import annotations

import argparse
import json
import sys

from .config import MESS_REGISTRY, GeneratorConfig
from .emit import emit_day
from .universe import build_universe


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="generate-data",
        description="Generate one daily drop of synthetic insurance data.",
    )
    p.add_argument(
        "--day",
        type=int,
        default=None,
        help="Which daily drop to emit (1 = initial load). Default 1.",
    )
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--n-policies", type=int, default=None)
    p.add_argument(
        "--out",
        dest="out_root",
        default=None,
        help="Landing-zone root. Default ./lakehouse/landing",
    )
    p.add_argument(
        "--list-defects",
        action="store_true",
        help="Print the registry of deliberate data defects and exit.",
    )
    p.add_argument("--quiet", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.list_defects:
        width = max(len(k) for k in MESS_REGISTRY)
        for code, (what, handled) in MESS_REGISTRY.items():
            print(f"{code:<{width}}  {what}\n{'':<{width}}  -> handled: {handled}")
        return 0

    cfg = GeneratorConfig.from_env(
        seed=args.seed,
        n_policies=args.n_policies,
        out_root=args.out_root,
    )
    day = args.day or 1
    cfg.day = day
    universe = build_universe(cfg)
    summary = emit_day(universe, day)

    if not args.quiet:
        print(
            f"day {day}  batch_date={summary['batch_date']}  "
            f"files={len(summary['files'])}  rows={summary['total_rows']}"
        )
        print("  defects: " + json.dumps(summary["defects"], default=str))
        print(f"  landing: {cfg.out_root}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
