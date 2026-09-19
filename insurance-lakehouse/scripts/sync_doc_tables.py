#!/usr/bin/env python
"""Regenerate the defect table in the domain primer from MESS_REGISTRY.

Docs that restate code drift silently. This keeps the one table that does so
generated, and ``tests/test_docs.py`` fails the build if it goes stale.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from data_generator.config import MESS_REGISTRY  # noqa: E402

PRIMER = REPO_ROOT / "docs" / "insurance_domain_primer.md"
START, END = "<!-- MESS_TABLE_START -->", "<!-- MESS_TABLE_END -->"


def render_table() -> str:
    rows = ["| Defect | What it looks like | Where it's handled |", "|---|---|---|"]
    rows += [f"| `{k}` | {what} | {handled} |" for k, (what, handled) in MESS_REGISTRY.items()]
    return "\n".join(rows)


def apply(text: str) -> str:
    head = text[: text.index(START) + len(START)]
    tail = text[text.index(END) :]
    return f"{head}\n{render_table()}\n{tail}"


def main() -> int:
    current = PRIMER.read_text(encoding="utf-8")
    updated = apply(current)
    if current == updated:
        print("docs already in sync")
        return 0
    PRIMER.write_text(updated, encoding="utf-8")
    print(f"updated {PRIMER.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
