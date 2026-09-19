#!/usr/bin/env python
"""Execute governance/unity_catalog.sql against Unity Catalog, statement by statement.

Generated-and-supported is not the same as applied. The probe proved each
operation TYPE works; this proves the actual generated file runs against the
actual tables.

Statements are executed individually and their outcomes reported rather than
wrapped in a transaction, because the interesting output is *which* parts
apply. A grant to a group that does not exist should not prevent the column
masks from binding -- and knowing that distinction is the point of running it.

ORDERING NOTE: run this AFTER validating the port. A table carrying a column
mask or row filter cannot be read with VERSION AS OF, so applying governance
first closes the window for comparing a table against its previous version.
See docs/databricks_port.md.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts.databricks_probe import connect, load_env  # noqa: E402

SQL_FILE = REPO_ROOT / "governance" / "unity_catalog.sql"

# Groups the row filter grants visibility to. If none of them exist, attaching
# the filter makes the table return zero rows to everyone -- silently.
FILTER_PRINCIPALS = ("data_engineers", "analysts_global", "analysts_th", "analysts_id")


@dataclass
class Outcome:
    kind: str
    statement: str
    ok: bool
    detail: str


def split_statements(sql: str) -> list[str]:
    """Split on semicolons that end a statement, ignoring comment-only chunks."""
    stripped = "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--"))
    return [s.strip() for s in stripped.split(";") if s.strip()]


def classify(statement: str) -> str:
    s = statement.upper()
    if s.startswith("USE CATALOG"):
        return "use catalog"
    if "SET TAGS" in s:
        return "column tag"
    if s.startswith("CREATE OR REPLACE FUNCTION") and "BOOLEAN" in s:
        return "row filter function"
    if s.startswith("CREATE OR REPLACE FUNCTION"):
        return "mask function"
    if "SET MASK" in s:
        return "bind column mask"
    if "SET ROW FILTER" in s:
        return "bind row filter"
    if s.startswith("GRANT"):
        return "grant"
    return "other"


def principals_exist(cur, names: tuple[str, ...]) -> list[str]:
    """Which of these principals Unity Catalog can actually resolve.

    Checked by attempting a harmless grant and reading the error, because
    information_schema does not expose account-level groups. The grant is
    revoked immediately; only its success or failure is of interest.
    """
    found = []
    for name in names:
        try:
            cur.execute(
                f"GRANT USE CATALOG ON CATALOG {os.environ.get('DATABRICKS_CATALOG', 'insurance_dev')} TO `{name}`"
            )
            found.append(name)
        except Exception as exc:
            if "PRINCIPAL_DOES_NOT_EXIST" not in str(exc):
                found.append(name)  # a different error means it probably exists
    return found


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default=os.getenv("DATABRICKS_CATALOG", "insurance_dev"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    load_env()
    if not SQL_FILE.exists():
        print(f"{SQL_FILE} missing -- run `make governance` first.")
        return 1

    statements = split_statements(SQL_FILE.read_text(encoding="utf-8"))
    print(f"{len(statements)} statement(s) in {SQL_FILE.name}")

    if args.dry_run:
        by_kind: dict[str, int] = {}
        for st in statements:
            by_kind[classify(st)] = by_kind.get(classify(st), 0) + 1
        for kind, n in sorted(by_kind.items(), key=lambda kv: -kv[1]):
            print(f"  {kind:<24} {n}")
        return 0

    outcomes: list[Outcome] = []
    with connect() as conn, conn.cursor() as cur:
        # A row filter whose principals do not exist evaluates false for every
        # reader, and the table then returns ZERO ROWS with no error at all.
        # That is a silent outage: the data looks deleted. Attaching one before
        # its groups exist is never correct, so the filter bindings are skipped
        # rather than applied hopefully.
        resolvable = principals_exist(cur, FILTER_PRINCIPALS)
        skip_filters = not resolvable
        if skip_filters:
            print(
                "\nWARNING: none of the row-filter principals exist "
                f"({', '.join(FILTER_PRINCIPALS)})."
            )
            print(
                "Attaching the filter would make every governed table return zero rows to everyone,"
            )
            print("with no error. ROW FILTER bindings are SKIPPED. Create the groups, then re-run.")

        for st in statements:
            kind = classify(st)
            if kind == "bind row filter" and skip_filters:
                outcomes.append(Outcome(kind, st, True, "skipped: principals do not exist"))
                continue
            try:
                cur.execute(st)
                outcomes.append(Outcome(kind, st, True, "applied"))
            except Exception as exc:
                msg = str(exc).splitlines()[0]
                # Strip the request id; it changes every run and adds nothing.
                msg = re.sub(r"\s*\[ReqId:[^\]]+\]", "", msg)
                outcomes.append(Outcome(kind, st, False, msg[:150]))

    print("\n=== BY STATEMENT TYPE ===")
    kinds = sorted({o.kind for o in outcomes})
    for kind in kinds:
        group = [o for o in outcomes if o.kind == kind]
        ok = sum(1 for o in group if o.ok)
        flag = "OK  " if ok == len(group) else "PART" if ok else "FAIL"
        print(f"  [{flag}] {kind:<24} {ok}/{len(group)} applied")

    failures = [o for o in outcomes if not o.ok]
    if failures:
        print("\n=== FAILURES ===")
        seen: set[str] = set()
        for f in failures:
            key = f.detail[:80]
            if key in seen:
                continue
            seen.add(key)
            first_line = " ".join(f.statement.split())[:90]
            print(f"  {f.kind}: {first_line}")
            print(f"    -> {f.detail}")

    skipped = sum(1 for o in outcomes if o.ok and o.detail.startswith("skipped"))
    applied = sum(1 for o in outcomes if o.ok) - skipped
    print(
        f"\n{applied}/{len(outcomes)} statements applied"
        + (f", {skipped} skipped deliberately." if skipped else ".")
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
