#!/usr/bin/env python
"""Generate dbt/models/sources.yml from config/sources.yml.

One registry, two consumers. The freshness SLAs, primary keys and PII metadata
declared in config/sources.yml drive both the bronze ingestion engine and dbt's
source definitions, so they cannot disagree. A test asserts the generated file
is current.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import yaml  # noqa: E402

from ingestion.config import META_INGESTED_AT, load_registry  # noqa: E402

OUT = REPO_ROOT / "dbt" / "models" / "sources.yml"
HEADER = (
    "# GENERATED FILE -- do not edit by hand.\n"
    "# Regenerate with `make dbt-sources` after editing config/sources.yml.\n"
    "# Freshness SLAs, primary keys and PII tags all originate there.\n"
)


def build() -> dict:
    registry = load_registry()
    tables = []
    for s in registry.sources:
        columns = [
            {
                "name": p.column,
                "description": f"PII ({p.pii_type}, sensitivity {p.sensitivity}).",
                "meta": {"pii": True, "pii_type": p.pii_type, "sensitivity": p.sensitivity},
            }
            for p in s.pii_columns
        ]
        table: dict = {
            "name": s.name,
            "description": s.description,
            "meta": {
                "primary_key": s.primary_key,
                "source_format": s.format,
                "landing_path": s.path,
            },
        }
        if s.freshness:
            table["loaded_at_field"] = META_INGESTED_AT
            table["freshness"] = {
                "warn_after": {"count": s.freshness["warn_after_hours"], "period": "hour"},
                "error_after": {"count": s.freshness["error_after_hours"], "period": "hour"},
            }
        if columns:
            table["columns"] = columns
        tables.append(table)

    return {
        "version": 2,
        "sources": [
            {
                "name": "bronze",
                "description": (
                    "Raw landed data. Bronze adds metadata columns and nothing else; "
                    "all cleaning happens in silver."
                ),
                "schema": registry.bronze_schema,
                "tables": tables,
            }
        ],
    }


class _IndentedDumper(yaml.SafeDumper):
    """Indent sequences under their parent key.

    PyYAML's default puts list items at the parent's indent level, which is
    valid YAML but fails yamllint's default indentation rule -- and this file is
    linted in CI like any other.
    """

    def increase_indent(self, flow: bool = False, indentless: bool = False):
        return super().increase_indent(flow, False)


def render() -> str:
    body = yaml.dump(
        build(),
        Dumper=_IndentedDumper,
        sort_keys=False,
        allow_unicode=True,
        default_flow_style=False,
        width=100,
    )
    return HEADER + body


def main() -> int:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    new = render()
    if OUT.exists() and OUT.read_text(encoding="utf-8") == new:
        print("dbt/models/sources.yml already in sync")
        return 0
    OUT.write_text(new, encoding="utf-8")
    print(f"wrote {OUT.relative_to(REPO_ROOT)} ({len(build()['sources'][0]['tables'])} tables)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
