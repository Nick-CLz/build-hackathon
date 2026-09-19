#!/usr/bin/env python
"""Generate Unity Catalog governance SQL from PII metadata.

SINGLE SOURCE OF TRUTH. PII classification is declared once -- in dbt column
`meta` for models, and in `config/sources.yml` for bronze -- and every
enforcement artefact is generated from it. Nothing here is hand-maintained.

That is the whole design argument. The alternative (tags applied by hand in the
catalog, masks written separately, a spreadsheet tracking sensitive columns)
does not fail immediately. It fails at the third schema change, when the
spreadsheet and the warehouse quietly diverge and nobody notices until an audit.

Output is `governance/unity_catalog.sql`: correct Databricks SQL, ready to run
against a workspace. It does not execute locally -- there is no Unity Catalog in
OSS Spark -- which is why it is generated as a reviewable artefact rather than
applied silently.
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import yaml  # noqa: E402

from ingestion.config import load_registry  # noqa: E402

OUT = REPO_ROOT / "governance" / "unity_catalog.sql"
DBT_MODELS = REPO_ROOT / "dbt" / "models"

# Groups the generated grants refer to. Kept here rather than in the SQL so the
# principle -- least privilege, one group per access need -- stays visible.
GROUPS = {
    "data_engineers": "Build and operate the pipeline. Full access on dev, read on prod.",
    "analysts": "Read gold and marts only. Never silver, never unmasked PII.",
    "restricted_health": "The sole group permitted unmasked health declarations.",
}

# pii_type -> (mask function name, returned expression for a non-privileged reader)
MASK_STRATEGIES = {
    "national_id": ("mask_national_id", "sha2(concat(secret('insurance', 'pii_salt'), col), 256)"),
    "phone": ("mask_phone", "concat(substr(col, 1, 3), '****', substr(col, -2, 2))"),
    "plate": ("mask_plate", "concat('***', substr(col, -2, 2))"),
    "email": ("mask_email", "concat(substr(col, 1, 1), '***@', split_part(col, '@', 2))"),
    "dob": ("mask_dob", "concat(cast(year(col) as string), '-01-01')"),
    "name": ("mask_name", "concat(substr(col, 1, 1), '.')"),
    "address": ("mask_address", "'[redacted]'"),
    "chassis": ("mask_chassis", "concat('***', substr(col, -4, 4))"),
    "health": ("mask_health", "null"),
}


def collect_from_dbt() -> list[dict]:
    """Every column carrying `meta.pii: true` in a dbt schema YAML."""
    found: list[dict] = []
    for path in sorted(DBT_MODELS.rglob("*.yml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for section, schema_key in (("models", None), ("sources", "schema")):
            for entry in doc.get(section) or []:
                tables = entry.get("tables", [entry]) if section == "sources" else [entry]
                schema = entry.get(schema_key) if schema_key else _schema_for(path)
                for table in tables:
                    for col in table.get("columns") or []:
                        meta = col.get("meta") or {}
                        if not meta.get("pii"):
                            continue
                        found.append(
                            {
                                "schema": schema,
                                "table": table["name"],
                                "column": col["name"],
                                "pii_type": meta.get("pii_type", "unknown"),
                                "sensitivity": meta.get("sensitivity", "medium"),
                                "already_masked": bool(meta.get("masked")),
                            }
                        )
    return found


def _schema_for(path: Path) -> str:
    """Layer inferred from the model's directory, matching dbt_project.yml."""
    parts = path.relative_to(DBT_MODELS).parts
    for layer in ("silver", "gold", "marts"):
        if layer in parts:
            return layer
    return "silver"


def collect_from_sources() -> list[dict]:
    """PII declared in config/sources.yml, which governs the bronze layer."""
    registry = load_registry()
    return [
        {
            "schema": registry.bronze_schema,
            "table": source.name,
            "column": pii.column,
            "pii_type": pii.pii_type,
            "sensitivity": pii.sensitivity,
            "already_masked": False,
        }
        for source in registry.sources
        for pii in source.pii_columns
    ]


def render(columns: list[dict], catalog: str) -> str:
    lines: list[str] = []
    w = lines.append

    w("-- =====================================================================")
    w("-- Unity Catalog governance for the insurance lakehouse")
    w("--")
    w("-- GENERATED FILE -- do not edit by hand.")
    w("--   Regenerate: make governance")
    w("--   Source of truth: dbt column `meta` + config/sources.yml")
    w("--")
    w(f"-- Generated {datetime.now(UTC):%Y-%m-%d %H:%M UTC}")
    w(f"-- Catalog: {catalog}")
    w(f"-- Classified columns: {len(columns)}")
    w("--")
    w("-- This does not run against OSS Spark; there is no Unity Catalog outside")
    w("-- Databricks. It is generated as a reviewable artefact so the governance")
    w("-- posture can be diffed in code review rather than clicked in a UI.")
    w("-- =====================================================================")
    w("")
    w(f"USE CATALOG {catalog};")
    w("")

    # ---------------------------------------------------------------- groups
    w("-- ---------------------------------------------------------------------")
    w("-- 1. Groups")
    w("-- ---------------------------------------------------------------------")
    for name, purpose in GROUPS.items():
        w(f"-- {name}: {purpose}")
    w("-- Groups are created at account level (SCIM or Terraform), not in SQL.")
    w("-- Listed here so the grants below are readable without cross-referencing.")
    w("")

    # ----------------------------------------------------------------- tags
    w("-- ---------------------------------------------------------------------")
    w("-- 2. Column tags")
    w("--")
    w("-- Tags drive discovery ('where is our national ID data?') and can drive")
    w("-- policy. They are the machine-readable form of the classification.")
    w("-- ---------------------------------------------------------------------")
    for c in sorted(columns, key=lambda x: (x["schema"], x["table"], x["column"])):
        w(
            f"ALTER TABLE {c['schema']}.{c['table']} "
            f"ALTER COLUMN {c['column']} "
            f"SET TAGS ('pii' = 'true', 'pii_type' = '{c['pii_type']}', "
            f"'sensitivity' = '{c['sensitivity']}');"
        )
    w("")

    # ---------------------------------------------------------------- masks
    w("-- ---------------------------------------------------------------------")
    w("-- 3. Column mask functions")
    w("--")
    w("-- The mask is applied at QUERY TIME by the engine, so it holds however")
    w("-- the table is read -- SQL, notebook, BI tool or JDBC. Masking only in a")
    w("-- dbt model protects the model; masking here protects the column.")
    w("--")
    w("-- The salt comes from a secret scope, never a literal. An unsalted hash")
    w("-- of a 13-digit national ID is trivially reversible by enumeration.")
    w("-- ---------------------------------------------------------------------")
    used = {c["pii_type"] for c in columns if c["pii_type"] in MASK_STRATEGIES}
    for pii_type in sorted(used):
        fn, expr = MASK_STRATEGIES[pii_type]
        privileged = "restricted_health" if pii_type == "health" else "data_engineers"
        w(f"CREATE OR REPLACE FUNCTION {fn}(col STRING)")
        w("  RETURNS STRING")
        w(f"  COMMENT 'Mask for {pii_type}. Unmasked for {privileged}.'")
        w("  RETURN CASE")
        w(f"    WHEN is_account_group_member('{privileged}') THEN col")
        w("    WHEN col IS NULL THEN NULL")
        w(f"    ELSE {expr}")
        w("  END;")
        w("")

    w("-- Bind masks to columns. Columns already masked in the dbt model are")
    w("-- bound anyway: defence in depth, and the model could be changed.")
    for c in sorted(columns, key=lambda x: (x["schema"], x["table"], x["column"])):
        if c["pii_type"] not in MASK_STRATEGIES:
            continue
        fn, _ = MASK_STRATEGIES[c["pii_type"]]
        w(f"ALTER TABLE {c['schema']}.{c['table']} ALTER COLUMN {c['column']} SET MASK {fn};")
    w("")

    # ----------------------------------------------------------- row filter
    w("-- ---------------------------------------------------------------------")
    w("-- 4. Row filter: jurisdiction")
    w("--")
    w("-- Thai and Indonesian personal data in one warehouse is a CROSS-BORDER")
    w("-- TRANSFER question, not merely a storage one. PDPA restricts transfer to")
    w("-- jurisdictions without adequate protection; Indonesia's PDP Law is")
    w("-- comparable. The default is therefore jurisdiction-restricted access,")
    w("-- with cross-border read as an explicit grant carrying a legal basis.")
    w("-- ---------------------------------------------------------------------")
    w("CREATE OR REPLACE FUNCTION filter_by_country(country STRING)")
    w("  RETURNS BOOLEAN")
    w("  COMMENT 'Restrict rows to the reader\\'s jurisdiction unless globally cleared.'")
    w("  RETURN")
    w("    is_account_group_member('data_engineers')")
    w("    OR is_account_group_member('analysts_global')")
    w("    OR (is_account_group_member('analysts_th') AND country = 'TH')")
    w("    OR (is_account_group_member('analysts_id') AND country = 'ID');")
    w("")
    for schema, table in (
        ("silver", "silver_policies"),
        ("gold", "fct_claims"),
        ("gold", "fct_written_premium"),
        ("gold", "dim_customer"),
    ):
        w(f"ALTER TABLE {schema}.{table} SET ROW FILTER filter_by_country ON (country);")
    w("")

    # --------------------------------------------------------------- grants
    w("-- ---------------------------------------------------------------------")
    w("-- 5. Grants")
    w("--")
    w("-- Least privilege, and note what is NOT granted: analysts have no access")
    w("-- to silver at all. Silver holds unmasked PII, and the boundary between")
    w("-- silver and gold is the primary control. Masks are the second line.")
    w("-- ---------------------------------------------------------------------")
    w(f"GRANT USE CATALOG ON CATALOG {catalog} TO `data_engineers`;")
    w(f"GRANT USE CATALOG ON CATALOG {catalog} TO `analysts`;")
    w(f"GRANT USE CATALOG ON CATALOG {catalog} TO `restricted_health`;")
    w("")
    for schema in ("bronze", "silver", "gold", "marts", "observability"):
        w(f"GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA {schema} TO `data_engineers`;")
    w("")
    for schema in ("gold", "marts"):
        w(f"GRANT USE SCHEMA, SELECT ON SCHEMA {schema} TO `analysts`;")
    w("GRANT USE SCHEMA, SELECT ON SCHEMA observability TO `analysts`;")
    w("")
    w("-- restricted_health reads gold like any analyst; its privilege is that")
    w("-- mask_health returns the real value for it.")
    w("GRANT USE SCHEMA, SELECT ON SCHEMA gold TO `restricted_health`;")
    w("")
    w("-- Explicitly NOT granted, and the reason:")
    w("--   analysts -> silver           : unmasked PII lives there")
    w("--   analysts -> bronze           : raw partner files, unvalidated")
    w("--   restricted_health -> silver  : health access does not imply raw access")
    w("")

    # -------------------------------------------------------------- summary
    w("-- ---------------------------------------------------------------------")
    w("-- 6. Verification")
    w("--")
    w("-- Run these after applying. A mask that was never verified against a")
    w("-- real principal is an assumption, not a control.")
    w("-- ---------------------------------------------------------------------")
    w("-- SELECT * FROM information_schema.column_tags WHERE tag_name = 'pii';")
    w("-- DESCRIBE EXTENDED gold.dim_customer national_id_hash;  -- shows the mask")
    w("-- SELECT count(*) FROM gold.fct_claims;  -- as each analyst group")
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="insurance_dev")
    args = ap.parse_args()

    # Deduplicate on (schema, table, column). Bronze PII is declared in BOTH
    # config/sources.yml and the dbt sources file generated from it, so without
    # this every bronze column emits its tag and mask binding twice. The
    # statements are idempotent, but duplicated DDL in a governance artefact
    # reads as carelessness in exactly the review where it matters most.
    seen: set[tuple[str, str, str]] = set()
    columns: list[dict] = []
    for c in collect_from_dbt() + collect_from_sources():
        key = (c["schema"], c["table"], c["column"])
        if key in seen:
            continue
        seen.add(key)
        columns.append(c)

    OUT.write_text(render(columns, args.catalog), encoding="utf-8")

    by_type: dict[str, int] = {}
    for c in columns:
        by_type[c["pii_type"]] = by_type.get(c["pii_type"], 0) + 1
    print(f"wrote {OUT.relative_to(REPO_ROOT)}")
    print(
        f"  {len(columns)} classified columns across "
        f"{len({(c['schema'], c['table']) for c in columns})} tables"
    )
    for t, n in sorted(by_type.items(), key=lambda kv: -kv[1]):
        print(f"    {t:<14} {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
