"""Guard the generated parts of the docs against drift."""

from pathlib import Path

from data_generator.config import MESS_REGISTRY

REPO_ROOT = Path(__file__).resolve().parents[1]


def _primer() -> str:
    return (REPO_ROOT / "docs" / "insurance_domain_primer.md").read_text(encoding="utf-8")


def test_defect_table_matches_registry():
    import sys

    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    from sync_doc_tables import apply

    current = _primer()
    assert current == apply(current), (
        "docs/insurance_domain_primer.md is stale -- run `make docs-sync`"
    )


def test_every_defect_is_documented():
    text = _primer()
    for code in MESS_REGISTRY:
        assert f"`{code}`" in text, f"{code} missing from the primer"


def test_dbt_sources_yml_is_in_sync():
    """config/sources.yml is the single registry; dbt's copy is generated from it.

    If this fails, someone edited config/sources.yml without running
    `make dbt-sources`, and dbt's freshness SLAs no longer match the ones the
    ingestion engine enforces.
    """
    import sys

    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    from generate_dbt_sources import OUT, render

    assert OUT.exists(), "dbt/models/sources.yml missing -- run `make dbt-sources`"
    assert OUT.read_text(encoding="utf-8") == render(), (
        "dbt/models/sources.yml is stale -- run `make dbt-sources`"
    )


def test_every_source_with_pii_is_tagged_in_dbt_sources():
    """PII declared for bronze must reach dbt's meta, or governance SQL misses it."""
    import yaml

    from ingestion.config import load_registry

    registry = load_registry()
    doc = yaml.safe_load((REPO_ROOT / "dbt" / "models" / "sources.yml").read_text(encoding="utf-8"))
    tables = {t["name"]: t for t in doc["sources"][0]["tables"]}
    for source in registry.sources:
        for pii in source.pii_columns:
            cols = {c["name"]: c for c in tables[source.name].get("columns", [])}
            assert pii.column in cols, f"{source.name}.{pii.column} not tagged in dbt sources"
            meta = cols[pii.column]["meta"]
            assert meta["pii"] is True
            assert meta["pii_type"] == pii.pii_type
            assert meta["sensitivity"] == pii.sensitivity
