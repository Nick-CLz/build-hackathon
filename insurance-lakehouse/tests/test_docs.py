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
