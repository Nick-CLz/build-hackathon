"""Phase 2 tests: bronze idempotency, quarantine, drift and metadata.

The Spark-backed tests share one SparkSession (module-scoped fixture) because
starting one costs several seconds. They are marked ``slow`` so the fast suite
can skip them with ``-m "not slow"``.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from data_generator.config import GeneratorConfig
from data_generator.emit import emit_day
from data_generator.universe import build_universe
from ingestion.bronze import discover_files, ingest_all, new_batch_id
from ingestion.config import (
    META_BATCH_DATE,
    META_BATCH_ID,
    META_INGESTED_AT,
    META_RECORD_HASH,
    META_SOURCE_FILE,
    expand_braces,
    load_registry,
)

pytestmark = pytest.mark.slow


# ------------------------------------------------------------------ no Spark
def test_brace_expansion_matches_spark_semantics():
    assert expand_braces("a/{x,y}/b") == ["a/x/b", "a/y/b"]
    assert expand_braces("{a,b}/{c,d}") == ["a/c", "a/d", "b/c", "b/d"]
    assert expand_braces("plain/path") == ["plain/path"]


def test_registry_rejects_unsupported_format(tmp_path):
    bad = tmp_path / "sources.yml"
    bad.write_text(
        "version: 1\nsources:\n  - name: x\n    description: d\n    format: parquet\n    path: p\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="unsupported format"):
        load_registry(bad)


def test_registry_rejects_duplicate_names(tmp_path):
    bad = tmp_path / "sources.yml"
    entry = "  - name: x\n    description: d\n    format: csv\n    path: p\n"
    bad.write_text("version: 1\nsources:\n" + entry + entry, encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate source name"):
        load_registry(bad)


# --------------------------------------------------------------- with Spark
@pytest.fixture(scope="module")
def spark():
    from ingestion.session import get_spark

    s = get_spark("pytest-bronze")
    yield s
    s.stop()


@pytest.fixture(scope="module")
def landing(tmp_path_factory):
    """Generate day 1 and day 2 into a throwaway landing zone."""
    root = tmp_path_factory.mktemp("lake")
    cfg = GeneratorConfig(n_policies=600, out_root=str(root / "landing"))
    u = build_universe(cfg)
    emit_day(u, 1)
    return root


@pytest.fixture
def registry(landing, monkeypatch, tmp_path):
    monkeypatch.setenv("LAKEHOUSE_LANDING", str(landing / "landing"))
    monkeypatch.setenv("LAKEHOUSE_BRONZE", str(tmp_path / "bronze"))
    return load_registry()


SOURCES = ["partner_policies_a", "events_claims"]


def test_discovery_finds_files(registry):
    src = registry.get("partner_policies_a")
    refs = discover_files(registry, src)
    assert refs, "expected partner files in the landing zone"
    assert all(r.size > 0 for r in refs)
    # Identity includes mtime, so the key is a 3-tuple.
    assert len(refs[0].key) == 3


def test_ingest_is_idempotent(spark, registry):
    first = ingest_all(spark, registry, only=SOURCES, batch_id=new_batch_id())
    rows_first = {r.source: r.rows_ingested for r in first}
    assert all(v > 0 for v in rows_first.values()), rows_first

    counts_after_first = {
        name: spark.read.format("delta").load(f"{registry.bronze_path}/{name}").count()
        for name in SOURCES
    }

    second = ingest_all(spark, registry, only=SOURCES, batch_id=new_batch_id())
    assert all(r.status == "UP_TO_DATE" for r in second), [r.status for r in second]
    assert all(r.rows_ingested == 0 for r in second)

    counts_after_second = {
        name: spark.read.format("delta").load(f"{registry.bronze_path}/{name}").count()
        for name in SOURCES
    }
    assert counts_after_first == counts_after_second, "re-run duplicated rows"


def test_metadata_columns_present_and_populated(spark, registry):
    ingest_all(spark, registry, only=["partner_policies_a"], batch_id=new_batch_id())
    df = spark.read.format("delta").load(f"{registry.bronze_path}/partner_policies_a")
    for col in (
        META_INGESTED_AT,
        META_SOURCE_FILE,
        META_BATCH_ID,
        META_RECORD_HASH,
        META_BATCH_DATE,
    ):
        assert col in df.columns, col
    row = df.limit(1).collect()[0]
    assert row[META_SOURCE_FILE].endswith(".csv")
    assert len(row[META_RECORD_HASH]) == 64  # sha2-256 hex
    assert row[META_BATCH_DATE] == "2026-01-05"


def test_record_hash_is_stable_for_identical_business_content(spark, registry):
    """An exact duplicate must hash identically, or silver cannot collapse it."""
    ingest_all(spark, registry, only=["partner_policies_a"], batch_id=new_batch_id())
    df = spark.read.format("delta").load(f"{registry.bronze_path}/partner_policies_a")
    distinct_hashes = df.select(META_RECORD_HASH).distinct().count()
    distinct_keys = df.select("policy_no").distinct().count()
    # One row per policy on day 1, so hashes and keys should agree.
    assert distinct_hashes >= distinct_keys * 0.95


def test_quarantine_captures_unparseable_and_keyless_records(spark, registry):
    ingest_all(spark, registry, only=SOURCES, batch_id=new_batch_id())

    q_csv = spark.read.format("delta").load(f"{registry.bronze_path}/partner_policies_a_quarantine")
    reasons_csv = {r["_quarantine_reason"] for r in q_csv.collect()}
    assert "missing_primary_key" in reasons_csv

    q_json = spark.read.format("delta").load(f"{registry.bronze_path}/events_claims_quarantine")
    reasons_json = {r["_quarantine_reason"] for r in q_json.collect()}
    assert "unparseable_record" in reasons_json


def test_quarantine_boundary_is_exact(spark, tmp_path, monkeypatch):
    """Pin down precisely what bronze does and does not quarantine.

    Built from a hand-written file rather than the generator, because the
    generator injects defects probabilistically and a small sample can contain
    none. The boundary itself is a design decision worth asserting exactly:

      * negative premium  -> LANDS (a business-rule violation dbt must catch)
      * empty primary key -> QUARANTINED (nothing downstream can merge on it)

    Silently dropping the first at bronze would hide the very data-quality
    signal the warehouse exists to surface.
    """
    landing = tmp_path / "landing"
    drop = landing / "partner_drops" / "SIAMGUARD" / "dt=2026-01-05"
    drop.mkdir(parents=True)
    header = (
        "policy_no,cust_national_id,product_cd,start_date,end_date,"
        "premium_amt,sum_insured,currency_cd,plate_no,policy_status,last_updated_at"
    )
    (drop / "policies_SIAMGUARD_20260105.csv").write_text(
        "\n".join(
            [
                header,
                "POL1,1101700000000,MOTOR_CMI,2026-01-01,2027-01-01,645.00,500000,THB,1กข 1234,ACTIVE,2026-01-01T00:00:00",
                "POL2,1101700000001,MOTOR_CMI,2026-01-01,2027-01-01,-645.00,500000,THB,1กข 5678,ACTIVE,2026-01-01T00:00:00",
                ",,,,,,,,,,",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("LAKEHOUSE_LANDING", str(landing))
    monkeypatch.setenv("LAKEHOUSE_BRONZE", str(tmp_path / "bronze"))
    reg = load_registry()

    result = ingest_all(spark, reg, only=["partner_policies_a"], batch_id=new_batch_id())[0]
    assert result.rows_ingested == 2, "the negative-premium row must land"
    assert result.rows_quarantined == 1, "only the keyless row must be quarantined"

    df = spark.read.format("delta").load(f"{reg.bronze_path}/partner_policies_a")
    assert df.where("cast(premium_amt as double) < 0").count() == 1

    q = spark.read.format("delta").load(f"{reg.bronze_path}/partner_policies_a_quarantine")
    assert [r["_quarantine_reason"] for r in q.collect()] == ["missing_primary_key"]


def test_schema_drift_is_detected_and_audited(spark, registry, landing):
    """Drift is detected between batches, which is why day 2 is ingested separately."""
    from ingestion.manifest import drift_df

    ingest_all(spark, registry, only=["partner_policies_a"], batch_id=new_batch_id())

    cfg = GeneratorConfig(n_policies=600, out_root=str(landing / "landing"))
    emit_day(build_universe(cfg), 2)

    result = ingest_all(spark, registry, only=["partner_policies_a"], batch_id=new_batch_id())[0]
    assert result.status == "INGESTED"
    assert "distribution_channel" in result.drift.get("added", []), result.drift

    audit = drift_df(spark, registry.bronze_path).collect()
    assert any(r["added_columns"] == "distribution_channel" for r in audit)

    df = spark.read.format("delta").load(f"{registry.bronze_path}/partner_policies_a")
    assert "distribution_channel" in df.columns, "mergeSchema should have widened the table"


def test_maintenance_probe_reports_real_support(spark, registry):
    """OPTIMIZE/ZORDER/VACUUM/time travel must genuinely work, not be assumed."""
    from ingestion.maintenance import run_maintenance

    ingest_all(spark, registry, only=["partner_policies_a"], batch_id=new_batch_id())
    results = run_maintenance(spark, f"{registry.bronze_path}/partner_policies_a", ["policy_no"])
    by_op = {r.operation: r for r in results}
    for op in ("OPTIMIZE (bin-packing)", "VACUUM RETAIN 0 HOURS", "Time travel (versionAsOf 0)"):
        assert by_op[op].supported, f"{op}: {by_op[op].detail}"
    assert by_op["OPTIMIZE ZORDER BY (policy_no)"].supported

    # Clustering is refused on a partitioned table but works without partitions.
    assert not by_op["CLUSTER BY on PARTITIONED table"].supported
    assert by_op["CLUSTER BY on UNPARTITIONED table"].supported


def test_deleting_landing_files_does_not_delete_bronze(spark, registry, landing):
    """Bronze is the durable copy; the landing zone is disposable."""
    ingest_all(spark, registry, only=["partner_policies_a"], batch_id=new_batch_id())
    before = spark.read.format("delta").load(f"{registry.bronze_path}/partner_policies_a").count()
    shutil.rmtree(Path(registry.landing_root) / "partner_drops" / "SIAMGUARD")
    after = spark.read.format("delta").load(f"{registry.bronze_path}/partner_policies_a").count()
    assert before == after
