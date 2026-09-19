"""Phase 1 tests: determinism, checksum validity, and presence of every defect.

These are the tests that make the generator trustworthy as a fixture. If the
generator silently stopped emitting late claims, the whole late-arrival story in
silver would still "pass" while proving nothing -- so each defect gets asserted
explicitly.
"""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pytest

from data_generator.config import MESS_REGISTRY, GeneratorConfig, MessToggles
from data_generator.emit import emit_day, fmt_date
from data_generator.thai import is_valid_national_id, make_national_id
from data_generator.universe import build_universe

N = 1200  # small enough to keep the suite fast, large enough for rates to settle


def _gen(tmp_path: Path, days=(1, 2), **kw):
    cfg = GeneratorConfig(n_policies=N, out_root=str(tmp_path), **kw)
    u = build_universe(cfg)
    return u, {d: emit_day(u, d) for d in days}


def _digest(root: Path) -> dict[str, str]:
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


# --------------------------------------------------------------------- seed
def test_same_seed_produces_byte_identical_output(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    _gen(a)
    _gen(b)
    assert _digest(a) == _digest(b)


def test_different_seed_produces_different_output(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    _gen(a, seed=42)
    _gen(b, seed=43)
    assert _digest(a) != _digest(b)


def test_regenerating_one_day_is_idempotent(tmp_path):
    """Re-emitting a day must not append or drift -- bronze idempotency leans on this."""
    u, _ = _gen(tmp_path, days=(1,))
    first = _digest(tmp_path)
    emit_day(u, 1)
    assert _digest(tmp_path) == first


# ------------------------------------------------------------------ Thai PII
def test_national_id_checksum_round_trip():
    import random

    rng = random.Random(1)
    assert all(is_valid_national_id(make_national_id(rng, True)) for _ in range(500))
    assert not any(is_valid_national_id(make_national_id(rng, False)) for _ in range(500))


def test_national_id_validity_rate_is_mostly_valid_but_not_perfect(tmp_path):
    u, _ = _gen(tmp_path, days=(1,))
    th = [c for c in u.customers if c["country"] == "TH"]
    valid = [c for c in th if is_valid_national_id(c["national_id"])]
    rate = len(valid) / len(th)
    # Deliberately imperfect: the valid_thai_national_id dbt test must have
    # something to catch, but the bulk of the data must still be usable.
    assert 0.90 <= rate <= 0.99, rate


def test_thai_plate_and_phone_shapes(tmp_path):
    u, _ = _gen(tmp_path, days=(1,))
    th_vehicles = [v for v in u.vehicles if v["country"] == "TH"]
    assert th_vehicles
    for v in th_vehicles[:50]:
        lead, _, number = v["plate_no"].partition(" ")
        assert lead[0].isdigit() and len(lead) == 3, v["plate_no"]
        assert number.isdigit()
    phones = [c["phone"] for c in u.customers if c["country"] == "TH"]
    assert any(p.startswith("+66") for p in phones)
    assert any("-" in p for p in phones)


# ------------------------------------------------------------------- defects
def test_every_registered_defect_has_a_toggle():
    assert set(MESS_REGISTRY) == set(vars(MessToggles()))


def test_buddhist_era_dates_appear_in_thai_partner_files(tmp_path):
    _gen(tmp_path)
    rows = list(
        csv.DictReader(
            (tmp_path / "partner_drops/BKKSURE/dt=2026-01-05/policies_BKKSURE_20260105.csv").open(
                encoding="utf-8"
            )
        )
    )
    years = [r["EffectiveDate"][-4:] for r in rows]
    # Buddhist Era years sit ~543 ahead, so they land in the 25xx range.
    assert any(y.startswith("25") for y in years), years[:5]


def test_mixed_date_formats_within_one_column(tmp_path):
    _gen(tmp_path)
    rows = list(
        csv.DictReader(
            (
                tmp_path / "partner_drops/SIAMGUARD/dt=2026-01-05/policies_SIAMGUARD_20260105.csv"
            ).open(encoding="utf-8")
        )
    )
    shapes = {("iso" if v[4:5] == "-" else "dmy") for v in (r["start_date"] for r in rows)}
    assert len(shapes) > 1, shapes


def test_schema_drift_adds_a_column_on_day_two(tmp_path):
    _gen(tmp_path)

    def header(day_dir, name):
        p = tmp_path / f"partner_drops/SIAMGUARD/dt={day_dir}/policies_SIAMGUARD_{name}.csv"
        return next(csv.reader(p.open(encoding="utf-8")))

    d1, d2 = header("2026-01-05", "20260105"), header("2026-01-06", "20260106")
    assert set(d2) - set(d1) == {"distribution_channel"}


def test_exact_and_near_duplicates_present_on_day_two(tmp_path):
    """A duplicate spans *files*, not rows within one file.

    The partner resends a record it already sent yesterday, so the same
    policy_no shows up in both daily drops. An exact duplicate is byte-identical
    on the business columns; a near duplicate carries a corrected premium and a
    newer timestamp, which is what forces silver's tie-break rule to be explicit.
    """
    _, s = _gen(tmp_path)
    assert s[2]["defects"]["exact_duplicates"] > 0
    assert s[2]["defects"]["near_duplicates"] > 0

    def rows(dt, stamp):
        p = tmp_path / f"partner_drops/SIAMGUARD/dt={dt}/policies_SIAMGUARD_{stamp}.csv"
        return list(csv.DictReader(p.open(encoding="utf-8")))

    d1, d2 = rows("2026-01-05", "20260105"), rows("2026-01-06", "20260106")
    resent = {r["policy_no"] for r in d1} & {r["policy_no"] for r in d2}
    assert resent, "expected day 2 to resend policy numbers already sent on day 1"

    business_cols = ["policy_no", "premium_amt", "sum_insured", "policy_status"]

    def sig(r):
        return tuple(r[c] for c in business_cols)

    d1_sigs = {sig(r) for r in d1}
    d2_resent = [r for r in d2 if r["policy_no"] in resent]
    assert any(sig(r) in d1_sigs for r in d2_resent), "expected an exact duplicate"
    assert any(sig(r) not in d1_sigs for r in d2_resent), "expected a near duplicate"

    # A near duplicate must be distinguishable by recency, or dedup is a coin toss.
    by_policy: dict[str, list[dict]] = {}
    for r in d1 + d2:
        by_policy.setdefault(r["policy_no"], []).append(r)
    changed = [
        v for v in by_policy.values() if len(v) > 1 and len({x["premium_amt"] for x in v}) > 1
    ]
    assert changed, "expected at least one corrected premium"
    for versions in changed:
        assert len({x["last_updated_at"] for x in versions}) > 1


def test_late_claims_arrive_after_their_batch(tmp_path):
    u, s = _gen(tmp_path)
    assert s[2]["defects"]["late_claims"] > 0
    late = [c for c in u.claims if c["is_late_reported"]]
    assert late and all(c["reporting_lag_days"] >= 21 for c in late)
    # The defining property: the claim is emitted in a batch later than the one
    # its loss date belongs to, which is what the lookback window must absorb.
    assert all(c["emit_day"] > 1 for c in late)


def test_backdated_endorsements_precede_their_request(tmp_path):
    u, _ = _gen(tmp_path)
    back = [e for e in u.endorsements if e["is_backdated"]]
    assert back
    assert all(e["effective_date"] < e["requested_date"] for e in back)


def test_cancellations_produce_negative_refunds(tmp_path):
    u, _ = _gen(tmp_path)
    refunds = [p for p in u.payments if p["is_refund"]]
    assert refunds
    assert all(r["amount"] < 0 for r in refunds)
    cancelled = {p["policy_id"] for p in u.policies if p["status"] == "CANCELLED"}
    assert {r["policy_id"] for r in refunds} <= cancelled


def test_validation_failures_are_injected(tmp_path):
    _gen(tmp_path)
    neg = 0
    for p in tmp_path.rglob("partner_drops/**/policies_*.csv"):
        for r in csv.DictReader(p.open(encoding="utf-8")):
            amt = r.get("premium_amt") or r.get("GrossPremium") or r.get("premi")
            if amt and float(amt) < 0:
                neg += 1
    assert neg > 0, "expected some negative premiums to quarantine"


def test_cdc_feed_has_all_three_ops(tmp_path):
    _, s = _gen(tmp_path)
    ops = s[2]["defects"]["cdc_ops"]
    assert ops["I"] > 0 and ops["U"] > 0 and ops["D"] > 0


def test_mixed_currencies_need_conversion(tmp_path):
    u, _ = _gen(tmp_path)
    assert {p["currency"] for p in u.policies} == {"THB", "IDR"}
    assert {(f["from_currency"], f["to_currency"]) for f in u.fx_rates} == {
        ("IDR", "THB"),
        ("THB", "THB"),
    }


def test_toggles_off_suppresses_defects(tmp_path):
    """The whole point of the toggles: isolate one behaviour at a time."""
    cfg = GeneratorConfig(n_policies=N, out_root=str(tmp_path), mess=MessToggles.all_off())
    u = build_universe(cfg)
    emit_day(u, 1)
    s2 = emit_day(u, 2)
    assert s2["defects"]["exact_duplicates"] == 0
    assert s2["defects"]["near_duplicates"] == 0
    p = tmp_path / "partner_drops/BKKSURE/dt=2026-01-05/policies_BKKSURE_20260105.csv"
    rows = list(csv.DictReader(p.open(encoding="utf-8")))
    assert all(r["EffectiveDate"][:2] == "20" for r in rows), "BE dates should be off"


# ------------------------------------------------------- referential integrity
def test_referential_integrity_of_the_universe(tmp_path):
    u, _ = _gen(tmp_path, days=(1,))
    customers = {c["customer_id"] for c in u.customers}
    vehicles = {v["vehicle_id"] for v in u.vehicles}
    policies = {p["policy_id"] for p in u.policies}
    products = {p["product_code"] for p in u.products}
    partners = {p["partner_code"] for p in u.partners}
    for p in u.policies:
        assert p["customer_id"] in customers
        assert p["product_code"] in products
        assert p["partner_code"] in partners
        assert p["vehicle_id"] is None or p["vehicle_id"] in vehicles
    for c in u.claims:
        assert c["policy_id"] in policies
    for e in u.endorsements:
        assert e["policy_id"] in policies


def test_claims_fall_within_coverage_before_defect_injection(tmp_path):
    """Orphans and out-of-cover losses are injected at *emit* time, not in the
    universe -- so the source of truth stays clean and the defects stay countable."""
    u, _ = _gen(tmp_path, days=(1,))
    by_id = {p["policy_id"]: p for p in u.policies}
    for c in u.claims:
        pol = by_id[c["policy_id"]]
        assert pol["inception_date"] <= c["loss_date"] <= pol["expiry_date"]


def test_jsonl_events_are_valid_json(tmp_path):
    _gen(tmp_path)
    files = list(tmp_path.rglob("*.jsonl"))
    assert files
    for f in files:
        for line in f.read_text(encoding="utf-8").splitlines():
            json.loads(line)


@pytest.mark.parametrize(
    ("style", "expected"),
    [
        ("iso", "2026-01-05"),
        ("dmy", "05/01/2026"),
        ("dmon", "5 Jan 2026"),
        ("be_iso", "2569-01-05"),
        ("be_dmy", "05/01/2569"),
    ],
)
def test_date_surface_forms(style, expected):
    from datetime import date

    assert fmt_date(date(2026, 1, 5), style) == expected
