"""Write the universe out as the messy integration surfaces a real insurer sees.

Three output shapes, deliberately inconsistent with each other:

* ``partner_drops/`` -- daily CSV files, one directory per partner insurer, each
  partner using its own column names, date conventions and language.
* ``events/``        -- internal microservice emissions as JSON lines, clean-ish.
* ``cdc/``           -- a Debezium-style change feed for policies with op/ts.

Reference data lands as CSV snapshots under ``reference/``.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import random
from datetime import date, timedelta
from typing import Any

from .config import GeneratorConfig
from .universe import Universe

BE_OFFSET = 543
THAI_MONTH_ABBR = [
    "ม.ค.",
    "ก.พ.",
    "มี.ค.",
    "เม.ย.",
    "พ.ค.",
    "มิ.ย.",
    "ก.ค.",
    "ส.ค.",
    "ก.ย.",
    "ต.ค.",
    "พ.ย.",
    "ธ.ค.",
]
EN_MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

STATUS_TH = {"ACTIVE": "มีผลบังคับ", "CANCELLED": "ยกเลิก", "EXPIRED": "หมดอายุ"}


# --------------------------------------------------------------------------
# date surface forms
# --------------------------------------------------------------------------
def fmt_date(d: date | None, style: str) -> str:
    """Render a date in one of the surface forms partners actually send."""
    if d is None:
        return ""
    if style == "iso":
        return d.isoformat()
    if style == "dmy":
        return f"{d.day:02d}/{d.month:02d}/{d.year}"
    if style == "dmon":
        return f"{d.day} {EN_MONTH_ABBR[d.month - 1]} {d.year}"
    if style == "be_iso":
        return f"{d.year + BE_OFFSET}-{d.month:02d}-{d.day:02d}"
    if style == "be_dmy":
        return f"{d.day:02d}/{d.month:02d}/{d.year + BE_OFFSET}"
    if style == "be_thai":
        return f"{d.day} {THAI_MONTH_ABBR[d.month - 1]} {d.year + BE_OFFSET}"
    raise ValueError(f"unknown date style {style!r}")


def _scuff(rng: random.Random, value: str, enabled: bool) -> str:
    """Add the whitespace/casing damage that survives most ETL tools."""
    if not enabled or value is None:
        return value
    r = rng.random()
    if r < 0.10:
        return f"  {value}"
    if r < 0.20:
        return f"{value}  "
    if r < 0.27:
        return value.lower()
    if r < 0.32:
        return value.upper()
    return value


def _row_hash(row: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest()[:16]


# --------------------------------------------------------------------------
# partner-specific schemas
# --------------------------------------------------------------------------
def _policy_row_variant_a(p: dict, rng: random.Random, m, drift: bool) -> dict:
    """Variant A: tidy snake_case English, ISO/DMY dates, THB."""
    style = rng.choice(["iso", "iso", "dmy"]) if m.mixed_date_formats else "iso"
    row = {
        "policy_no": p["policy_id"],
        "cust_national_id": p["national_id"],
        "product_cd": p["product_code"],
        "start_date": fmt_date(p["inception_date"], style),
        "end_date": fmt_date(p["expiry_date"], style),
        "premium_amt": f"{p['written_premium']:.2f}",
        "sum_insured": f"{p['sum_insured']:.2f}",
        "currency_cd": p["currency"],
        "plate_no": p["plate_no"] or "",
        "policy_status": _scuff(rng, p["status"], m.whitespace_casing),
        "last_updated_at": f"{p['updated_at']}T0{rng.randint(1, 9)}:00:00",
    }
    if drift:
        row["distribution_channel"] = rng.choice(["web", "agent", "bancassurance"])
    return row


def _policy_row_variant_b(p: dict, rng: random.Random, m, drift: bool) -> dict:
    """Variant B: Thai insurer, PascalCase, Buddhist Era years, Thai statuses."""
    style = rng.choice(["be_iso", "be_dmy", "be_thai"]) if m.buddhist_era_dates else "iso"
    status = p["status"]
    if m.mixed_language_text and rng.random() < 0.5:
        status = STATUS_TH.get(status, status)
    row = {
        "PolicyNumber": p["policy_id"],
        "IDCard": p["national_id"],
        "ProductCode": _scuff(rng, p["product_code"], m.whitespace_casing),
        "EffectiveDate": fmt_date(p["inception_date"], style),
        "ExpiryDate": fmt_date(p["expiry_date"], style),
        "GrossPremium": f"{p['written_premium']:.2f}",
        "SumInsured": f"{p['sum_insured']:.2f}",
        "Curr": p["currency"],
        "VehiclePlate": p["plate_no"] or "",
        "Status": status,
        "UpdatedTimestamp": f"{p['updated_at']} 0{rng.randint(1, 9)}:30:00",
    }
    if drift:
        row["AgentCode"] = f"AG{rng.randint(1000, 9999)}"
    return row


def _policy_row_variant_c(p: dict, rng: random.Random, m, drift: bool) -> dict:
    """Variant C: Indonesian partner, Bahasa column names, DMY dates, IDR."""
    style = rng.choice(["dmy", "dmon"]) if m.mixed_date_formats else "iso"
    row = {
        "nomor_polis": p["policy_id"],
        "no_ktp": p["national_id"],
        "kode_produk": p["product_code"],
        "tanggal_mulai": fmt_date(p["inception_date"], style),
        "tanggal_akhir": fmt_date(p["expiry_date"], style),
        "premi": f"{p['written_premium']:.2f}",
        "nilai_pertanggungan": f"{p['sum_insured']:.2f}",
        "mata_uang": p["currency"],
        "nomor_plat": p["plate_no"] or "",
        "status_polis": _scuff(rng, p["status"], m.whitespace_casing),
        "waktu_pembaruan": f"{p['updated_at']}T0{rng.randint(1, 9)}:15:00Z",
    }
    if drift:
        row["kanal_distribusi"] = rng.choice(["web", "agen", "mitra"])
    return row


POLICY_VARIANTS = {
    "A": _policy_row_variant_a,
    "B": _policy_row_variant_b,
    "C": _policy_row_variant_c,
}
CLAIM_KEY_VARIANTS = {
    "A": {
        "claim_no": "claim_no",
        "policy_no": "policy_no",
        "loss_dt": "loss_date",
        "reported_dt": "reported_date",
        "amount": "incurred_amt",
        "ccy": "currency_cd",
        "cause": "cause_of_loss",
        "status": "claim_status",
    },
    "B": {
        "claim_no": "ClaimNumber",
        "policy_no": "PolicyNumber",
        "loss_dt": "LossDate",
        "reported_dt": "NotifiedDate",
        "amount": "IncurredAmount",
        "ccy": "Curr",
        "cause": "CauseOfLoss",
        "status": "ClaimStatus",
    },
    "C": {
        "claim_no": "nomor_klaim",
        "policy_no": "nomor_polis",
        "loss_dt": "tanggal_kejadian",
        "reported_dt": "tanggal_lapor",
        "amount": "jumlah_klaim",
        "ccy": "mata_uang",
        "cause": "penyebab",
        "status": "status_klaim",
    },
}


def _claim_row(c: dict, variant: str, rng: random.Random, m) -> dict:
    k = CLAIM_KEY_VARIANTS[variant]
    if variant == "B":
        style = rng.choice(["be_iso", "be_dmy"]) if m.buddhist_era_dates else "iso"
    elif variant == "C":
        style = rng.choice(["dmy", "dmon"]) if m.mixed_date_formats else "iso"
    else:
        style = rng.choice(["iso", "dmy"]) if m.mixed_date_formats else "iso"
    return {
        k["claim_no"]: c["claim_no"],
        k["policy_no"]: c["policy_id"],
        k["loss_dt"]: fmt_date(c["loss_date"], style),
        k["reported_dt"]: fmt_date(c["reported_date"], style),
        k["amount"]: f"{c['gross_incurred']:.2f}",
        k["ccy"]: c["currency"],
        k["cause"]: _scuff(rng, c["claim_cause"], m.whitespace_casing),
        k["status"]: c["status"],
    }


# --------------------------------------------------------------------------
# writers
# --------------------------------------------------------------------------
def _write_csv(path: str, rows: list[dict]) -> None:
    if not rows:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Union of keys preserves the drifted column even when only later rows have it.
    fieldnames: list[str] = []
    for r in rows:
        for key in r:
            if key not in fieldnames:
                fieldnames.append(key)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _write_jsonl(path: str, rows: list[dict]) -> None:
    if not rows:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, default=str, ensure_ascii=False) + "\n")


def _partner_policy_rows(u: Universe, day: int, partner: dict, rng: random.Random) -> list[dict]:
    """Policies this partner reports on ``day``, in that partner's own schema."""
    m = u.cfg.mess
    drift = m.schema_drift and day >= 2
    builder = POLICY_VARIANTS[partner["schema_variant"]]
    rows = []
    for p in u.policies:
        if p["partner_code"] != partner["partner_code"] or p["emit_day"] != day:
            continue
        pol = dict(p)
        # Deliberate validation failures, injected on a small slice.
        if m.validation_failures:
            r = rng.random()
            if r < 0.004:
                pol["written_premium"] = -abs(pol["written_premium"])
            elif r < 0.007:
                pol["national_id"] = "0000000000000"  # fails mod-11
        rows.append(builder(pol, rng, m, drift))
    return rows


def emit_day(u: Universe, day: int, out_root: str | None = None) -> dict[str, Any]:
    """Write every file for ``day``. Returns a summary used by tests and the CLI."""
    cfg: GeneratorConfig = u.cfg
    m = cfg.mess
    root = out_root or cfg.out_root
    bd = cfg.batch_date(day)
    dt = bd.isoformat()
    stamp = bd.strftime("%Y%m%d")
    # Seeded per-day so a given day is reproducible in isolation.
    rng = random.Random(cfg.seed * 1000 + day)
    summary: dict[str, Any] = {"day": day, "batch_date": dt, "files": {}, "defects": {}}

    def record(path: str, rows: list[dict]) -> None:
        summary["files"][os.path.relpath(path, root)] = len(rows)

    # ---- partner CSV drops -------------------------------------------
    dup_exact = dup_near = 0
    for partner in u.partners:
        code = partner["partner_code"]
        variant = partner["schema_variant"]
        rows = _partner_policy_rows(u, day, partner, rng)

        if day >= 2:
            prev = _partner_policy_rows(
                u, day - 1, partner, random.Random(cfg.seed * 1000 + day - 1)
            )
            if m.exact_duplicates and prev:
                k = max(1, int(len(prev) * 0.02))
                picked = rng.sample(prev, min(k, len(prev)))
                rows.extend(picked)
                dup_exact += len(picked)
            if m.near_duplicates and prev:
                k = max(1, int(len(prev) * 0.015))
                for src in rng.sample(prev, min(k, len(prev))):
                    near = dict(src)
                    # A correction: premium restated, timestamp moves forward.
                    for amt_key in ("premium_amt", "GrossPremium", "premi"):
                        if amt_key in near:
                            near[amt_key] = f"{float(near[amt_key]) * 1.05:.2f}"
                    for ts_key in ("last_updated_at", "UpdatedTimestamp", "waktu_pembaruan"):
                        if ts_key in near:
                            near[ts_key] = f"{dt}T23:59:00"
                    rows.append(near)
                    dup_near += 1

        p = f"{root}/partner_drops/{code}/dt={dt}/policies_{code}_{stamp}.csv"
        _write_csv(p, rows)
        record(p, rows)

        claims = _partner_claims(u, day, code)
        crows = []
        for c in claims:
            cc = dict(c)
            if m.validation_failures and rng.random() < 0.01:
                # Loss date pushed outside the coverage period.
                cc["loss_date"] = cc["loss_date"] - timedelta(days=rng.randint(400, 800))
            if m.validation_failures and rng.random() < 0.005:
                cc["policy_id"] = f"POL9{rng.randint(1000000, 9999999)}"  # orphan FK
            crows.append(_claim_row(cc, variant, rng, m))
        p = f"{root}/partner_drops/{code}/dt={dt}/claims_{code}_{stamp}.csv"
        _write_csv(p, crows)
        record(p, crows)

    summary["defects"]["exact_duplicates"] = dup_exact
    summary["defects"]["near_duplicates"] = dup_near

    # ---- internal microservice events (JSON lines) --------------------
    quotes = [q for q in u.quotes if q["emit_day"] == day]
    p = f"{root}/events/quotes/dt={dt}/quotes_{stamp}.jsonl"
    _write_jsonl(
        p,
        [
            {
                "event_id": f"EVQ-{q['quote_id']}",
                "event_type": "quote.created",
                "emitted_at": f"{dt}T02:00:00Z",
                **q,
            }
            for q in quotes
        ],
    )
    record(p, quotes)

    ends = [e for e in u.endorsements if e["emit_day"] == day]
    p = f"{root}/events/endorsements/dt={dt}/endorsements_{stamp}.jsonl"
    _write_jsonl(
        p,
        [
            {
                "event_id": f"EVE-{e['endorsement_id']}",
                "event_type": "policy.endorsed",
                "emitted_at": f"{dt}T02:05:00Z",
                **e,
            }
            for e in ends
        ],
    )
    record(p, ends)
    summary["defects"]["backdated_endorsements"] = len([e for e in ends if e["is_backdated"]])

    pays = [x for x in u.payments if x["emit_day"] == day]
    p = f"{root}/events/premium_payments/dt={dt}/payments_{stamp}.jsonl"
    _write_jsonl(
        p,
        [
            {
                "event_id": f"EVP-{x['payment_id']}",
                "event_type": "premium.payment",
                "emitted_at": f"{dt}T02:10:00Z",
                **x,
            }
            for x in pays
        ],
    )
    record(p, pays)
    summary["defects"]["cancellations_refunds"] = len([x for x in pays if x["is_refund"]])

    clm = [c for c in u.claims if c["emit_day"] == day]
    p = f"{root}/events/claims/dt={dt}/claims_{stamp}.jsonl"
    _write_jsonl(
        p,
        [
            {
                "event_id": f"EVC-{c['claim_id']}",
                "event_type": "claim.reported",
                "emitted_at": f"{dt}T02:15:00Z",
                **c,
            }
            for c in clm
        ],
    )
    record(p, clm)
    summary["defects"]["late_claims"] = len([c for c in clm if c["is_late_reported"]])

    # ---- CDC change feed for policies ---------------------------------
    cdc = []
    for pol in u.policies:
        if pol["emit_day"] == day:
            cdc.append(_cdc(pol, "I", f"{dt}T01:00:00Z"))
    for e in ends:
        pol = next((x for x in u.policies if x["policy_id"] == e["policy_id"]), None)
        if pol:
            upd = dict(pol)
            upd["sum_insured"] = e["new_sum_insured"]
            upd["updated_at"] = e["requested_date"]
            cdc.append(_cdc(upd, "U", f"{dt}T01:30:00Z"))
    for pol in u.policies:
        if pol.get("cancellation_date") and pol["emit_day"] == max(1, day - 1):
            cdc.append(_cdc(pol, "U", f"{dt}T01:45:00Z"))
    if m.validation_failures and day >= 2 and u.policies:
        victim = u.policies[rng.randrange(len(u.policies))]
        cdc.append(_cdc(victim, "D", f"{dt}T01:50:00Z"))
    p = f"{root}/cdc/policies/dt={dt}/policies_cdc_{stamp}.jsonl"
    _write_jsonl(p, cdc)
    record(p, cdc)
    summary["defects"]["cdc_ops"] = {op: len([c for c in cdc if c["op"] == op]) for op in "IUD"}

    # ---- reference snapshots ------------------------------------------
    refs = {
        "products": [dict(x) for x in u.products],
        "partner_insurers": [dict(x) for x in u.partners],
        "fx_rates": [x for x in u.fx_rates if x["rate_date"] <= bd],
    }
    if day == 1:
        refs["customers"] = [_public_customer(c) for c in u.customers]
        refs["vehicles"] = [dict(v) for v in u.vehicles]
    for name, rows in refs.items():
        p = f"{root}/reference/{name}/dt={dt}/{name}_{stamp}.csv"
        _write_csv(p, [{k: ("" if v is None else v) for k, v in r.items()} for r in rows])
        record(p, rows)

    summary["defects"]["mixed_currencies"] = len({p["currency"] for p in u.policies})
    summary["total_rows"] = sum(summary["files"].values())
    return summary


def _partner_claims(u: Universe, day: int, partner_code: str) -> list[dict]:
    policy_partner = {p["policy_id"]: p["partner_code"] for p in u.policies}
    return [
        c
        for c in u.claims
        if c["emit_day"] == day and policy_partner.get(c["policy_id"]) == partner_code
    ]


def _public_customer(c: dict) -> dict:
    out = {k: v for k, v in c.items() if k not in {"national_id_valid", "emit_day"}}
    return out


def _cdc(pol: dict, op: str, ts: str) -> dict:
    return {
        "op": op,
        "ts": ts,
        "policy_id": pol["policy_id"],
        "customer_id": pol["customer_id"],
        "product_code": pol["product_code"],
        "partner_code": pol["partner_code"],
        "country": pol["country"],
        "currency": pol["currency"],
        "inception_date": str(pol["inception_date"]),
        "expiry_date": str(pol["expiry_date"]),
        "sum_insured": pol["sum_insured"],
        "written_premium": pol["written_premium"],
        "status": "CANCELLED" if op == "U" and pol.get("cancellation_date") else pol["status"],
        "vehicle_id": pol.get("vehicle_id"),
        "updated_at": str(pol.get("updated_at")),
        "_change_hash": _row_hash({k: pol.get(k) for k in ("policy_id", "sum_insured", "status")}),
    }
