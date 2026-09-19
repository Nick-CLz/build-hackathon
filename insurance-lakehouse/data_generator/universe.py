"""Deterministic construction of the whole synthetic insurance universe.

The generator does **not** keep state between runs. Instead it rebuilds the
entire universe from ``(seed, n_policies)`` every time and then *slices* it by
``emit_day``. Two consequences worth knowing:

* ``make data DAY=2`` cannot drift from ``make data DAY=1`` -- both are views
  over the same deterministic object graph, so referential integrity across
  daily drops is guaranteed by construction rather than by bookkeeping.
* Re-running any day is naturally idempotent, which is what lets the bronze
  manifest test mean something.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import date, timedelta

from faker import Faker

from .config import GeneratorConfig
from .thai import (
    BMI_BANDS,
    PRE_EXISTING_CONDITIONS,
    PROVINCES_EN,
    PROVINCES_TH,
    make_national_id,
    make_phone,
    make_plate,
    make_thai_name,
)

REPORTING_CURRENCY = "THB"
CURRENCY_BY_COUNTRY = {"TH": "THB", "ID": "IDR"}

# Partner insurers. ``schema_variant`` drives how differently their CSV drops are
# shaped -- this is the whole point of the partner-harmonisation layer in silver.
PARTNERS = [
    ("SIAMGUARD", "Siam Guard Insurance PCL", "TH", "A", 0.15),
    ("BKKSURE", "Bangkok Sure Assurance", "TH", "B", 0.12),
    ("THAIVIVAT", "Thai Vivat General", "TH", "A", 0.14),
    ("KRUNGSRI_GI", "Krungsri General Insurance", "TH", "B", 0.11),
    ("NUSANTARA", "PT Asuransi Nusantara", "ID", "C", 0.18),
    ("GARUDA_INS", "PT Garuda Insurance", "ID", "C", 0.16),
]

# product_code, en, th, line, cover, base premium (local ccy), default SI
PRODUCTS = [
    (
        "MOTOR_CMI",
        "Compulsory Motor Insurance",
        "ประกันภัยรถยนต์ภาคบังคับ (พ.ร.บ.)",
        "motor",
        "compulsory",
        645.0,
        500_000.0,
    ),
    (
        "MOTOR_VOL_C1",
        "Voluntary Motor Class 1",
        "ประกันภัยรถยนต์ชั้น 1",
        "motor",
        "voluntary",
        18_500.0,
        800_000.0,
    ),
    (
        "MOTOR_VOL_C2",
        "Voluntary Motor Class 2",
        "ประกันภัยรถยนต์ชั้น 2",
        "motor",
        "voluntary",
        9_800.0,
        400_000.0,
    ),
    (
        "MOTOR_VOL_C3",
        "Voluntary Motor Class 3",
        "ประกันภัยรถยนต์ชั้น 3",
        "motor",
        "voluntary",
        6_200.0,
        200_000.0,
    ),
    (
        "HEALTH_ADJ",
        "Adjustable Health Plan",
        "ประกันสุขภาพแบบปรับได้",
        "health",
        "health",
        24_000.0,
        1_000_000.0,
    ),
]
PRODUCT_WEIGHTS = [0.30, 0.22, 0.14, 0.16, 0.18]

VEHICLE_MAKES = [
    ("Toyota", ["Vios", "Yaris", "Fortuner", "Hilux Revo"]),
    ("Honda", ["City", "Civic", "CR-V", "Jazz"]),
    ("Isuzu", ["D-Max", "MU-X"]),
    ("Mitsubishi", ["Xpander", "Triton"]),
    ("Nissan", ["Almera", "Navara"]),
    ("Mazda", ["Mazda2", "CX-30"]),
]

CLAIM_CAUSES_MOTOR = [
    "collision",
    "theft",
    "flood",
    "fire",
    "third_party_property",
    "windscreen",
    "vandalism",
]
CLAIM_CAUSES_HEALTH = ["ipd_admission", "opd_visit", "surgery", "critical_illness"]

# Claim severity as a multiple of written premium, by product. Chosen so that
# multiple * claim_frequency lands each product in a plausible loss-ratio band.
# Compulsory motor runs hot on purpose: it is a thin-margin statutory product
# that insurers cross-subsidise, which is a real feature of the Thai market.
SEVERITY_MULTIPLE = {
    "MOTOR_CMI": (4.0, 14.0),  # statutory bodily-injury caps, thin premium
    "MOTOR_VOL_C1": (3.0, 11.0),  # own damage included: higher frequency, lower severity per baht
    "MOTOR_VOL_C2": (3.5, 12.0),
    "MOTOR_VOL_C3": (3.0, 10.0),  # third party only
    "HEALTH_ADJ": (2.5, 9.0),  # many small episodes
    "_default": (3.0, 11.0),
}


def _weighted(rng: random.Random, options, weights):
    return rng.choices(options, weights=weights, k=1)[0]


@dataclass
class Universe:
    cfg: GeneratorConfig
    partners: list[dict] = field(default_factory=list)
    products: list[dict] = field(default_factory=list)
    customers: list[dict] = field(default_factory=list)
    vehicles: list[dict] = field(default_factory=list)
    quotes: list[dict] = field(default_factory=list)
    policies: list[dict] = field(default_factory=list)
    endorsements: list[dict] = field(default_factory=list)
    payments: list[dict] = field(default_factory=list)
    claims: list[dict] = field(default_factory=list)
    fx_rates: list[dict] = field(default_factory=list)


def build_universe(cfg: GeneratorConfig) -> Universe:
    rng = random.Random(cfg.seed)
    fake_th = Faker("th_TH")
    fake_id = Faker("id_ID")
    Faker.seed(cfg.seed)

    u = Universe(cfg=cfg)
    horizon_start = cfg.base_date - timedelta(days=540)

    # --- reference data -------------------------------------------------
    u.partners = [
        {
            "partner_code": code,
            "partner_name": name,
            "country": country,
            "schema_variant": variant,
            "commission_rate": rate,
            "onboarded_date": horizon_start - timedelta(days=rng.randint(30, 900)),
        }
        for code, name, country, variant, rate in PARTNERS
    ]
    u.products = [
        {
            "product_code": c,
            "product_name_en": en,
            "product_name_th": th,
            "line_of_business": lob,
            "cover_type": cover,
            "base_premium_local": base,
            "default_sum_insured": si,
        }
        for c, en, th, lob, cover, base, si in PRODUCTS
    ]

    # Daily IDR->THB with a gentle random walk, plus the identity row for THB.
    rate = 0.00223
    d = horizon_start
    # The spine must extend well past base_date. A policy's inception date is its
    # quote date plus up to 21 days, so inceptions run past the last daily batch;
    # with a spine ending at base_date those policies found no rate and their
    # converted premium came out NULL. That NULL was caught by a not_null test
    # rather than silently passing an unconverted IDR amount through as THB,
    # which is the behaviour convert_currency is designed for -- but the spine
    # should cover the data regardless.
    fx_end = cfg.base_date + timedelta(days=cfg.max_days + 60)
    while d <= fx_end:
        rate *= 1 + rng.gauss(0, 0.004)
        u.fx_rates.append(
            {
                "rate_date": d,
                "from_currency": "IDR",
                "to_currency": "THB",
                "rate": round(rate, 8),
            }
        )
        u.fx_rates.append(
            {
                "rate_date": d,
                "from_currency": "THB",
                "to_currency": "THB",
                "rate": 1.0,
            }
        )
        d += timedelta(days=1)

    # --- customers ------------------------------------------------------
    n_customers = max(10, int(cfg.n_policies * 0.82))
    countries, cweights = zip(*cfg.country_weights, strict=True)
    for i in range(n_customers):
        country = _weighted(rng, list(countries), list(cweights))
        # ~4% of national IDs fail the checksum, mirroring real capture errors.
        valid_id = rng.random() > 0.04
        if country == "TH":
            given, family = make_thai_name(rng)
            nid = make_national_id(rng, valid=valid_id)
            pidx = rng.randrange(len(PROVINCES_TH))
            province = PROVINCES_TH[pidx] if rng.random() < 0.6 else PROVINCES_EN[pidx]
            address = fake_th.address().replace("\n", " ")
        else:
            given, family = fake_id.first_name(), fake_id.last_name()
            nid = "".join(str(rng.randint(0, 9)) for _ in range(16))  # Indonesian NIK
            province = rng.choice(["Jakarta", "Jawa Barat", "Bali", "Jawa Timur"])
            address = fake_id.address().replace("\n", " ")
        dob = date(rng.randint(1955, 2005), rng.randint(1, 12), rng.randint(1, 28))
        u.customers.append(
            {
                "customer_id": f"CUST{i + 1:07d}",
                "first_name": given,
                "last_name": family,
                "national_id": nid,
                "national_id_valid": valid_id,
                "date_of_birth": dob,
                "email": f"{given[:6].lower()}.{i + 1}@example.invalid".replace(" ", ""),
                "phone": make_phone(rng),
                "address": address,
                "province": province,
                "country": country,
                "created_at": horizon_start - timedelta(days=rng.randint(0, 400)),
                "emit_day": 1,
            }
        )

    # --- vehicles -------------------------------------------------------
    for i in range(int(n_customers * 0.75)):
        cust = u.customers[rng.randrange(n_customers)]
        make, models = rng.choice(VEHICLE_MAKES)
        plate, plate_province = make_plate(rng)
        if cust["country"] == "ID":
            plate = f"{rng.choice('BDLNF')} {rng.randint(1000, 9999)} {rng.choice('ABCDEFGHJK')}{rng.choice('ABCDEFGHJK')}"
            plate_province = cust["province"]
        u.vehicles.append(
            {
                "vehicle_id": f"VEH{i + 1:07d}",
                "customer_id": cust["customer_id"],
                "plate_no": plate,
                "plate_province": plate_province,
                "make": make,
                "model": rng.choice(models),
                "manufacture_year": rng.randint(2008, 2026),
                "engine_cc": rng.choice([1200, 1500, 1800, 2000, 2400, 2800]),
                "chassis_no": f"{rng.choice('MRJK')}{''.join(rng.choice('ABCDEFGHJKLMNPRSTUVWXYZ0123456789') for _ in range(16))}",
                "vehicle_value_local": float(rng.randrange(250, 2200) * 1000),
                "country": cust["country"],
                "emit_day": 1,
            }
        )
    vehicles_by_country: dict[str, list[dict]] = {}
    for v in u.vehicles:
        vehicles_by_country.setdefault(v["country"], []).append(v)

    # --- quotes and policies -------------------------------------------
    n_quotes = int(cfg.n_policies / cfg.quote_to_policy_rate)
    policy_seq = 0
    day_weights = [0.80, 0.14, 0.06][: cfg.max_days]

    for q in range(n_quotes):
        cust = u.customers[rng.randrange(n_customers)]
        country = cust["country"]
        currency = CURRENCY_BY_COUNTRY[country]
        partner = rng.choice([p for p in u.partners if p["country"] == country])
        product = _weighted(rng, u.products, PRODUCT_WEIGHTS)
        quoted_at = horizon_start + timedelta(
            days=rng.randint(0, (cfg.base_date - horizon_start).days)
        )
        fx = 1.0 if currency == "THB" else 450.0  # local-currency scaling for IDR
        premium = round(product["base_premium_local"] * rng.uniform(0.75, 1.45) * fx, 2)
        converted = rng.random() < cfg.quote_to_policy_rate
        quote_id = f"QT{q + 1:08d}"
        policy_id = None

        if converted and policy_seq < cfg.n_policies:
            policy_seq += 1
            policy_id = f"POL{policy_seq:08d}"
            inception = quoted_at + timedelta(days=rng.randint(0, 21))
            expiry = inception + timedelta(days=365)
            emit_day = _weighted(rng, list(range(1, len(day_weights) + 1)), day_weights)
            vehicle = None
            if product["line_of_business"] == "motor":
                pool = vehicles_by_country.get(country) or u.vehicles
                vehicle = pool[rng.randrange(len(pool))]
            si = product["default_sum_insured"] * rng.uniform(0.8, 1.6) * fx
            policy = {
                "policy_id": policy_id,
                "quote_id": quote_id,
                "customer_id": cust["customer_id"],
                "vehicle_id": vehicle["vehicle_id"] if vehicle else None,
                "plate_no": vehicle["plate_no"] if vehicle else None,
                "product_code": product["product_code"],
                "partner_code": partner["partner_code"],
                "country": country,
                "currency": currency,
                "inception_date": inception,
                "expiry_date": expiry,
                "sum_insured": round(si, 2),
                "written_premium": premium,
                "status": "ACTIVE",
                "national_id": cust["national_id"],
                "created_at": inception,
                "updated_at": inception,
                "emit_day": emit_day,
                "line_of_business": product["line_of_business"],
            }
            if product["line_of_business"] == "health":
                k = rng.choice([0, 1, 1, 2])
                policy["pre_existing_conditions"] = (
                    ";".join(rng.sample(PRE_EXISTING_CONDITIONS[:-1], k)) or "none"
                )
                policy["bmi_band"] = rng.choice(BMI_BANDS)
            u.policies.append(policy)

        u.quotes.append(
            {
                "quote_id": quote_id,
                "customer_id": cust["customer_id"],
                "product_code": product["product_code"],
                "partner_code": partner["partner_code"],
                "country": country,
                "currency": currency,
                "quoted_at": quoted_at,
                "quoted_premium": premium,
                "channel": rng.choice(["web", "agent", "broker", "telesales", "partner_api"]),
                "status": "CONVERTED"
                if policy_id
                else rng.choice(["EXPIRED", "REJECTED", "LAPSED"]),
                "converted_policy_id": policy_id,
                "emit_day": 1
                if not policy_id
                else next(p["emit_day"] for p in reversed(u.policies) if p["quote_id"] == quote_id),
            }
        )

    _build_policy_lifecycle(u, rng)
    return u


def _build_policy_lifecycle(u: Universe, rng: random.Random) -> None:
    """Endorsements, cancellations, premium instalments and claims."""
    cfg = u.cfg
    max_day = cfg.max_days
    end_seq = pay_seq = clm_seq = 0

    for pol in u.policies:
        # --- endorsements (including backdated ones) --------------------
        if rng.random() < cfg.endorsement_rate:
            end_seq += 1
            term_days = (pol["expiry_date"] - pol["inception_date"]).days
            eff = pol["inception_date"] + timedelta(days=rng.randint(15, max(16, term_days - 30)))
            backdated = rng.random() < 0.35
            requested = eff + timedelta(days=rng.randint(5, 45)) if backdated else eff
            etype = rng.choice(
                ["SUM_INSURED_CHANGE", "COVERAGE_CHANGE", "VEHICLE_CHANGE", "ADDRESS_CHANGE"]
            )
            old_si = pol["sum_insured"]
            new_si = (
                round(old_si * rng.uniform(1.05, 1.45), 2)
                if etype == "SUM_INSURED_CHANGE"
                else old_si
            )
            delta = round((new_si - old_si) * 0.012, 2)
            u.endorsements.append(
                {
                    "endorsement_id": f"END{end_seq:08d}",
                    "policy_id": pol["policy_id"],
                    "endorsement_type": etype,
                    "effective_date": eff,
                    "requested_date": requested,
                    "is_backdated": backdated,
                    "old_sum_insured": old_si,
                    "new_sum_insured": new_si,
                    "premium_delta": delta,
                    "currency": pol["currency"],
                    "emit_day": min(max_day, pol["emit_day"] + (1 if backdated else 0)),
                }
            )
            if etype == "SUM_INSURED_CHANGE":
                pol["endorsed_sum_insured"] = new_si
                pol["endorsed_effective_date"] = eff
                pol["endorsement_premium_delta"] = delta

        # --- cancellation with pro-rata refund --------------------------
        cancelled_on = None
        if rng.random() < cfg.cancellation_rate:
            term_days = (pol["expiry_date"] - pol["inception_date"]).days
            cancelled_on = pol["inception_date"] + timedelta(days=rng.randint(30, term_days - 1))
            if cancelled_on <= cfg.base_date:
                pol["status"] = "CANCELLED"
                pol["cancellation_date"] = cancelled_on
            else:
                cancelled_on = None
        elif pol["expiry_date"] < cfg.base_date:
            pol["status"] = "EXPIRED"

        # --- premium instalments ----------------------------------------
        n_inst = rng.choice([1, 1, 1, 2, 4, 12])
        per = round(pol["written_premium"] / n_inst, 2)
        for i in range(n_inst):
            due = pol["inception_date"] + timedelta(days=int(365 / n_inst * i))
            if due > cfg.base_date + timedelta(days=max_day):
                break
            pay_seq += 1
            paid = due + timedelta(days=rng.randint(-3, 12))
            u.payments.append(
                {
                    "payment_id": f"PAY{pay_seq:09d}",
                    "policy_id": pol["policy_id"],
                    "installment_no": i + 1,
                    "due_date": due,
                    "paid_date": paid if paid <= cfg.base_date else None,
                    "amount": per,
                    "currency": pol["currency"],
                    "payment_method": rng.choice(
                        ["credit_card", "bank_transfer", "promptpay", "cash", "qris"]
                    ),
                    "status": "PAID" if paid <= cfg.base_date else "PENDING",
                    "is_refund": False,
                    "emit_day": pol["emit_day"],
                }
            )
        if cancelled_on:
            pay_seq += 1
            unused = (pol["expiry_date"] - cancelled_on).days / 365.0
            u.payments.append(
                {
                    "payment_id": f"PAY{pay_seq:09d}",
                    "policy_id": pol["policy_id"],
                    "installment_no": 0,
                    "due_date": cancelled_on,
                    "paid_date": cancelled_on + timedelta(days=rng.randint(3, 20)),
                    "amount": -round(pol["written_premium"] * unused * 0.9, 2),
                    "currency": pol["currency"],
                    "payment_method": "refund",
                    "status": "REFUNDED",
                    "is_refund": True,
                    "emit_day": min(max_day, pol["emit_day"] + 1),
                }
            )

        # --- claims ------------------------------------------------------
        exposure_end = min(pol["expiry_date"], cfg.base_date)
        if exposure_end <= pol["inception_date"]:
            continue
        n_claims = 0
        while rng.random() < cfg.claim_frequency and n_claims < 3:
            n_claims += 1
            clm_seq += 1
            span = (exposure_end - pol["inception_date"]).days
            loss = pol["inception_date"] + timedelta(days=rng.randint(0, max(1, span)))
            # Reporting lag: mostly prompt, with a genuine long tail.
            lag = rng.choice([0, 0, 1, 1, 2, 3, 5, 8, 14, 21, 30, 45, 60])
            reported = loss + timedelta(days=lag)
            is_late = lag >= 21
            # Severity is scaled off PREMIUM, not sum insured, then capped at
            # the sum insured. Scaling off sum insured alone is what produced a
            # 667% loss ratio on Thai compulsory motor in an earlier version:
            # the premium is statutorily fixed at ~THB 645 while the sum insured
            # is THB 500,000, so any fraction of the sum insured dwarfs the
            # premium. Pricing works the other way round -- premium is set to
            # cover expected losses -- so a realistic book has
            # severity ~ premium / frequency, times a spread.
            #
            # With claim_frequency ~0.085 and the multiples below, portfolio
            # loss ratios land in the 45-85% band that a real motor and health
            # book occupies, which is what makes mart_loss_ratio worth reading.
            if pol["line_of_business"] == "motor":
                cause = rng.choice(CLAIM_CAUSES_MOTOR)
                multiple = SEVERITY_MULTIPLE.get(pol["product_code"], SEVERITY_MULTIPLE["_default"])
            else:
                cause = rng.choice(CLAIM_CAUSES_HEALTH)
                multiple = SEVERITY_MULTIPLE["HEALTH_ADJ"]
            severity = pol["written_premium"] * rng.uniform(*multiple)
            # A claim cannot exceed the sum insured.
            incurred = round(min(severity, pol["sum_insured"]), 2)
            status = rng.choice(["OPEN", "SETTLED", "SETTLED", "SETTLED", "REJECTED"])
            paid_amt = (
                incurred
                if status == "SETTLED"
                else (0.0 if status == "REJECTED" else round(incurred * 0.4, 2))
            )
            u.claims.append(
                {
                    "claim_id": f"CLM{clm_seq:08d}",
                    "claim_no": f"{pol['partner_code'][:3]}-{cfg.base_date.year}-{clm_seq:06d}",
                    "policy_id": pol["policy_id"],
                    "loss_date": loss,
                    "reported_date": reported,
                    "reporting_lag_days": lag,
                    "claim_cause": cause,
                    "claim_type": pol["line_of_business"],
                    "status": status,
                    "gross_incurred": incurred,
                    "paid_amount": paid_amt,
                    "reserve_amount": round(max(0.0, incurred - paid_amt), 2),
                    "currency": pol["currency"],
                    "at_fault": rng.random() < 0.45,
                    "settled_date": reported + timedelta(days=rng.randint(5, 90))
                    if status == "SETTLED"
                    else None,
                    "is_late_reported": is_late,
                    "emit_day": min(max_day, pol["emit_day"] + (1 if is_late else 0)),
                }
            )
