"""Generator configuration and the registry of deliberate data-quality defects.

Every defect the generator injects is declared in :data:`MESS_REGISTRY` with a
human-readable description and the layer that is expected to deal with it.
Tests assert that each one actually appears in the output, and the docs table in
``docs/insurance_domain_primer.md`` is generated from this same registry so the
two cannot drift apart.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, fields
from datetime import date

# code -> (what it is, where it gets handled)
MESS_REGISTRY: dict[str, tuple[str, str]] = {
    "exact_duplicates": (
        "A partner resends a byte-identical row in a later file.",
        "bronze keeps both; silver dedups on _record_hash",
    ),
    "near_duplicates": (
        "The same business record resent with a corrected field and a newer timestamp.",
        "silver dedup keeps latest per key with documented tie-break",
    ),
    "mixed_date_formats": (
        "Dates arrive as YYYY-MM-DD, DD/MM/YYYY and D MMM YYYY in the same column.",
        "silver staging coalesces multiple to_date patterns",
    ),
    "buddhist_era_dates": (
        "Thai partners emit Buddhist Era years (2569 = 2026).",
        "be_to_ce_date() macro in silver",
    ),
    "mixed_language_text": (
        "Thai and English values in the same column (province, status, name).",
        "silver mapping tables normalise to English codes",
    ),
    "whitespace_casing": (
        "Trailing/leading whitespace and inconsistent casing on keys and codes.",
        "trim + upper in silver staging",
    ),
    "late_claims": (
        "A claim reported weeks after the loss date, arriving after its batch closed.",
        "incremental claims model with a configurable lookback window",
    ),
    "backdated_endorsements": (
        "A mid-term endorsement whose effective date precedes rows already captured.",
        "SCD2 snapshot with check strategy; as-of-date model",
    ),
    "cancellations_refunds": (
        "Policy cancelled mid-term with a negative pro-rata refund payment.",
        "premium >= 0 singular test carves out refunds",
    ),
    "schema_drift": (
        "A partner adds a new column in a later daily file.",
        "bronze mergeSchema + drift audit table",
    ),
    "validation_failures": (
        "Negative premium, claim outside coverage, orphan foreign keys.",
        "quarantine at bronze; warn/error dbt tests at silver",
    ),
    "mixed_currencies": (
        "THB and IDR amounts in one column, needing fx conversion.",
        "convert_currency() macro against fx_rates",
    ),
}


@dataclass
class MessToggles:
    """Every defect can be switched off individually, for isolating behaviour."""

    exact_duplicates: bool = True
    near_duplicates: bool = True
    mixed_date_formats: bool = True
    buddhist_era_dates: bool = True
    mixed_language_text: bool = True
    whitespace_casing: bool = True
    late_claims: bool = True
    backdated_endorsements: bool = True
    cancellations_refunds: bool = True
    schema_drift: bool = True
    validation_failures: bool = True
    mixed_currencies: bool = True

    @classmethod
    def all_off(cls) -> MessToggles:
        return cls(**{f.name: False for f in fields(cls)})

    @classmethod
    def from_env(cls) -> MessToggles:
        """``LAKEHOUSE_MESS=none`` disables everything; ``a,b`` enables only a and b."""
        raw = os.getenv("LAKEHOUSE_MESS", "").strip()
        if not raw:
            return cls()
        if raw.lower() in {"none", "off"}:
            return cls.all_off()
        wanted = {p.strip() for p in raw.split(",") if p.strip()}
        unknown = wanted - set(MESS_REGISTRY)
        if unknown:
            raise ValueError(f"Unknown mess toggles: {sorted(unknown)}")
        return cls(**{f.name: f.name in wanted for f in fields(cls)})


@dataclass
class GeneratorConfig:
    seed: int = 42
    n_policies: int = 20_000
    base_date: date = date(2026, 1, 5)
    day: int = 1
    max_days: int = 3
    out_root: str = "./lakehouse/landing"
    mess: MessToggles = field(default_factory=MessToggles)

    # Business shape knobs. Kept here so the whole domain is tunable from one place.
    quote_to_policy_rate: float = 0.34
    claim_frequency: float = 0.085
    cancellation_rate: float = 0.04
    endorsement_rate: float = 0.18
    country_weights: tuple[tuple[str, float], ...] = (("TH", 0.72), ("ID", 0.28))

    @classmethod
    def from_env(cls, **overrides) -> GeneratorConfig:
        base = cls(
            seed=int(os.getenv("LAKEHOUSE_SEED", "42")),
            n_policies=int(os.getenv("LAKEHOUSE_N_POLICIES", "20000")),
            base_date=date.fromisoformat(os.getenv("LAKEHOUSE_BASE_DATE", "2026-01-05")),
            out_root=os.getenv("LAKEHOUSE_LANDING", "./lakehouse/landing"),
            mess=MessToggles.from_env(),
        )
        for k, v in overrides.items():
            if v is not None:
                setattr(base, k, v)
        return base

    def batch_date(self, day: int | None = None) -> date:
        from datetime import timedelta

        return self.base_date + timedelta(days=(day or self.day) - 1)
