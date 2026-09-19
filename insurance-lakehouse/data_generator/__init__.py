"""Seeded synthetic insurance data generator.

Rebuilds a complete, referentially-consistent insurance universe from a seed and
emits it as daily integration drops. See ``docs/insurance_domain_primer.md`` for
the domain and ``data_generator/config.py`` for the defect registry.
"""

from .config import MESS_REGISTRY, GeneratorConfig, MessToggles
from .emit import emit_day
from .universe import Universe, build_universe

__all__ = [
    "MESS_REGISTRY",
    "GeneratorConfig",
    "MessToggles",
    "Universe",
    "build_universe",
    "emit_day",
]
