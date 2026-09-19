"""Load and validate ``config/sources.yml``.

The bronze engine is generic over whatever this file declares. Registering a new
source is a YAML edit; there is no per-source Python anywhere in ``ingestion/``.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = REPO_ROOT / "config" / "sources.yml"

# Metadata columns bronze adds to every record. Named with a leading underscore
# so they never collide with a partner's own columns.
META_INGESTED_AT = "_ingested_at"
META_SOURCE_FILE = "_source_file"
META_BATCH_ID = "_batch_id"
META_RECORD_HASH = "_record_hash"
META_SOURCE_NAME = "_source_name"
META_BATCH_DATE = "_batch_date"
CORRUPT_COLUMN = "_corrupt_record"

METADATA_COLUMNS = (
    META_INGESTED_AT,
    META_SOURCE_FILE,
    META_BATCH_ID,
    META_RECORD_HASH,
    META_SOURCE_NAME,
    META_BATCH_DATE,
)

MANIFEST_TABLE = "_ingestion_manifest"
SCHEMA_AUDIT_TABLE = "_schema_audit"
QUARANTINE_SUFFIX = "_quarantine"


@dataclass(frozen=True)
class PiiColumn:
    column: str
    pii_type: str
    sensitivity: str


@dataclass
class Source:
    name: str
    description: str
    format: str
    path: str
    reader_options: dict[str, str] = field(default_factory=dict)
    primary_key: list[str] = field(default_factory=list)
    timestamp_column: str | None = None
    partition_by: list[str] = field(default_factory=list)
    freshness: dict[str, int] = field(default_factory=dict)
    pii_columns: list[PiiColumn] = field(default_factory=list)

    def table_name(self) -> str:
        return self.name

    def quarantine_table(self) -> str:
        return f"{self.name}{QUARANTINE_SUFFIX}"


@dataclass
class SourceRegistry:
    landing_root: str
    bronze_path: str
    bronze_schema: str
    batch_date_pattern: str
    sources: list[Source]

    def get(self, name: str) -> Source:
        for s in self.sources:
            if s.name == name:
                return s
        raise KeyError(f"unknown source {name!r}. Known: {[s.name for s in self.sources]}")

    def names(self) -> list[str]:
        return [s.name for s in self.sources]

    def select(self, only: list[str] | None) -> list[Source]:
        if not only:
            return list(self.sources)
        return [self.get(n) for n in only]


_REQUIRED = ("name", "description", "format", "path")
_SUPPORTED_FORMATS = {"csv", "json"}


def load_registry(path: str | os.PathLike | None = None) -> SourceRegistry:
    cfg_path = Path(path) if path else DEFAULT_CONFIG
    raw: dict[str, Any] = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    defaults = raw.get("defaults", {})

    sources: list[Source] = []
    seen: set[str] = set()
    for entry in raw.get("sources", []):
        missing = [k for k in _REQUIRED if not entry.get(k)]
        if missing:
            raise ValueError(f"source {entry.get('name', '<unnamed>')} missing keys: {missing}")
        if entry["format"] not in _SUPPORTED_FORMATS:
            raise ValueError(
                f"source {entry['name']}: unsupported format {entry['format']!r}; "
                f"supported: {sorted(_SUPPORTED_FORMATS)}"
            )
        if entry["name"] in seen:
            raise ValueError(f"duplicate source name {entry['name']!r}")
        seen.add(entry["name"])

        pii = [
            PiiColumn(column=col, pii_type=meta["pii_type"], sensitivity=meta["sensitivity"])
            for col, meta in (entry.get("pii_columns") or {}).items()
        ]
        sources.append(
            Source(
                name=entry["name"],
                description=" ".join(entry["description"].split()),
                format=entry["format"],
                path=entry["path"],
                reader_options={k: str(v) for k, v in (entry.get("reader_options") or {}).items()},
                primary_key=list(entry.get("primary_key") or []),
                timestamp_column=entry.get("timestamp_column"),
                partition_by=list(entry.get("partition_by") or []),
                freshness=dict(entry.get("freshness") or {}),
                pii_columns=pii,
            )
        )

    if not sources:
        raise ValueError(f"{cfg_path} declares no sources")

    return SourceRegistry(
        landing_root=os.getenv(
            "LAKEHOUSE_LANDING", defaults.get("landing_root", "lakehouse/landing")
        ),
        bronze_path=os.getenv("LAKEHOUSE_BRONZE", defaults.get("bronze_path", "lakehouse/bronze")),
        bronze_schema=defaults.get("bronze_schema", "bronze"),
        batch_date_pattern=defaults.get("batch_date_from_path", r"dt=([0-9]{4}-[0-9]{2}-[0-9]{2})"),
        sources=sources,
    )


def expand_braces(pattern: str) -> list[str]:
    """Expand ``{a,b}`` alternatives -- Python's glob has no brace support.

    Spark accepts brace globs natively, but bronze lists files itself so that
    the manifest can decide what is new, so the expansion has to happen here.
    """
    match = re.search(r"\{([^{}]*)\}", pattern)
    if not match:
        return [pattern]
    head, tail = pattern[: match.start()], pattern[match.end() :]
    out: list[str] = []
    for alt in match.group(1).split(","):
        out.extend(expand_braces(f"{head}{alt.strip()}{tail}"))
    return out
