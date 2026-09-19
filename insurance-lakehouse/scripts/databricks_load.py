#!/usr/bin/env python
"""Bootstrap a Databricks catalog and load the landing zone into bronze.

WHY THIS EXISTS. Locally, `ingestion/bronze.py` runs PySpark against the landing
files. That cannot run on Databricks Free Edition, which is serverless-only:
there is no cluster to configure, no JARs to attach, no driver to submit to.

So the ingestion PATH changes while the ingestion CONTRACT does not. Files are
uploaded to a Unity Catalog Volume and read with `COPY INTO`, which is
Databricks' own idempotent-by-file loader -- it tracks which files it has
already consumed, exactly as the local manifest table does. The same
`config/sources.yml` drives both: same sources, same formats, same metadata
columns, same primary keys.

That symmetry is the point. `COPY INTO` is also the stepping stone to Auto
Loader: swap it for `cloudFiles` and the same registry keeps working.

Order of operations:
  1. create catalog + schemas (bronze/silver/gold/marts/observability)
  2. create a managed Volume for the landing zone
  3. upload every landing file the registry knows about
  4. COPY INTO one bronze table per source, adding the metadata columns
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from ingestion.bronze import discover_files, new_batch_id  # noqa: E402
from ingestion.config import load_registry  # noqa: E402
from scripts.databricks_probe import connect, load_env  # noqa: E402

SCHEMAS = ("bronze", "silver", "gold", "marts", "observability")
VOLUME = "landing"


def sql(cur, statement: str, label: str | None = None) -> None:
    if label:
        print(f"  {label}")
    cur.execute(statement)


def bootstrap(cur, catalog: str) -> None:
    print("bootstrapping catalog and schemas")
    sql(cur, f"CREATE CATALOG IF NOT EXISTS {catalog}", f"catalog {catalog}")
    sql(cur, f"USE CATALOG {catalog}")
    for schema in SCHEMAS:
        sql(cur, f"CREATE SCHEMA IF NOT EXISTS {schema}", f"schema {schema}")
    sql(cur, f"CREATE VOLUME IF NOT EXISTS bronze.{VOLUME}", f"volume bronze.{VOLUME}")


def volume_root(catalog: str) -> str:
    return f"/Volumes/{catalog}/bronze/{VOLUME}"


def upload(registry, catalog: str, dry_run: bool) -> dict[str, list[str]]:
    """Upload each source's files to the Volume, preserving relative paths.

    Relative paths are preserved because bronze derives `_batch_date` from the
    `dt=` component of the path. Flattening the upload would silently strip the
    batch date out of every row.
    """
    from databricks.sdk import WorkspaceClient

    client = (
        None
        if dry_run
        else WorkspaceClient(
            host=os.environ["DATABRICKS_HOST"],
            token=os.environ["DATABRICKS_TOKEN"],
        )
    )
    root = volume_root(catalog)
    uploaded: dict[str, list[str]] = {}

    for source in registry.sources:
        refs = discover_files(registry, source)
        if not refs:
            continue
        targets = []
        for ref in refs:
            rel = os.path.relpath(ref.path, os.path.abspath(registry.landing_root))
            dest = f"{root}/{rel}"
            targets.append(dest)
            if not dry_run:
                with open(ref.path, "rb") as fh:
                    client.files.upload(dest, fh, overwrite=True)
        uploaded[source.name] = targets
        print(f"  {source.name:<28} {len(targets)} file(s)")
    return uploaded


def copy_into(cur, registry, source, catalog: str, batch_id: str) -> None:
    """COPY INTO one bronze table, adding the same metadata columns as local bronze.

    `COPY INTO` is idempotent per file: re-running skips files already loaded,
    which is the same guarantee the local manifest table provides.
    """
    table = f"{catalog}.bronze.{source.name}"
    pattern = f"{volume_root(catalog)}/{source.path}"
    fmt = "CSV" if source.format == "csv" else "JSON"
    # Two different mergeSchema settings are needed, and they do different jobs.
    #
    # FORMAT_OPTIONS mergeSchema makes the reader infer across ALL files rather
    # than a sample. Without it, a column a partner adds on day 2 is silently
    # dropped: the rows still land, so row-count parity passes while the
    # drifted column is missing. That is the same failure the local engine has
    # (ADR-0008, fixed there by grouping files on header signature) showing up
    # on a different platform through a different mechanism.
    #
    # COPY_OPTIONS mergeSchema, set below, is the other half: it lets the
    # TARGET table gain the column once the reader has found it.
    #
    # rescuedDataColumn captures content that did not parse into the declared
    # schema. Without it a malformed JSON line lands as a row of nulls that is
    # indistinguishable from a sparse but valid record.
    opts = (
        {"header": "true", "inferSchema": "false", "mergeSchema": "true"}
        if source.format == "csv"
        else {"rescuedDataColumn": "_rescued_data", "mergeSchema": "true"}
    )
    opt_sql = ", ".join(f"'{k}' = '{v}'" for k, v in opts.items())

    # Empty shell first: COPY INTO requires the target to exist. mergeSchema on
    # the copy widens it as partner files drift.
    sql(
        cur,
        f"CREATE TABLE IF NOT EXISTS {table} USING DELTA "
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
    )
    sql(
        cur,
        f"""
        COPY INTO {table}
        FROM (
          SELECT
            *,
            current_timestamp()                                   AS _ingested_at,
            _metadata.file_path                                   AS _source_file,
            '{batch_id}'                                          AS _batch_id,
            '{source.name}'                                       AS _source_name,
            regexp_extract(_metadata.file_path,
                           '{registry.batch_date_pattern}', 1)    AS _batch_date
          FROM '{pattern}'
        )
        FILEFORMAT = {fmt}
        {"FORMAT_OPTIONS (" + opt_sql + ")" if opt_sql else ""}
        COPY_OPTIONS ('mergeSchema' = 'true')
    """,
        f"COPY INTO {source.name}",
    )


def add_record_hash(cur, source, catalog: str) -> None:
    """Add _record_hash, the one metadata column COPY INTO cannot supply inline.

    The local engine hashes the BUSINESS columns only, so a record a partner
    resends verbatim hashes identically and silver can collapse it. That
    definition needs the column list, which is only known after COPY INTO has
    inferred the schema -- hence a second pass rather than an expression in the
    SELECT.

    Omitting it broke twelve staging models on Databricks with
    UNRESOLVED_COLUMN, because dedupe_latest() uses it as the final tie-break.
    Row-count parity did not catch that: the counts were identical and the
    schemas were not.
    """
    table = f"{catalog}.bronze.{source.name}"

    cur.execute(f"DESCRIBE TABLE {table}")
    columns = [r[0] for r in cur.fetchall() if r[0] and not r[0].startswith("#")]
    if "_record_hash" not in columns:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN _record_hash STRING")

    business = [c for c in columns if not c.startswith("_")]
    if not business:
        return

    # Mirrors ingestion/bronze.py::add_metadata exactly: '||' separator and a
    # '<null>' sentinel, so a null and an empty string do not collide.
    parts = ", ".join(f"coalesce(cast(`{c}` AS STRING), '<null>')" for c in business)
    cur.execute(
        f"UPDATE {table} SET _record_hash = sha2(concat_ws('||', {parts}), 256) "
        "WHERE _record_hash IS NULL"
    )


def quarantine(cur, source, catalog: str) -> None:
    """Move unlandable records out of the bronze table, as local bronze does.

    COPY INTO has no notion of quarantine: it loads every row it can parse and
    leaves the rest to the rescued-data column. Without this step the Databricks
    tables carry records the local ones deliberately hold back, and the two
    targets stop meaning the same thing -- which the parity check caught as an
    exact +4 on precisely the four tables that have a quarantine table locally.

    The rule is ADR-0007's, unchanged: quarantine only what cannot be landed at
    all -- a record with no primary key to merge on, or one that did not parse.
    Business-rule violations (negative premium, orphan FK) still land, because
    they are the signal the dbt tests exist to measure.
    """
    table = f"{catalog}.bronze.{source.name}"
    qtable = f"{catalog}.bronze.{source.name}_quarantine"

    cur.execute(f"DESCRIBE TABLE {table}")
    columns = [r[0] for r in cur.fetchall() if r[0] and not r[0].startswith("#")]
    business = [c for c in columns if not c.startswith("_")]

    # Order matters: the first matching condition supplies the reason, and the
    # local engine reports "unparseable" in preference to "no key" for a record
    # that is both.
    conditions: list[tuple[str, str]] = []

    if "_rescued_data" in columns:
        conditions.append(("_rescued_data IS NOT NULL", "unparseable_record"))

    if source.format == "json" and business:
        # Databricks' JSON reader is more permissive than the local one. Given
        # `{not valid json` it does NOT populate _rescued_data -- it emits a row
        # with every field null. A JSON object cannot legitimately produce that,
        # so the shape itself is the evidence of a parse failure, and labelling
        # it "unparseable" rather than "no primary key" keeps the two targets
        # describing the same defect the same way.
        all_null = " AND ".join(f"`{c}` IS NULL" for c in business)
        conditions.append((f"({all_null})", "unparseable_record"))

    if source.primary_key:
        pk = " OR ".join(
            f"`{c}` IS NULL OR trim(cast(`{c}` AS STRING)) = ''" for c in source.primary_key
        )
        conditions.append((f"({pk})", "missing_primary_key"))

    if not conditions:
        return

    predicate = " OR ".join(c for c, _ in conditions)

    # Only materialise a quarantine table when something is actually quarantined.
    # Creating one per source leaves a dozen empty tables that imply a problem
    # where there is none, and diverges from the local layout.
    cur.execute(f"SELECT count(*) FROM {table} WHERE {predicate}")
    if cur.fetchone()[0] == 0:
        return

    reason = (
        "CASE " + " ".join(f"WHEN {cond} THEN '{label}'" for cond, label in conditions) + " END"
    )
    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {qtable} AS "
        f"SELECT *, CAST(NULL AS STRING) AS _quarantine_reason FROM {table} WHERE 1 = 0"
    )
    cur.execute(
        f"INSERT INTO {qtable} BY NAME "
        f"SELECT *, {reason} AS _quarantine_reason FROM {table} WHERE {predicate}"
    )
    cur.execute(f"DELETE FROM {table} WHERE {predicate}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default=os.getenv("DATABRICKS_CATALOG", "insurance_dev"))
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would happen without connecting or uploading.",
    )
    ap.add_argument(
        "--skip-upload",
        action="store_true",
        help="Files are already in the Volume; only run COPY INTO.",
    )
    args = ap.parse_args()

    load_env()
    registry = load_registry()
    batch_id = new_batch_id()

    if args.dry_run:
        print(f"DRY RUN -- catalog={args.catalog} batch_id={batch_id}")
        print(f"volume root: {volume_root(args.catalog)}\n")
        print("would upload:")
        upload(registry, args.catalog, dry_run=True)
        print("\nwould COPY INTO:")
        for s in registry.sources:
            print(f"  {args.catalog}.bronze.{s.name:<28} <- {s.path}")
        return 0

    # HTTP_PATH is optional: connect() discovers a warehouse when it is unset.
    missing = [k for k in ("DATABRICKS_HOST", "DATABRICKS_TOKEN") if not os.getenv(k)]
    if missing:
        print("Missing: " + ", ".join(missing) + "  (see .env.example)")
        return 1

    with connect() as conn, conn.cursor() as cur:
        bootstrap(cur, args.catalog)
        if not args.skip_upload:
            print("uploading landing files to the Volume")
            upload(registry, args.catalog, dry_run=False)
        print("loading bronze tables")
        for source in registry.sources:
            copy_into(cur, registry, source, args.catalog, batch_id)
            # Hash before quarantining, so held-back records carry it too --
            # the same order as the local engine.
            add_record_hash(cur, source, args.catalog)
            quarantine(cur, source, args.catalog)

    print(f"\nbronze loaded into {args.catalog}.bronze (batch {batch_id})")
    print("next:  make dbt-build TARGET=databricks")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
