#!/usr/bin/env python
"""Create the workspace groups the governance grants reference.

Groups cannot be created from SQL -- they are an identity concern, not a
catalog one -- so this uses the SCIM API. It needs a token carrying the `scim`
scope; the `sql` and `unity-catalog` scopes that everything else in this
project uses are not sufficient.

Why script it rather than click three times in the admin console: the grants in
governance/unity_catalog.sql are generated from PII metadata and re-applied on
every change. If the principals they reference are created by hand, the two
drift the moment someone adds a fourth group, and a grant to a missing
principal fails in a way that looks like a permissions bug rather than a
missing group.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts.databricks_probe import hostname, load_env  # noqa: E402

# Kept in step with GROUPS in governance/generate_unity_catalog.py, plus the
# jurisdiction groups the row filter needs.
GROUPS = {
    "data_engineers": "Build and operate the pipeline.",
    "analysts": "Read gold and marts only.",
    "restricted_health": "The only group permitted unmasked health declarations.",
    "analysts_global": "Analysts cleared to read across jurisdictions.",
    "analysts_th": "Analysts restricted to the Thai book.",
    "analysts_id": "Analysts restricted to the Indonesian book.",
}


def scim(method: str, path: str, payload: dict | None = None):
    req = urllib.request.Request(
        f"https://{hostname()}/api/2.0/preview/scim/v2/{path}",
        method=method,
        data=json.dumps(payload).encode() if payload else None,
        headers={
            "Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}",
            "Content-Type": "application/scim+json",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = resp.read().decode()
        return json.loads(body) if body else {}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true", help="Only show existing groups.")
    args = ap.parse_args()
    load_env()

    if not os.getenv("DATABRICKS_TOKEN"):
        print("DATABRICKS_TOKEN not set (see .env.example)")
        return 1

    try:
        existing = {g["displayName"] for g in scim("GET", "Groups").get("Resources", [])}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")[:200]
        print(f"Cannot read groups ({exc.code}): {body}")
        if "scim" in body:
            print("\nThe token lacks the `scim` scope. Either regenerate it with")
            print("all-apis (or add `scim`), or create these groups by hand in")
            print("Settings -> Identity and access -> Groups:")
            for name, purpose in GROUPS.items():
                print(f"  {name:<20} {purpose}")
        return 1

    print(f"existing groups: {', '.join(sorted(existing)) or '(none)'}")
    if args.list:
        return 0

    created = failed = 0
    for name, purpose in GROUPS.items():
        if name in existing:
            print(f"  [SKIP] {name:<20} already exists")
            continue
        try:
            scim(
                "POST",
                "Groups",
                {
                    "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
                    "displayName": name,
                },
            )
            created += 1
            print(f"  [OK  ] {name:<20} {purpose}")
        except urllib.error.HTTPError as exc:
            failed += 1
            print(f"  [FAIL] {name:<20} {exc.code}: {exc.read().decode('utf-8', 'replace')[:90]}")

    print(f"\n{created} created, {failed} failed.")
    if created:
        print("Next: make databricks-governance  (the grants will now resolve)")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
