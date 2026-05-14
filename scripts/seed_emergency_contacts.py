#!/usr/bin/env python3
"""Upsert Firebase `hotlines/` table from the Emergency Contacts CSV.

Writes one record per row keyed by a stable slug (name_en + city).
Never touches entities/providers or any other Firebase path.

Usage:
    python scripts/seed_emergency_contacts.py [--csv PATH] [--dry-run]
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import unicodedata
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scraper.logging_config import setup_logging

DEFAULT_CSV = Path(__file__).resolve().parent.parent / "data" / "emergency_contacts.csv"

setup_logging()
log = logging.getLogger("seed-hotlines")


def slugify(value: str) -> str:
    value = unicodedata.normalize("NFKD", value)
    value = value.encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^\w\s-]", "", value).strip().lower()
    value = re.sub(r"[\s_-]+", "-", value)
    return value or "unknown"


def clean(value: str | None) -> str | None:
    if not value:
        return None
    s = str(value).strip()
    return s or None


def row_to_record(row: dict) -> tuple[str, dict] | None:
    name_en = clean(row.get("Emergency Contact Name en"))
    if not name_en:
        return None
    city = clean(row.get("City")) or "Nationwide"
    slug = slugify(f"{name_en}-{city}")
    return slug, {
        "id": slug,
        "category": clean(row.get("Category")) or "",
        "city": city,
        "name_en": name_en,
        "name_ar": clean(row.get("Emergency Contact Name ar")),
        "hotline": clean(row.get("Hotline")),
        "phone": clean(row.get("Emergency Contact Phone")),
        "email": clean(row.get("Emergency Contact Email")),
        "source_url": clean(row.get("Source URL")),
        "inserted_at": clean(row.get("Inserted at")) or "",
        "updated_at": clean(row.get("Updated at")),
    }


def write_firebase(records: dict[str, dict], db_url: str, cred_path: str) -> None:
    import firebase_admin
    from firebase_admin import credentials
    from firebase_admin import db as rtdb

    if not firebase_admin._apps:
        firebase_admin.initialize_app(credentials.Certificate(cred_path), {"databaseURL": db_url})

    rtdb.reference("hotlines").update(records)
    log.info("Upserted %d records to Firebase hotlines/", len(records))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.csv.exists():
        raise SystemExit(f"CSV not found: {args.csv}")

    with args.csv.open(encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    log.info("Loaded %d rows from %s", len(rows), args.csv)

    records: dict[str, dict] = {}
    for row in rows:
        result = row_to_record(row)
        if result:
            slug, record = result
            records[slug] = record

    log.info("Parsed %d hotline records", len(records))

    if args.dry_run:
        print(json.dumps(list(records.values())[:3], indent=2, ensure_ascii=False))
        print("\nDry-run only — no Firebase writes performed.")
        return

    cred_path = os.getenv("FIREBASE_CRED_PATH")
    db_url = os.getenv("FIREBASE_DB_URL")
    if not cred_path or not db_url:
        raise SystemExit("FIREBASE_CRED_PATH and FIREBASE_DB_URL must be set. Aborting.")

    write_firebase(records, db_url, cred_path)


if __name__ == "__main__":
    main()
