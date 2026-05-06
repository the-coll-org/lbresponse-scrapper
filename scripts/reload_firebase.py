#!/usr/bin/env python3
"""Wipe Firebase entities/categories and rebuild from the latest CSV.

Reads the Service Mapping table CSV (the rich one with every column the
help-center UI needs), groups rows by Organization, and writes one
denormalised Provider record per organisation:

    entities/providers/{slug}: {
        provider_id, provider_name, slug,
        primary_contact:   {name, email, phone, whatsapp},
        secondary_contact: {name, email, phone, whatsapp} | null,
        sectors:   ["Child Protection", ...],
        districts: ["Akkar", ...],
        services:  [{name, sector, district, target_age_gender,
                     target_population, accessible}],
        service_count, is_name_valid, pinned, verified, updated_at,
    }

Plus seeds two category groups derived from the table:
    categories/sector/{key}:   {key, en_label, sort_order}
    categories/district/{key}: {key, en_label, sort_order}

Pass --dry-run to print a sample provider without touching Firebase.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
import unicodedata
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

# Allow `python scripts/reload_firebase.py` from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scraper.data_transformer import (  # noqa: E402
    extract_lebanese_whatsapp_from_row,
    is_provider_name_valid,
    is_valid_lebanese_phone,
)
from scraper.logging_config import setup_logging  # noqa: E402

CSV_PATH = (
    Path(__file__).resolve().parent.parent
    / "output"
    / "Service_Mapping_Main_horizontal_26_tableEx.csv"
)

setup_logging()
log = logging.getLogger("reload-firebase")


def slugify(value: str) -> str:
    """Make a Firebase-safe key from a free-form name."""
    value = unicodedata.normalize("NFKD", value)
    value = value.encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^\w\s-]", "", value).strip().lower()
    value = re.sub(r"[\s_-]+", "-", value)
    return value or "unknown"


def clean(value: str | None) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def first_phone(text: str | None) -> str | None:
    """Pick the first Lebanese-shaped phone from a comma/slash separated cell."""
    if not text:
        return None
    candidates = re.split(r"[,/]| or ", text)
    for raw in candidates:
        candidate = raw.strip()
        if candidate and is_valid_lebanese_phone(candidate):
            return candidate
    return None


def build_contact(name: str | None, email: str | None, phone_text: str | None) -> dict | None:
    name = clean(name)
    email = clean(email)
    phone = first_phone(phone_text)
    if not (name or email or phone):
        return None
    whatsapp = extract_lebanese_whatsapp_from_row(
        {"name": name or "", "email": email or "", "phone": phone or ""}
    )
    return {
        "name": name,
        "email": email,
        "phone": phone,
        "whatsapp": whatsapp,
    }


def _row_primary(row: dict) -> dict | None:
    return build_contact(
        row.get("CONTACT: Primary Focal Point (Full name)"),
        row.get("CONTACT: Primary Focal Point (Email)"),
        row.get("CONTACT: Primary Focal Point (phone number)"),
    )


def _row_secondary(row: dict) -> dict | None:
    return build_contact(
        row.get("CONTACT: Secondary Focal Point (Full name)"),
        row.get("CONTACT: Secondary Focal Point (Email)"),
        row.get("CONTACT: Secondary Focal Point (Phone number)"),
    )


def aggregate(
    rows: list[dict],
) -> tuple[dict[tuple[str, str], dict], list[str], list[str]]:
    """Keep only the FIRST row encountered for each (Organization, District).

    The source CSV repeats the same (org, district) across many rows
    (different services / target populations). We treat each (org, district)
    as one card and ignore subsequent duplicate rows. The kept row's
    contacts, sector, and service are the only ones surfaced.
    """
    by_group: dict[tuple[str, str], dict] = {}
    sector_set: set[str] = set()
    district_set: set[str] = set()

    for row in rows:
        org = clean(row.get("Organization"))
        district = clean(row.get("District"))
        if not org or not district:
            continue

        key = (org, district)
        if key in by_group:
            continue  # only the first row counts

        sector = clean(row.get("Sector"))
        service = clean(row.get("Services"))
        district_set.add(district)
        if sector:
            sector_set.add(sector)

        services_list = (
            [
                {
                    "name": service,
                    "sector": sector,
                    "target_age_gender": clean(row.get("Targeted (Age/Gender)")),
                    "target_population": clean(row.get("Targeted Population Group")),
                    "accessible": (
                        clean(row.get("Service accessible for persons with disabilities"))
                        == "Yes"
                    ),
                }
            ]
            if service
            else []
        )

        by_group[key] = {
            "provider_name": org,
            "district": district,
            "primary_contact": _row_primary(row),
            "secondary_contact": _row_secondary(row),
            "sectors": {sector} if sector else set(),
            "services": services_list,
        }

    return by_group, sorted(sector_set), sorted(district_set)


def to_record(
    key: tuple[str, str], raw: dict, now_iso: str
) -> tuple[str, dict]:
    org, district = key
    org_slug = slugify(org)
    district_slug = slugify(district)
    composite_slug = f"{org_slug}__{district_slug}"
    provider_id = str(
        uuid5(NAMESPACE_URL, f"lbresponse:provider:{composite_slug}")
    )
    sectors_sorted = sorted(raw["sectors"])
    services_sorted = sorted(
        raw["services"],
        key=lambda s: (s["sector"] or "", s["name"] or ""),
    )
    return composite_slug, {
        "provider_id": provider_id,
        "provider_name": org,
        "org_slug": org_slug,
        "district": district,
        "district_slug": district_slug,
        "primary_contact": raw["primary_contact"],
        "secondary_contact": raw["secondary_contact"],
        "sectors": sectors_sorted,
        "districts": [district],
        "services": services_sorted,
        "service_count": len(services_sorted),
        "is_name_valid": is_provider_name_valid(org),
        "pinned": False,
        "verified": False,
        "updated_at": now_iso,
    }


def build_categories(sectors: list[str], districts: list[str]) -> dict[str, dict[str, dict]]:
    return {
        "sector": {
            slugify(s): {
                "key": slugify(s),
                "en_label": s,
                "ar_label": None,
                "sort_order": idx,
            }
            for idx, s in enumerate(sectors)
        },
        "district": {
            slugify(d): {
                "key": slugify(d),
                "en_label": d,
                "ar_label": None,
                "sort_order": idx,
            }
            for idx, d in enumerate(districts)
        },
    }


def write_firebase(
    providers: dict[str, dict],
    categories: dict[str, dict[str, dict]],
) -> None:
    import firebase_admin
    from firebase_admin import credentials, db as rtdb

    cred_path = os.getenv("FIREBASE_CRED_PATH")
    db_url = os.getenv("FIREBASE_DB_URL")
    if not cred_path or not db_url:
        raise SystemExit(
            "FIREBASE_CRED_PATH and FIREBASE_DB_URL must be set in the environment "
            "(loaded from .env). Aborting before any Firebase writes."
        )
    if not firebase_admin._apps:
        firebase_admin.initialize_app(
            credentials.Certificate(cred_path), {"databaseURL": db_url}
        )

    log.info("Wiping entities/* and categories/* on %s", db_url)
    rtdb.reference("entities").delete()
    rtdb.reference("categories").delete()

    log.info("Writing %d providers to entities/providers", len(providers))
    rtdb.reference("entities/providers").set(providers)

    counts = {kind: len(values) for kind, values in categories.items()}
    log.info("Writing categories: %s", counts)
    rtdb.reference("categories").set(categories)

    rtdb.reference("entities_metadata").set(
        {
            "last_mirrored": datetime.now(UTC).isoformat(),
            "source": "scripts/reload_firebase.py",
            "counts": {
                "providers": len(providers),
                "category_sector": counts.get("sector", 0),
                "category_district": counts.get("district", 0),
            },
        }
    )
    log.info("Done.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv",
        type=Path,
        default=CSV_PATH,
        help=f"Path to the Service Mapping table CSV (default: {CSV_PATH})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Aggregate and print summary + a sample provider; do not write Firebase.",
    )
    args = parser.parse_args()

    if not args.csv.exists():
        raise SystemExit(f"CSV not found: {args.csv}")

    with args.csv.open() as f:
        rows = list(csv.DictReader(f))
    log.info("Loaded %d rows from %s", len(rows), args.csv)

    raw_by_group, sectors, districts = aggregate(rows)
    log.info(
        "Aggregated: %d (org, district) cards, %d sectors, %d districts",
        len(raw_by_group),
        len(sectors),
        len(districts),
    )

    now_iso = datetime.now(UTC).isoformat()
    providers = dict(
        to_record(key, raw, now_iso) for key, raw in raw_by_group.items()
    )
    categories = build_categories(sectors, districts)

    sample_key = next(iter(providers))
    sample = providers[sample_key]
    log.info("Sample card %r:", sample_key)
    import json

    print(json.dumps(sample, indent=2, ensure_ascii=False))

    print()
    print(f"Sectors ({len(sectors)}): {sectors}")
    print()
    print(f"Districts ({len(districts)}): {districts[:8]}{' ...' if len(districts) > 8 else ''}")

    counts = defaultdict(int)
    for record in providers.values():
        if record["primary_contact"] and record["primary_contact"].get("phone"):
            counts["with_phone"] += 1
        if record["primary_contact"] and record["primary_contact"].get("email"):
            counts["with_email"] += 1
        if record["service_count"]:
            counts["with_services"] += 1
    print()
    print(
        "Coverage: "
        f"with_phone={counts['with_phone']}/{len(providers)}, "
        f"with_email={counts['with_email']}/{len(providers)}, "
        f"with_services={counts['with_services']}/{len(providers)}"
    )

    if args.dry_run:
        print("\nDry-run only — no Firebase writes performed.")
        return

    write_firebase(providers, categories)


if __name__ == "__main__":
    main()
