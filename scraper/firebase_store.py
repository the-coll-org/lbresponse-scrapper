"""Store scraped Power BI data in Firebase Realtime Database."""

import logging
from datetime import UTC, datetime

import firebase_admin
from firebase_admin import credentials
from firebase_admin import db as rtdb

from config import FIREBASE_COLLECTION, FIREBASE_CRED_PATH, FIREBASE_DB_URL

log = logging.getLogger(__name__)

_app = None


def _init():
    global _app
    if _app is not None:
        return

    if not FIREBASE_CRED_PATH or not FIREBASE_DB_URL:
        raise RuntimeError(
            "Firebase not configured. Set FIREBASE_CRED_PATH and FIREBASE_DB_URL "
            "environment variables (see .env.example)."
        )

    cred = credentials.Certificate(FIREBASE_CRED_PATH)
    _app = firebase_admin.initialize_app(cred, {"databaseURL": FIREBASE_DB_URL})


def store_visual_data(
    visual_name: str,
    page_name: str,
    rows: list[dict],
    entities: set[str],
) -> int:
    """Store rows in Firebase RTDB.

    Structure: powerbi_data/{visual_key}/metadata + powerbi_data/{visual_key}/rows
    """
    _init()
    now = datetime.now(UTC).isoformat()
    doc_id = _sanitize_key(visual_name)
    ref = rtdb.reference(f"{FIREBASE_COLLECTION}/{doc_id}")

    ref.child("metadata").set(
        {
            "visual_name": visual_name,
            "page": page_name,
            "entities": list(entities),
            "last_scraped": now,
            "row_count": len(rows),
        }
    )

    cleaned_rows = []
    for row in rows:
        cleaned = {_sanitize_key(k): _clean_value(v) for k, v in row.items()}
        cleaned["_scraped_at"] = now
        cleaned_rows.append(cleaned)

    ref.child("rows").set(cleaned_rows)

    log.info("Stored %d rows for visual '%s' in Firebase RTDB", len(rows), visual_name)
    return len(rows)


def clear_visual_data(visual_name: str):
    """Delete all data for a visual before re-scraping."""
    _init()
    doc_id = _sanitize_key(visual_name)
    ref = rtdb.reference(f"{FIREBASE_COLLECTION}/{doc_id}")
    ref.delete()
    log.info("Cleared data for visual '%s'", visual_name)


def mirror_entities(snapshot: dict[str, dict]) -> dict[str, int]:
    """Mirror the normalized ER snapshot to Firebase under `entities/` and `categories/`.

    Expected snapshot shape:
      {
        "providers":   {provider_id: {...}, ...},
        "services":    {...},
        "locations":   {...},
        "service_availability": {...},
        "shelters":    {...},
        "shelter_needs": {...},
        "aid_matches": {...},
        "categories":  {category_type: {key: {...}, ...}, ...},
      }
    """
    _init()
    now = datetime.now(UTC).isoformat()
    counts: dict[str, int] = {}

    entities_root = rtdb.reference("entities")
    for entity_type, records in snapshot.items():
        if entity_type == "categories":
            continue
        cleaned = {
            _sanitize_key(rid): {_sanitize_key(k): _clean_value(v) for k, v in rec.items()}
            for rid, rec in records.items()
        }
        entities_root.child(entity_type).set(cleaned)
        counts[entity_type] = len(cleaned)

    categories = snapshot.get("categories", {})
    if categories:
        cat_ref = rtdb.reference("categories")
        cleaned_cats = {
            _sanitize_key(ctype): {
                _sanitize_key(key): {k: _clean_value(v) for k, v in rec.items()}
                for key, rec in entries.items()
            }
            for ctype, entries in categories.items()
        }
        cat_ref.set(cleaned_cats)
        counts["categories"] = sum(len(v) for v in cleaned_cats.values())

    rtdb.reference("entities_metadata").set(
        {
            "last_mirrored": now,
            "counts": counts,
        }
    )

    log.info("Mirrored ER snapshot to Firebase: %s", counts)
    return counts


def _sanitize_key(name: str) -> str:
    """Firebase RTDB keys cannot contain . $ # [ ] /"""
    for ch in ".$/[]#":
        name = name.replace(ch, "_")
    return name.strip()[:200]


def _clean_value(v):
    if v is None:
        return None
    if isinstance(v, float) and (v != v):  # NaN
        return None
    return v
