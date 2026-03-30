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
        cleaned = {k: _clean_value(v) for k, v in row.items()}
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
