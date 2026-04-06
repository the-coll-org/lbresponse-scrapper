"""Transform Power BI data to structured database format."""

import logging
from datetime import date, datetime
from typing import Any

from scraper.models import (
    Accessibility,
    AgeGroup,
    AidMatchStatus,
    AidType,
    GenderTarget,
    Governorate,
    ProviderType,
    Sector,
    ServiceStatus,
    ServiceSubtype,
    Severity,
    ShelterNeedStatus,
    ShelterStatus,
    ShelterType,
)

log = logging.getLogger(__name__)


def infer_enum_value(value: Any, enum_class, default=None):
    """Try to match a string value to an enum."""
    if value is None:
        return default

    if isinstance(value, enum_class):
        return value

    value_str = str(value).lower().strip().replace(" ", "_").replace("-", "_")

    # Exact match on value
    for member in enum_class:
        if member.value == value_str:
            return member

    # Exact match on name
    for member in enum_class:
        if member.name.lower() == value_str:
            return member

    # Substring match
    for member in enum_class:
        if member.value in value_str or value_str in member.value:
            return member

    return default


def _parse_int(val):
    """Safely parse an integer from various input types."""
    if val is None:
        return None
    if isinstance(val, int):
        return val
    try:
        return int(float(str(val).strip().split()[0]))
    except (ValueError, AttributeError, IndexError):
        return None


def _parse_float(val):
    """Safely parse a float from various input types."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return None


def _parse_date(val):
    """Safely parse a date from various input types."""
    if val is None:
        return None
    if isinstance(val, date):
        return val
    if isinstance(val, datetime):
        return val.date()
    val_str = str(val).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(val_str, fmt).date()
        except ValueError:
            continue
    return None


def _parse_datetime(val):
    """Safely parse a datetime from various input types."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, date):
        return datetime(val.year, val.month, val.day)
    val_str = str(val).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(val_str, fmt)
        except ValueError:
            continue
    return None


def _parse_bool(val):
    """Safely parse a boolean from various input types."""
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    val_str = str(val).lower().strip()
    if val_str in ("true", "1", "yes", "y"):
        return True
    if val_str in ("false", "0", "no", "n"):
        return False
    return None


def _get(row: dict, *keys, default=None):
    """Get the first non-None value from a row using multiple possible keys."""
    for key in keys:
        val = row.get(key)
        if val is not None:
            return val
    return default


# --- Governorate inference ---

_GOVERNORATE_ALIASES = {
    "beirut": Governorate.BEIRUT,
    "mount lebanon": Governorate.MOUNT_LEBANON,
    "mount_lebanon": Governorate.MOUNT_LEBANON,
    "mt lebanon": Governorate.MOUNT_LEBANON,
    "north lebanon": Governorate.NORTH_LEBANON,
    "north_lebanon": Governorate.NORTH_LEBANON,
    "north": Governorate.NORTH_LEBANON,
    "akkar": Governorate.AKKAR,
    "begaa": Governorate.BEGAA,
    "bekaa": Governorate.BEGAA,
    "beqaa": Governorate.BEGAA,
    "baalbek-hermel": Governorate.BAALBEK_HERMEL,
    "baalbek_hermel": Governorate.BAALBEK_HERMEL,
    "baalbeck-hermel": Governorate.BAALBEK_HERMEL,
    "baalbek hermel": Governorate.BAALBEK_HERMEL,
    "south lebanon": Governorate.SOUTH_LEBANON,
    "south_lebanon": Governorate.SOUTH_LEBANON,
    "south": Governorate.SOUTH_LEBANON,
    "nabatieh": Governorate.NABATIEH,
    "nabatiyeh": Governorate.NABATIEH,
}


def _infer_governorate(value):
    """Infer governorate from a string value using aliases."""
    if value is None:
        return None
    if isinstance(value, Governorate):
        return value
    key = str(value).lower().strip()
    return _GOVERNORATE_ALIASES.get(key, infer_enum_value(value, Governorate))


# --- Entity transformers ---


def transform_provider_row(row: dict) -> dict:
    """Transform a Power BI provider row to database format."""
    return {
        "provider_id": row.get("provider_id"),
        "provider_name": _get(
            row, "provider_name", "name", "Partner", "partner",
            "Organization", "organization", "Concatenated Orgs 26",
            "Funded By", "Supported by Partner",
        ),
        "provider_type": infer_enum_value(
            _get(row, "provider_type", "type", "org_type", "PHC Ownership"),
            ProviderType,
            ProviderType.NGO,
        ),
        "website": _get(row, "website", "web_url"),
        "contact_name": _get(
            row, "contact_name", "contact",
            "CONTACT: Primary Focal Point (Full name)",
            "Focal Point", "Focal Point Contact",
            default="",
        ),
        "contact_phone": _get(
            row, "contact_phone", "phone",
            "CONTACT: Primary Focal Point (phone number)",
            "Phone",
            default="",
        ),
        "is_active": _parse_bool(
            _get(row, "is_active", "active", "status", default=True)
        ),
    }


def transform_service_row(row: dict) -> dict:
    """Transform a Power BI service row to database format."""
    return {
        "service_id": row.get("service_id"),
        "provider_id": row.get("provider_id"),
        "service_name": _get(
            row, "service_name", "name", "Service", "service",
            "Services", "services", "Support to",
        ),
        "service_code": _get(row, "service_code", "code"),
        "sector": infer_enum_value(
            _get(row, "sector", "Sector", "Sectors", "Sector.Sectors",
                 "Services.Sector", "service_sector"),
            Sector,
            Sector.FOOD_SECURITY_AGRICULTURE,
        ),
        "service_type": infer_enum_value(
            _get(row, "service_type", "service_subtype", "sub_type"),
            ServiceSubtype,
        ),
        "description": _get(
            row, "description", "desc", "Description of the support",
        ),
        "aid_type": infer_enum_value(
            _get(row, "aid_type", "modality"),
            AidType,
            AidType.IN_KIND,
        ),
        "status": infer_enum_value(
            _get(row, "status", "service_status"),
            ServiceStatus,
            ServiceStatus.PLANNED,
        ),
    }


def transform_location_row(row: dict) -> dict:
    """Transform a Power BI location row to database format."""
    # Try to extract coordinates
    latitude = _parse_float(_get(row, "latitude", "lat"))
    longitude = _parse_float(_get(row, "longitude", "lon", "lng"))

    coordinates = _get(row, "coordinates")
    if isinstance(coordinates, str) and "," in coordinates and latitude is None:
        try:
            lat_str, lon_str = coordinates.split(",")
            latitude = float(lat_str.strip())
            longitude = float(lon_str.strip())
        except (ValueError, AttributeError):
            pass

    return {
        "location_id": row.get("location_id"),
        "governorate": _infer_governorate(
            _get(row, "governorate", "Governorate1", "Governorate", "gov")
        ),
        "city": _get(row, "city", "City", "location"),
        "district": _get(row, "district", "District", "District1", "caza"),
        "locality": _get(
            row, "locality", "Locality", "Cadaster", "cadaster",
            "village", "neighborhood",
        ),
        "longitude": longitude,
        "latitude": latitude,
        "accessibility": infer_enum_value(
            _get(row, "accessibility", "Accessibility.Desc", "access"),
            Accessibility,
            Accessibility.MEDIUM,
        ),
    }


def transform_shelter_row(row: dict) -> dict:
    """Transform a Power BI shelter row to database format."""
    return {
        "shelter_id": row.get("shelter_id"),
        "shelter_name": _get(
            row, "shelter_name", "name", "site_name",
            "Centers Name", "Hospital Name",
        ),
        "shelter_type": infer_enum_value(
            _get(row, "shelter_type", "type", "site_type"),
            ShelterType,
            ShelterType.COLLECTIVE_CENTER,
        ),
        "location_id": row.get("location_id"),
        "capacity_total": _parse_int(
            _get(row, "capacity_total", "capacity")
        ) or 0,
        "population_total": _parse_int(
            _get(row, "population_total", "population")
        ),
        "households_count": _parse_int(
            _get(row, "households_count", "household_count", "households")
        ),
        "women_count": _parse_int(
            _get(row, "women_count", "women")
        ),
        "children_count": _parse_int(
            _get(row, "children_count", "children")
        ),
        "elderly_count": _parse_int(
            _get(row, "elderly_count", "elderly")
        ),
        "pwds_count": _parse_int(
            _get(row, "pwds_count", "pwd_count", "persons_with_disabilities", "pwd")
        ),
        "status": infer_enum_value(
            _get(row, "status"),
            ShelterStatus,
            ShelterStatus.ACTIVE,
        ),
        "contact_name": _get(row, "contact_name", "contact"),
        "contact_phone": _get(row, "contact_phone", "phone"),
    }


def transform_service_availability_row(row: dict) -> dict:
    """Transform a Power BI service availability row to database format."""
    # Parse disability inclusion — handle descriptive text like "Yes" / "No"
    disability_raw = _get(
        row, "disability_inclusion", "Disability.Desc",
        "disabilities_inclusion", "includes_pwd",
        "Service accessible for persons with disabilities",
    )
    disability_val = _parse_bool(disability_raw)

    return {
        "availability_id": row.get("availability_id"),
        "service_id": row.get("service_id"),
        "location_id": row.get("location_id"),
        "gender_target": infer_enum_value(
            _get(row, "gender_target", "Sex.Desc", "sex", "gender"),
            GenderTarget,
        ),
        "age_group": infer_enum_value(
            _get(row, "age_group", "AgeGroup.Desc", "Age Groups",
                 "age", "target_age", "Targeted (Age/Gender)"),
            AgeGroup,
        ),
        "disability_inclusion": disability_val,
        "accessibility_notes": _get(row, "accessibility_notes", "notes"),
        "capacity": _parse_int(_get(row, "capacity")),
        "last_verified": _parse_datetime(
            _get(row, "last_verified", "last_update", "verified_at")
        ),
    }


def transform_shelter_need_row(row: dict, shelter_id) -> dict:
    """Transform a Power BI shelter need row to database format."""
    return {
        "need_id": row.get("need_id"),
        "shelter_id": shelter_id,
        "sector": infer_enum_value(
            _get(row, "sector", "Sector", "need_sector"),
            Sector,
            Sector.FOOD_SECURITY_AGRICULTURE,
        ),
        "need_type": _get(row, "need_type", "commodity", "item_type") or "",
        "severity": infer_enum_value(
            _get(row, "severity"),
            Severity,
        ),
        "people_in_need": _parse_int(
            _get(row, "people_in_need", "affected")
        ),
        "description": _get(row, "description", "desc", "need_description"),
        "reported_at": _parse_datetime(
            _get(row, "reported_at", "report_date")
        ) or datetime.utcnow(),
        "status": infer_enum_value(
            _get(row, "status"),
            ShelterNeedStatus,
            ShelterNeedStatus.OPEN,
        ),
        "valid_from": _parse_date(_get(row, "valid_from", "start_date")),
        "valid_to": _parse_date(_get(row, "valid_to", "end_date")),
    }


def transform_aid_match_row(row: dict) -> dict:
    """Transform a Power BI aid match row to database format."""
    return {
        "id": row.get("id") or row.get("aid_match_id"),
        "service_id": row.get("service_id"),
        "need_id": row.get("need_id") or row.get("needed_id"),
        "provider_id": row.get("provider_id"),
        "quantity_provided": _parse_int(
            _get(row, "quantity_provided", "quantity", "amount")
        ),
        "status": infer_enum_value(
            _get(row, "status"),
            AidMatchStatus,
            AidMatchStatus.PLANNED,
        ),
        "date": _parse_date(
            _get(row, "date", "match_date", "delivery_date")
        ) or date.today(),
        "verified_by": _get(row, "verified_by"),
    }


def batch_transform_rows(rows: list[dict], entity_type: str, **kwargs) -> list[dict]:
    """Transform a batch of rows based on entity type."""
    transformer_map = {
        "provider": transform_provider_row,
        "service": transform_service_row,
        "shelter": transform_shelter_row,
        "location": transform_location_row,
        "shelter_need": lambda r: transform_shelter_need_row(r, kwargs.get("shelter_id")),
        "service_availability": transform_service_availability_row,
        "aid_match": transform_aid_match_row,
    }

    transformer = transformer_map.get(entity_type.lower())
    if not transformer:
        log.warning("Unknown entity type: %s, returning rows as-is", entity_type)
        return rows

    transformed = []
    for row in rows:
        try:
            transformed.append(transformer(row))
        except Exception as e:
            log.warning("Failed to transform row of type %s: %s", entity_type, e)
    return transformed


def identify_entity_types(columns: list[str]) -> set[str]:
    """Identify entity types from column names using heuristic matching."""
    column_str = " ".join(columns).lower()
    entity_types = set()

    patterns = {
        "provider": [
            "provider", "organisation", "organization", "partner",
            "focal point", "funded by",
        ],
        "service": ["service", "services", "sector", "sectors", "support to"],
        "shelter": [
            "shelter", "camp", "site", "centers name", "hospital name",
            "health facility",
        ],
        "location": [
            "location", "city", "district", "latitude", "longitude",
            "governorate", "locality", "cadaster",
        ],
        "aid_match": ["match", "matched", "distribution"],
        "shelter_need": ["need", "commodity", "item"],
        "service_availability": [
            "availability", "sex", "gender", "disability",
            "accessible", "age group", "agegroup",
        ],
    }

    for entity_type, keywords in patterns.items():
        if any(keyword in column_str for keyword in keywords):
            entity_types.add(entity_type)

    return entity_types
