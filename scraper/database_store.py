"""Store scraped Power BI data in structured database format."""

import logging
from datetime import date, datetime
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from config import DATABASE_URL
from scraper.models import (
    AidMatch,
    AidMatchStatus,
    AidType,
    Base,
    Category,
    CategoryType,
    Location,
    Provider,
    ProviderType,
    Sector,
    Service,
    ServiceAvailability,
    ServiceStatus,
    ServiceSubtype,
    Shelter,
    ShelterNeed,
    ShelterNeedStatus,
    ShelterStatus,
    ShelterType,
)

log = logging.getLogger(__name__)

_engine = None
_SessionLocal: sessionmaker[Session] | None = None


def _init_database():
    """Initialize database engine and session factory."""
    global _engine, _SessionLocal

    if _engine is not None:
        return

    if not DATABASE_URL:
        raise RuntimeError(
            "Database not configured. Set DATABASE_URL environment variable. "
            "Example: postgresql://user:password@localhost/lbresponse"
        )

    _engine = create_engine(DATABASE_URL, echo=False)
    _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_engine)

    Base.metadata.create_all(_engine)
    log.info("Database initialized: %s", DATABASE_URL)
    try:
        seed_categories()
    except Exception:
        log.exception("Failed to seed categories (will retry on next run)")


def get_session() -> Session:
    """Get a database session."""
    _init_database()
    assert _SessionLocal is not None, "Database not initialized"
    return _SessionLocal()


def store_provider(provider_data: dict) -> Provider:
    """Store or update a provider record."""
    session = get_session()
    try:
        provider = (
            session.query(Provider)
            .filter_by(provider_name=provider_data.get("provider_name"))
            .first()
        )

        if provider is None:
            provider = Provider(
                provider_id=provider_data.get("provider_id") or uuid4(),
                provider_name=provider_data["provider_name"],
                provider_name_ar=provider_data.get("provider_name_ar"),
                provider_type=provider_data.get("provider_type", ProviderType.NGO),
                description=provider_data.get("description"),
                description_ar=provider_data.get("description_ar"),
                website=provider_data.get("website"),
                contact_name=provider_data.get("contact_name", ""),
                contact_phone=provider_data.get("contact_phone", ""),
                contact_type=provider_data.get("contact_type"),
                is_active=provider_data.get("is_active", True),
                pinned=provider_data.get("pinned", False),
                verified=provider_data.get("verified", False),
            )
            session.add(provider)
        else:
            for field in (
                "provider_name_ar",
                "description",
                "description_ar",
                "website",
                "contact_name",
                "contact_phone",
                "contact_type",
            ):
                if provider_data.get(field) is not None:
                    setattr(provider, field, provider_data[field])
            for flag in ("is_active", "pinned", "verified"):
                if provider_data.get(flag) is not None:
                    setattr(provider, flag, provider_data[flag])

        session.commit()
        log.info("Stored provider: %s", provider.provider_name)
        return provider
    except Exception as e:
        session.rollback()
        log.error("Failed to store provider: %s", e)
        raise
    finally:
        session.close()


def store_service(service_data: dict, provider_id) -> Service:
    """Store or update a service record."""
    session = get_session()
    try:
        service = (
            session.query(Service)
            .filter_by(
                service_name=service_data.get("service_name"),
                provider_id=provider_id,
            )
            .first()
        )

        if service is None:
            service = Service(
                service_id=service_data.get("service_id") or uuid4(),
                provider_id=provider_id,
                service_name=service_data["service_name"],
                service_name_ar=service_data.get("service_name_ar"),
                service_code=service_data.get("service_code"),
                sector=service_data.get("sector", Sector.FOOD_SECURITY_AGRICULTURE),
                service_type=service_data.get("service_type"),
                description=service_data.get("description"),
                description_ar=service_data.get("description_ar"),
                aid_type=service_data.get("aid_type", AidType.IN_KIND),
                status=service_data.get("status", ServiceStatus.PLANNED),
                pinned=service_data.get("pinned", False),
                verified=service_data.get("verified", False),
            )
            session.add(service)
        else:
            for field in (
                "service_name_ar",
                "service_code",
                "description",
                "description_ar",
                "status",
            ):
                if service_data.get(field) is not None:
                    setattr(service, field, service_data[field])
            for flag in ("pinned", "verified"):
                if service_data.get(flag) is not None:
                    setattr(service, flag, service_data[flag])
            service.updated_at = datetime.utcnow()

        session.commit()
        log.info("Stored service: %s", service.service_name)
        return service
    except Exception as e:
        session.rollback()
        log.error("Failed to store service: %s", e)
        raise
    finally:
        session.close()


def store_location(location_data: dict) -> Location:
    """Store or update a location record."""
    session = get_session()
    try:
        location = None

        # Try to find existing by city + governorate
        if location_data.get("city") and location_data.get("governorate"):
            location = (
                session.query(Location)
                .filter_by(
                    city=location_data["city"],
                    governorate=location_data["governorate"],
                )
                .first()
            )

        # Fallback: match by city only
        if location is None and location_data.get("city"):
            location = session.query(Location).filter_by(city=location_data["city"]).first()

        if location is None:
            location = Location(
                location_id=location_data.get("location_id") or uuid4(),
                governorate=location_data["governorate"],
                city=location_data["city"],
                city_ar=location_data.get("city_ar"),
                district=location_data.get("district"),
                district_ar=location_data.get("district_ar"),
                locality=location_data.get("locality"),
                locality_ar=location_data.get("locality_ar"),
                longitude=location_data.get("longitude"),
                latitude=location_data.get("latitude"),
                accessibility=location_data["accessibility"],
            )
            session.add(location)
        else:
            for field in (
                "city_ar",
                "district",
                "district_ar",
                "locality",
                "locality_ar",
                "latitude",
                "longitude",
            ):
                if location_data.get(field) is not None:
                    setattr(location, field, location_data[field])

        session.commit()
        log.info("Stored location: %s", location.city)
        return location
    except Exception as e:
        session.rollback()
        log.error("Failed to store location: %s", e)
        raise
    finally:
        session.close()


def store_service_availability(availability_data: dict) -> ServiceAvailability:
    """Store a service availability record."""
    session = get_session()
    try:
        availability = ServiceAvailability(
            availability_id=availability_data.get("availability_id") or uuid4(),
            service_id=availability_data["service_id"],
            location_id=availability_data["location_id"],
            gender_target=availability_data.get("gender_target"),
            age_group=availability_data.get("age_group"),
            disability_inclusion=availability_data.get("disability_inclusion"),
            accessibility_notes=availability_data.get("accessibility_notes"),
            capacity=availability_data.get("capacity"),
            last_verified=availability_data.get("last_verified"),
        )
        session.add(availability)
        session.commit()
        log.info("Stored service availability")
        return availability
    except Exception as e:
        session.rollback()
        log.error("Failed to store service availability: %s", e)
        raise
    finally:
        session.close()


def store_shelter(shelter_data: dict, location_id) -> Shelter:
    """Store or update a shelter record."""
    session = get_session()
    try:
        shelter = (
            session.query(Shelter).filter_by(shelter_name=shelter_data.get("shelter_name")).first()
        )

        if shelter is None:
            shelter = Shelter(
                shelter_id=shelter_data.get("shelter_id") or uuid4(),
                shelter_name=shelter_data["shelter_name"],
                shelter_name_ar=shelter_data.get("shelter_name_ar"),
                shelter_type=shelter_data.get("shelter_type", ShelterType.COLLECTIVE_CENTER),
                location_id=location_id,
                capacity_total=shelter_data.get("capacity_total", 0),
                population_total=shelter_data.get("population_total"),
                households_count=shelter_data.get("households_count"),
                women_count=shelter_data.get("women_count"),
                children_count=shelter_data.get("children_count"),
                elderly_count=shelter_data.get("elderly_count"),
                pwds_count=shelter_data.get("pwds_count"),
                status=shelter_data.get("status", ShelterStatus.ACTIVE),
                contact_name=shelter_data.get("contact_name"),
                contact_phone=shelter_data.get("contact_phone"),
                contact_type=shelter_data.get("contact_type"),
                pinned=shelter_data.get("pinned", False),
                verified=shelter_data.get("verified", False),
            )
            session.add(shelter)
        else:
            for field in (
                "shelter_name_ar",
                "population_total",
                "households_count",
                "women_count",
                "children_count",
                "elderly_count",
                "pwds_count",
                "status",
                "contact_name",
                "contact_phone",
                "contact_type",
            ):
                if shelter_data.get(field) is not None:
                    setattr(shelter, field, shelter_data[field])
            for flag in ("pinned", "verified"):
                if shelter_data.get(flag) is not None:
                    setattr(shelter, flag, shelter_data[flag])
            shelter.last_update = datetime.utcnow()

        session.commit()
        log.info("Stored shelter: %s", shelter.shelter_name)
        return shelter
    except Exception as e:
        session.rollback()
        log.error("Failed to store shelter: %s", e)
        raise
    finally:
        session.close()


def store_shelter_need(shelter_need_data: dict, shelter_id) -> ShelterNeed:
    """Store a shelter need record."""
    session = get_session()
    try:
        shelter_need = ShelterNeed(
            need_id=shelter_need_data.get("need_id") or uuid4(),
            shelter_id=shelter_id,
            sector=shelter_need_data["sector"],
            need_type=shelter_need_data["need_type"],
            severity=shelter_need_data.get("severity"),
            people_in_need=shelter_need_data.get("people_in_need"),
            description=shelter_need_data.get("description"),
            description_ar=shelter_need_data.get("description_ar"),
            reported_at=shelter_need_data.get("reported_at", datetime.utcnow()),
            status=shelter_need_data.get("status", ShelterNeedStatus.OPEN),
            valid_from=shelter_need_data.get("valid_from"),
            valid_to=shelter_need_data.get("valid_to"),
        )
        session.add(shelter_need)
        session.commit()
        log.info("Stored shelter need: %s", shelter_need_data.get("need_type"))
        return shelter_need
    except Exception as e:
        session.rollback()
        log.error("Failed to store shelter need: %s", e)
        raise
    finally:
        session.close()


def store_aid_match(aid_match_data: dict) -> AidMatch:
    """Store an aid match record."""
    session = get_session()
    try:
        aid_match = AidMatch(
            id=aid_match_data.get("id") or uuid4(),
            service_id=aid_match_data["service_id"],
            need_id=aid_match_data["need_id"],
            provider_id=aid_match_data.get("provider_id"),
            quantity_provided=aid_match_data.get("quantity_provided"),
            status=aid_match_data.get("status", AidMatchStatus.PLANNED),
            date=aid_match_data.get("date", date.today()),
            verified_by=aid_match_data.get("verified_by"),
        )
        session.add(aid_match)
        session.commit()
        log.info("Stored aid match")
        return aid_match
    except Exception as e:
        session.rollback()
        log.error("Failed to store aid match: %s", e)
        raise
    finally:
        session.close()


_DEFAULT_CATEGORY_LABELS: dict[CategoryType, dict[str, tuple[str, str]]] = {
    CategoryType.SECTOR: {
        Sector.FOOD_SECURITY_AGRICULTURE: ("Food Security & Agriculture", "الأمن الغذائي والزراعة"),
        Sector.WASH: ("WASH", "المياه والصرف الصحي والنظافة"),
        Sector.NUTRITION: ("Nutrition", "التغذية"),
        Sector.CLOTHING: ("Clothing", "الملابس"),
    },
    CategoryType.SERVICE_SUBTYPE: {
        ServiceSubtype.NUTRITION: ("Nutrition", "التغذية"),
        ServiceSubtype.WASH: ("WASH", "المياه والصرف الصحي والنظافة"),
        ServiceSubtype.SLEEPING_SUPPLIES: ("Sleeping Supplies", "مستلزمات النوم"),
        ServiceSubtype.CLOTHING: ("Clothing", "الملابس"),
    },
    CategoryType.PROVIDER_TYPE: {
        ProviderType.NGO: ("NGO", "منظمة غير حكومية"),
        ProviderType.UN: ("UN Agency", "وكالة أممية"),
        ProviderType.LOCAL_ORGANIZATION: ("Local Organization", "منظمة محلية"),
        ProviderType.GOVERNMENT: ("Government", "حكومة"),
        ProviderType.PRIVATE_DONOR: ("Private Donor", "مانح خاص"),
        ProviderType.PRIVATE_COMPANY: ("Private Company", "شركة خاصة"),
    },
    CategoryType.SHELTER_TYPE: {
        ShelterType.COLLECTIVE_CENTER: ("Collective Center", "مركز جماعي"),
        ShelterType.INFORMAL_SETTLEMENT: ("Informal Settlement", "تجمع غير رسمي"),
        ShelterType.HOST_COMMUNITY: ("Host Community", "مجتمع مضيف"),
    },
}


def seed_categories() -> int:
    """Seed the categories table with default bilingual labels. Idempotent."""
    session = get_session()
    inserted = 0
    try:
        for cat_type, entries in _DEFAULT_CATEGORY_LABELS.items():
            for order, (key, (en, ar)) in enumerate(entries.items()):
                existing = (
                    session.query(Category).filter_by(category_type=cat_type, key=str(key)).first()
                )
                if existing is None:
                    session.add(
                        Category(
                            category_type=cat_type,
                            key=str(key),
                            en_label=en,
                            ar_label=ar,
                            sort_order=order,
                        )
                    )
                    inserted += 1
                elif existing.ar_label is None:
                    existing.ar_label = ar
        session.commit()
        if inserted:
            log.info("Seeded %d category labels", inserted)
        return inserted
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _to_plain(value):
    """Convert SQLAlchemy attribute to a JSON-safe value."""
    if value is None:
        return None
    if isinstance(value, datetime | date):
        return value.isoformat()
    return str(value) if hasattr(value, "hex") else value


def _row_to_dict(row, columns: list[str]) -> dict:
    return {col: _to_plain(getattr(row, col)) for col in columns}


_ENTITY_COLUMNS: dict[str, tuple[type, list[str]]] = {
    "providers": (
        Provider,
        [
            "provider_id",
            "provider_name",
            "provider_name_ar",
            "provider_type",
            "description",
            "description_ar",
            "website",
            "contact_name",
            "contact_phone",
            "contact_type",
            "is_active",
            "pinned",
            "verified",
            "created_at",
        ],
    ),
    "services": (
        Service,
        [
            "service_id",
            "provider_id",
            "service_name",
            "service_name_ar",
            "service_code",
            "sector",
            "service_type",
            "description",
            "description_ar",
            "aid_type",
            "status",
            "pinned",
            "verified",
            "created_at",
            "updated_at",
        ],
    ),
    "locations": (
        Location,
        [
            "location_id",
            "governorate",
            "city",
            "city_ar",
            "district",
            "district_ar",
            "locality",
            "locality_ar",
            "longitude",
            "latitude",
            "accessibility",
        ],
    ),
    "service_availability": (
        ServiceAvailability,
        [
            "availability_id",
            "service_id",
            "location_id",
            "gender_target",
            "age_group",
            "disability_inclusion",
            "accessibility_notes",
            "capacity",
            "last_verified",
        ],
    ),
    "shelters": (
        Shelter,
        [
            "shelter_id",
            "shelter_name",
            "shelter_name_ar",
            "shelter_type",
            "location_id",
            "capacity_total",
            "population_total",
            "households_count",
            "women_count",
            "children_count",
            "elderly_count",
            "pwds_count",
            "status",
            "contact_name",
            "contact_phone",
            "contact_type",
            "pinned",
            "verified",
            "last_update",
        ],
    ),
    "shelter_needs": (
        ShelterNeed,
        [
            "need_id",
            "shelter_id",
            "sector",
            "need_type",
            "severity",
            "people_in_need",
            "description",
            "description_ar",
            "reported_at",
            "status",
            "valid_from",
            "valid_to",
        ],
    ),
    "aid_matches": (
        AidMatch,
        [
            "id",
            "service_id",
            "need_id",
            "provider_id",
            "quantity_provided",
            "status",
            "date",
            "verified_by",
        ],
    ),
}

_ID_COLUMN: dict[str, str] = {
    "providers": "provider_id",
    "services": "service_id",
    "locations": "location_id",
    "service_availability": "availability_id",
    "shelters": "shelter_id",
    "shelter_needs": "need_id",
    "aid_matches": "id",
}


def export_entities_snapshot() -> dict[str, dict[str, dict]]:
    """Export all entities and categories as id-keyed dicts for mirroring."""
    session = get_session()
    try:
        snapshot: dict[str, dict[str, dict]] = {}
        for name, (model, columns) in _ENTITY_COLUMNS.items():
            id_col = _ID_COLUMN[name]
            rows: list = session.query(model).all()
            snapshot[name] = {str(getattr(row, id_col)): _row_to_dict(row, columns) for row in rows}

        category_rows = session.query(Category).all()
        categories: dict[str, dict[str, dict]] = {}
        for row in category_rows:
            bucket = categories.setdefault(str(row.category_type), {})
            bucket[str(row.key)] = {
                "key": row.key,
                "en_label": row.en_label,
                "ar_label": row.ar_label,
                "sort_order": row.sort_order,
            }
        snapshot["categories"] = categories  # type: ignore[assignment]
        return snapshot
    finally:
        session.close()
