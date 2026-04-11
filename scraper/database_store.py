"""Store scraped Power BI data in structured database format."""

import logging
from datetime import date, datetime
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config import DATABASE_URL
from scraper.models import (
    AidMatch,
    AidMatchStatus,
    AidType,
    Base,
    Location,
    Provider,
    ProviderType,
    Sector,
    Service,
    ServiceAvailability,
    ServiceStatus,
    Shelter,
    ShelterNeed,
    ShelterNeedStatus,
    ShelterStatus,
    ShelterType,
)

log = logging.getLogger(__name__)

_engine = None
_SessionLocal = None


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


def get_session():
    """Get a database session."""
    _init_database()
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
                provider_type=provider_data.get("provider_type", ProviderType.NGO),
                website=provider_data.get("website"),
                contact_name=provider_data.get("contact_name", ""),
                contact_phone=provider_data.get("contact_phone", ""),
                is_active=provider_data.get("is_active", True),
            )
            session.add(provider)
        else:
            if provider_data.get("website"):
                provider.website = provider_data["website"]
            if provider_data.get("contact_name"):
                provider.contact_name = provider_data["contact_name"]
            if provider_data.get("contact_phone"):
                provider.contact_phone = provider_data["contact_phone"]
            if provider_data.get("is_active") is not None:
                provider.is_active = provider_data["is_active"]

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
                service_code=service_data.get("service_code"),
                sector=service_data.get("sector", Sector.FOOD_SECURITY_AGRICULTURE),
                service_type=service_data.get("service_type"),
                description=service_data.get("description"),
                aid_type=service_data.get("aid_type", AidType.IN_KIND),
                status=service_data.get("status", ServiceStatus.PLANNED),
            )
            session.add(service)
        else:
            if service_data.get("status"):
                service.status = service_data["status"]
            if service_data.get("service_code"):
                service.service_code = service_data["service_code"]
            if service_data.get("description"):
                service.description = service_data["description"]
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
                district=location_data.get("district"),
                locality=location_data.get("locality"),
                longitude=location_data.get("longitude"),
                latitude=location_data.get("latitude"),
                accessibility=location_data["accessibility"],
            )
            session.add(location)
        else:
            if location_data.get("district"):
                location.district = location_data["district"]
            if location_data.get("locality"):
                location.locality = location_data["locality"]
            if location_data.get("latitude"):
                location.latitude = location_data["latitude"]
            if location_data.get("longitude"):
                location.longitude = location_data["longitude"]

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
            )
            session.add(shelter)
        else:
            shelter.population_total = shelter_data.get(
                "population_total", shelter.population_total
            )
            shelter.households_count = shelter_data.get(
                "households_count", shelter.households_count
            )
            shelter.women_count = shelter_data.get("women_count", shelter.women_count)
            shelter.children_count = shelter_data.get("children_count", shelter.children_count)
            shelter.elderly_count = shelter_data.get("elderly_count", shelter.elderly_count)
            shelter.pwds_count = shelter_data.get("pwds_count", shelter.pwds_count)
            shelter.status = shelter_data.get("status", shelter.status)
            shelter.contact_name = shelter_data.get("contact_name", shelter.contact_name)
            shelter.contact_phone = shelter_data.get("contact_phone", shelter.contact_phone)
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
