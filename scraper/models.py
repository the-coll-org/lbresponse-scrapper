"""SQLAlchemy ORM models for Lebanon Response database schema."""

from datetime import datetime
from enum import StrEnum
from uuid import uuid4

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    create_engine,
)
from sqlalchemy import (
    Enum as SQLEnum,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()


# --- Enums ---


class ProviderType(StrEnum):
    NGO = "ngo"
    UN = "un"
    LOCAL_ORGANIZATION = "local_organization"
    GOVERNMENT = "government"
    PRIVATE_DONOR = "private_donor"
    PRIVATE_COMPANY = "private_company"


class Governorate(StrEnum):
    BEIRUT = "beirut"
    MOUNT_LEBANON = "mount_lebanon"
    NORTH_LEBANON = "north_lebanon"
    AKKAR = "akkar"
    BEGAA = "begaa"
    BAALBEK_HERMEL = "baalbek_hermel"
    SOUTH_LEBANON = "south_lebanon"
    NABATIEH = "nabatieh"


class Sector(StrEnum):
    FOOD_SECURITY_AGRICULTURE = "food_security_agriculture"
    WASH = "wash"
    NUTRITION = "nutrition"
    CLOTHING = "clothing"


class ServiceSubtype(StrEnum):
    NUTRITION = "nutrition"
    WASH = "wash"
    SLEEPING_SUPPLIES = "sleeping_supplies"
    CLOTHING = "clothing"


class AidType(StrEnum):
    CASH = "cash"
    IN_KIND = "in_kind"


class ServiceStatus(StrEnum):
    COMPLETED = "completed"
    SUSPENDED = "suspended"
    PLANNED = "planned"
    IN_PROGRESS = "in_progress"


class Accessibility(StrEnum):
    EASY = "easy"
    MEDIUM = "medium"
    HARD = "hard"


class GenderTarget(StrEnum):
    M = "m"
    F = "f"
    ALL = "all"


class AgeGroup(StrEnum):
    CHILDREN = "children"
    YOUTH = "youth"
    ADULT = "adult"
    ALL = "all"


class ShelterType(StrEnum):
    COLLECTIVE_CENTER = "collective_center"
    INFORMAL_SETTLEMENT = "informal_settlement"
    HOST_COMMUNITY = "host_community"


class ShelterStatus(StrEnum):
    ACTIVE = "active"
    CLOSED = "closed"


class Severity(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ShelterNeedStatus(StrEnum):
    OPEN = "open"
    IN_PROGRESS = "in_progress"
    CLOSED = "closed"


class AidMatchStatus(StrEnum):
    PLANNED = "planned"
    DELIVERED = "delivered"
    PARTIAL = "partial"


# --- Models ---


class Provider(Base):
    __tablename__ = "providers"

    provider_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    provider_name = Column(String(255), nullable=False)
    provider_type = Column(SQLEnum(ProviderType, name="provider_type_enum"), nullable=False)
    website = Column(String(255), nullable=True)
    contact_name = Column(String(255), nullable=False)
    contact_phone = Column(String(50), nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    services = relationship("Service", back_populates="provider", cascade="all, delete-orphan")
    aid_matches = relationship("AidMatch", back_populates="provider", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Provider {self.provider_id}: {self.provider_name}>"


class Service(Base):
    __tablename__ = "services"

    service_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    provider_id = Column(UUID(as_uuid=True), ForeignKey("providers.provider_id"), nullable=False)
    service_name = Column(String(255), nullable=False)
    service_code = Column(String(50), nullable=True)
    sector = Column(SQLEnum(Sector, name="sector_enum"), nullable=False)
    service_type = Column(SQLEnum(ServiceSubtype, name="service_subtype_enum"), nullable=True)
    description = Column(Text, nullable=True)
    aid_type = Column(SQLEnum(AidType, name="aid_type_enum"), nullable=False)
    status = Column(SQLEnum(ServiceStatus, name="service_status_enum"), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    provider = relationship("Provider", back_populates="services")
    availabilities = relationship(
        "ServiceAvailability", back_populates="service", cascade="all, delete-orphan"
    )
    aid_matches = relationship("AidMatch", back_populates="service", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Service {self.service_id}: {self.service_name}>"


class Location(Base):
    __tablename__ = "locations"

    location_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    governorate = Column(SQLEnum(Governorate, name="governorate_enum"), nullable=False)
    city = Column(String(100), nullable=False)
    district = Column(String(100), nullable=True)
    locality = Column(String(255), nullable=True)
    longitude = Column(Float, nullable=True)
    latitude = Column(Float, nullable=True)
    accessibility = Column(SQLEnum(Accessibility, name="accessibility_enum"), nullable=False)

    # Relationships
    shelters = relationship("Shelter", back_populates="location", cascade="all, delete-orphan")
    service_availabilities = relationship(
        "ServiceAvailability", back_populates="location", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<Location {self.location_id}: {self.city}>"


class ServiceAvailability(Base):
    __tablename__ = "service_availability"

    availability_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    service_id = Column(UUID(as_uuid=True), ForeignKey("services.service_id"), nullable=False)
    location_id = Column(UUID(as_uuid=True), ForeignKey("locations.location_id"), nullable=False)
    gender_target = Column(SQLEnum(GenderTarget, name="gender_target_enum"), nullable=True)
    age_group = Column(SQLEnum(AgeGroup, name="age_group_enum"), nullable=True)
    disability_inclusion = Column(Boolean, nullable=True)
    accessibility_notes = Column(Text, nullable=True)
    capacity = Column(Integer, nullable=True)
    last_verified = Column(DateTime, nullable=True)

    # Relationships
    service = relationship("Service", back_populates="availabilities")
    location = relationship("Location", back_populates="service_availabilities")

    def __repr__(self):
        return f"<ServiceAvailability {self.availability_id}>"


class Shelter(Base):
    __tablename__ = "shelters"

    shelter_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    shelter_name = Column(String(255), nullable=False)
    shelter_type = Column(SQLEnum(ShelterType, name="shelter_type_enum"), nullable=False)
    location_id = Column(UUID(as_uuid=True), ForeignKey("locations.location_id"), nullable=False)
    capacity_total = Column(Integer, nullable=False)
    population_total = Column(Integer, nullable=True)
    households_count = Column(Integer, nullable=True)
    women_count = Column(Integer, nullable=True)
    children_count = Column(Integer, nullable=True)
    elderly_count = Column(Integer, nullable=True)
    pwds_count = Column(Integer, nullable=True)
    status = Column(SQLEnum(ShelterStatus, name="shelter_status_enum"), nullable=False)
    contact_name = Column(String(255), nullable=True)
    contact_phone = Column(String(50), nullable=True)
    last_update = Column(DateTime, nullable=False, default=datetime.utcnow)

    # Relationships
    location = relationship("Location", back_populates="shelters")
    shelter_needs = relationship(
        "ShelterNeed", back_populates="shelter", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<Shelter {self.shelter_id}: {self.shelter_name}>"


class ShelterNeed(Base):
    __tablename__ = "shelter_needs"

    need_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    shelter_id = Column(UUID(as_uuid=True), ForeignKey("shelters.shelter_id"), nullable=False)
    sector = Column(SQLEnum(Sector, name="sector_enum"), nullable=False)
    need_type = Column(String(100), nullable=False)
    severity = Column(SQLEnum(Severity, name="severity_enum"), nullable=True)
    people_in_need = Column(Integer, nullable=True)
    description = Column(Text, nullable=True)
    reported_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    status = Column(SQLEnum(ShelterNeedStatus, name="shelter_need_status_enum"), nullable=False)
    valid_from = Column(Date, nullable=True)
    valid_to = Column(Date, nullable=True)

    # Relationships
    shelter = relationship("Shelter", back_populates="shelter_needs")
    aid_matches = relationship("AidMatch", back_populates="need", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<ShelterNeed {self.need_id}: {self.need_type}>"


class AidMatch(Base):
    __tablename__ = "aid_matches"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    service_id = Column(UUID(as_uuid=True), ForeignKey("services.service_id"), nullable=False)
    need_id = Column(UUID(as_uuid=True), ForeignKey("shelter_needs.need_id"), nullable=False)
    provider_id = Column(UUID(as_uuid=True), ForeignKey("providers.provider_id"), nullable=True)
    quantity_provided = Column(Integer, nullable=True)
    status = Column(SQLEnum(AidMatchStatus, name="aid_match_status_enum"), nullable=False)
    date = Column(Date, nullable=False)
    verified_by = Column(String(100), nullable=True)

    # Relationships
    service = relationship("Service", back_populates="aid_matches")
    need = relationship("ShelterNeed", back_populates="aid_matches")
    provider = relationship("Provider", back_populates="aid_matches")

    def __repr__(self):
        return f"<AidMatch {self.id}>"


# --- Database management ---


def create_engine_from_config(database_url: str | None = None) -> object:
    if database_url is None:
        database_url = "sqlite:///./lbresponse.db"
    return create_engine(database_url, echo=False)


def init_db(database_url: str | None = None) -> None:
    engine = create_engine_from_config(database_url)
    Base.metadata.create_all(engine)


def get_session(database_url: str | None = None):
    engine = create_engine_from_config(database_url)
    session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = session_factory()
    try:
        yield session
    finally:
        session.close()
