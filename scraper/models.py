"""SQLAlchemy ORM models for Lebanon Response database schema."""

from datetime import datetime
from enum import StrEnum
from uuid import uuid4

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Engine,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy import (
    Enum as SQLEnum,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker


class Base(DeclarativeBase):
    pass


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


class ContactType(StrEnum):
    PHONE = "phone"
    WHATSAPP = "whatsapp"
    EMAIL = "email"
    TELEGRAM = "telegram"
    SMS = "sms"
    OTHER = "other"


class CategoryType(StrEnum):
    SECTOR = "sector"
    SERVICE_SUBTYPE = "service_subtype"
    NEED_TYPE = "need_type"
    PROVIDER_TYPE = "provider_type"
    SHELTER_TYPE = "shelter_type"


# --- Models ---


class Provider(Base):
    __tablename__ = "providers"

    provider_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    provider_name = Column(String(255), nullable=False)
    provider_name_ar = Column(String(255), nullable=True)
    provider_type: Mapped[ProviderType] = mapped_column(
        SQLEnum(ProviderType, name="provider_type_enum"), nullable=False
    )
    description = Column(Text, nullable=True)
    description_ar = Column(Text, nullable=True)
    website = Column(String(255), nullable=True)
    contact_name = Column(String(255), nullable=False)
    contact_phone = Column(String(50), nullable=False)
    contact_type: Mapped[ContactType | None] = mapped_column(
        SQLEnum(ContactType, name="contact_type_enum"), nullable=True
    )
    is_active = Column(Boolean, nullable=False, default=True)
    is_name_valid = Column(Boolean, nullable=False, default=True)
    pinned = Column(Boolean, nullable=False, default=False)
    verified = Column(Boolean, nullable=False, default=False)
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
    service_name_ar = Column(String(255), nullable=True)
    service_code = Column(String(50), nullable=True)
    sector: Mapped[Sector] = mapped_column(SQLEnum(Sector, name="sector_enum"), nullable=False)
    service_type: Mapped[ServiceSubtype | None] = mapped_column(
        SQLEnum(ServiceSubtype, name="service_subtype_enum"), nullable=True
    )
    description = Column(Text, nullable=True)
    description_ar = Column(Text, nullable=True)
    aid_type: Mapped[AidType] = mapped_column(
        SQLEnum(AidType, name="aid_type_enum"), nullable=False
    )
    status: Mapped[ServiceStatus] = mapped_column(
        SQLEnum(ServiceStatus, name="service_status_enum"), nullable=False
    )
    pinned = Column(Boolean, nullable=False, default=False)
    verified = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

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
    governorate: Mapped[Governorate] = mapped_column(
        SQLEnum(Governorate, name="governorate_enum"), nullable=False
    )
    city = Column(String(100), nullable=False)
    city_ar = Column(String(100), nullable=True)
    district = Column(String(100), nullable=True)
    district_ar = Column(String(100), nullable=True)
    locality = Column(String(255), nullable=True)
    locality_ar = Column(String(255), nullable=True)
    longitude = Column(Float, nullable=True)
    latitude = Column(Float, nullable=True)
    accessibility: Mapped[Accessibility] = mapped_column(
        SQLEnum(Accessibility, name="accessibility_enum"), nullable=False
    )

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
    gender_target: Mapped[GenderTarget | None] = mapped_column(
        SQLEnum(GenderTarget, name="gender_target_enum"), nullable=True
    )
    age_group: Mapped[AgeGroup | None] = mapped_column(
        SQLEnum(AgeGroup, name="age_group_enum"), nullable=True
    )
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
    shelter_name_ar = Column(String(255), nullable=True)
    shelter_type: Mapped[ShelterType] = mapped_column(
        SQLEnum(ShelterType, name="shelter_type_enum"), nullable=False
    )
    location_id = Column(UUID(as_uuid=True), ForeignKey("locations.location_id"), nullable=False)
    capacity_total = Column(Integer, nullable=False)
    population_total = Column(Integer, nullable=True)
    households_count = Column(Integer, nullable=True)
    women_count = Column(Integer, nullable=True)
    children_count = Column(Integer, nullable=True)
    elderly_count = Column(Integer, nullable=True)
    pwds_count = Column(Integer, nullable=True)
    status: Mapped[ShelterStatus] = mapped_column(
        SQLEnum(ShelterStatus, name="shelter_status_enum"), nullable=False
    )
    contact_name = Column(String(255), nullable=True)
    contact_phone = Column(String(50), nullable=True)
    contact_type: Mapped[ContactType | None] = mapped_column(
        SQLEnum(ContactType, name="contact_type_enum"), nullable=True
    )
    pinned = Column(Boolean, nullable=False, default=False)
    verified = Column(Boolean, nullable=False, default=False)
    last_update: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

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
    sector: Mapped[Sector] = mapped_column(SQLEnum(Sector, name="sector_enum"), nullable=False)
    need_type = Column(String(100), nullable=False)
    severity: Mapped[Severity | None] = mapped_column(
        SQLEnum(Severity, name="severity_enum"), nullable=True
    )
    people_in_need = Column(Integer, nullable=True)
    description = Column(Text, nullable=True)
    description_ar = Column(Text, nullable=True)
    reported_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    status: Mapped[ShelterNeedStatus] = mapped_column(
        SQLEnum(ShelterNeedStatus, name="shelter_need_status_enum"), nullable=False
    )
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
    status: Mapped[AidMatchStatus] = mapped_column(
        SQLEnum(AidMatchStatus, name="aid_match_status_enum"), nullable=False
    )
    date = Column(Date, nullable=False)
    verified_by = Column(String(100), nullable=True)

    # Relationships
    service = relationship("Service", back_populates="aid_matches")
    need = relationship("ShelterNeed", back_populates="aid_matches")
    provider = relationship("Provider", back_populates="aid_matches")

    def __repr__(self):
        return f"<AidMatch {self.id}>"


class Category(Base):
    """Bilingual display labels for enum values (sector, service_type, need_type, etc.)."""

    __tablename__ = "categories"
    __table_args__ = (UniqueConstraint("category_type", "key", name="uq_category_type_key"),)

    category_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    category_type: Mapped[CategoryType] = mapped_column(
        SQLEnum(CategoryType, name="category_type_enum"), nullable=False
    )
    key = Column(String(100), nullable=False)
    en_label = Column(String(255), nullable=False)
    ar_label = Column(String(255), nullable=True)
    sort_order = Column(Integer, nullable=False, default=0)

    def __repr__(self):
        return f"<Category {self.category_type}:{self.key}>"


# --- Database management ---


def create_engine_from_config(database_url: str | None = None) -> Engine:
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
