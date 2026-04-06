# Lebanon Response Data Model

## Overview

Normalized storage of Lebanon Response data:
- **Providers**: Service-providing organizations
- **Services**: Services offered by providers
- **Locations**: Geographic locations (governorate/city/district)
- **Service Availability**: Services available at specific locations
- **Shelters**: Shelter facilities and populations
- **Shelter Needs**: Specific needs of shelters
- **Aid Matches**: Matching between services and needs

## Database

Uses SQLAlchemy ORM. Set `DATABASE_URL` environment variable:

```bash
export DATABASE_URL="postgresql://user:password@localhost/lbresponse"
# Default: sqlite:///./lbresponse.db
```

## Entity Relationships

```
Provider (1) ──── (N) Service (1) ──── (N) ServiceAvailability
                       │                        │
                       │                   Location (1) ── (N) Shelter
                       │                                        │
                  AidMatch ──────────────── ShelterNeed (N) ────┘
```

## Tables

### providers

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| provider_id | UUID | yes | primary key |
| provider_name | string | yes | |
| provider_type | ENUM | yes | NGO, UN, local_organization, government, private_donor, private_company |
| website | string | no | |
| contact_name | string | yes | |
| contact_phone | string | yes | |
| is_active | boolean | yes | default true |
| created_at | timestamp | yes | |

### services

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| service_id | UUID | yes | primary key |
| provider_id | UUID | yes | FK → providers |
| service_name | string | yes | |
| service_code | string | no | e.g. L16 |
| sector | ENUM | yes | food_security_agriculture, wash, nutrition, clothing |
| service_type | ENUM | no | nutrition, wash, sleeping_supplies, clothing |
| description | text | no | |
| aid_type | ENUM | yes | cash, in_kind |
| status | ENUM | yes | completed, suspended, planned, in_progress |
| created_at | timestamp | yes | |
| updated_at | timestamp | yes | |

### locations

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| location_id | UUID | yes | primary key |
| governorate | ENUM | yes | Beirut, Mount Lebanon, North Lebanon, Akkar, Begaa, Baalbek-Hermel, South Lebanon, Nabatieh |
| city | string | yes | |
| district | string | no | |
| locality | string | no | google maps |
| longitude | float | no | |
| latitude | float | no | |
| accessibility | ENUM | yes | easy, medium, hard |

### service_availability

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| availability_id | UUID | yes | primary key |
| service_id | UUID | yes | FK → services |
| location_id | UUID | yes | FK → locations |
| gender_target | ENUM | no | M, F, all |
| age_group | ENUM | no | children, youth, adult, all |
| disability_inclusion | boolean | no | |
| accessibility_notes | text | no | |
| capacity | integer | no | |
| last_verified | timestamp | no | data freshness check |

### shelters

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| shelter_id | UUID | yes | primary key |
| shelter_name | string | yes | |
| shelter_type | ENUM | yes | collective_center, informal_settlement, host_community |
| location_id | UUID | yes | FK → locations |
| capacity_total | integer | yes | max capacity |
| population_total | integer | no | current population |
| households_count | integer | no | |
| women_count | integer | no | |
| children_count | integer | no | |
| elderly_count | integer | no | |
| pwds_count | integer | no | persons with disabilities |
| status | ENUM | yes | active, closed |
| contact_name | string | no | |
| contact_phone | string | no | |
| last_update | timestamp | yes | data freshness |

### shelter_needs

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| need_id | UUID | yes | primary key |
| shelter_id | UUID | yes | FK → shelters |
| sector | ENUM | yes | same as services.sector |
| need_type | string | yes | same as services.service_type |
| severity | ENUM | no | low, medium, high, critical |
| people_in_need | integer | no | |
| description | text | no | |
| reported_at | timestamp | yes | |
| status | ENUM | yes | open, in_progress, closed |
| valid_from | date | no | |
| valid_to | date | no | |

### aid_matches

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| id | UUID | yes | primary key |
| service_id | UUID | yes | FK → services |
| need_id | UUID | yes | FK → shelter_needs |
| provider_id | UUID | no | FK → providers |
| quantity_provided | integer | no | |
| status | ENUM | yes | planned, delivered, partial |
| date | date | yes | |
| verified_by | string | no | |

## Usage

```bash
# Scrape once with database storage
python3 main.py once --csv

# Without database
python3 main.py once --csv --no-database

# Periodic scraping
python3 main.py schedule --interval 6
```

## Data Pipeline

1. Fetch data from Power BI
2. Identify entity types from column names
3. Transform raw data to structured format (governorate aliases, enum inference, etc.)
4. Validate required fields
5. Store to database with deduplication
