"""Process and store scraped Power BI data into structured database format."""

import logging

from scraper import database_store, data_transformer

log = logging.getLogger(__name__)


def process_visual_data(
    visual_name: str,
    rows: list[dict],
    entities: set[str],
    to_database: bool = True,
) -> dict:
    """Process scraped visual data and store to database.

    Args:
        visual_name: Name of the visual from Power BI
        rows: List of data rows from the visual
        entities: Set of entity types in the data
        to_database: Whether to store to database

    Returns:
        Dictionary with processing results
    """
    results = {
        "visual_name": visual_name,
        "total_rows": len(rows),
        "entities": list(entities),
        "stored": {},
        "errors": [],
    }

    if not to_database or not rows:
        return results

    # Identify entity types from columns
    columns = list(rows[0].keys()) if rows else []
    identified_entities = data_transformer.identify_entity_types(columns)
    identified_entities.update(entities)

    log.info(
        "Processing visual '%s' with %d rows and entities: %s",
        visual_name,
        len(rows),
        identified_entities,
    )

    for entity_type in identified_entities:
        try:
            count = _process_entity_type(entity_type, rows)
            results["stored"][entity_type] = count
            log.info("  Stored %d %s records", count, entity_type)
        except Exception as e:
            error_msg = f"Failed to process {entity_type}: {str(e)}"
            results["errors"].append(error_msg)
            log.error(error_msg)

    return results


def _process_entity_type(entity_type: str, rows: list[dict]) -> int:
    """Process and store data for a specific entity type."""
    entity_type = entity_type.lower().strip()
    count = 0

    if entity_type == "provider":
        for row in rows:
            transformed = data_transformer.transform_provider_row(row)
            if not transformed.get("provider_name"):
                continue
            try:
                database_store.store_provider(transformed)
                count += 1
            except Exception as e:
                log.debug("Failed to store provider: %s", e)

    elif entity_type == "service":
        for row in rows:
            transformed = data_transformer.transform_service_row(row)
            if not transformed.get("service_name") or not transformed.get("provider_id"):
                continue
            try:
                database_store.store_service(transformed, transformed["provider_id"])
                count += 1
            except Exception as e:
                log.debug("Failed to store service: %s", e)

    elif entity_type == "shelter":
        for row in rows:
            # Get or create location
            if row.get("location_id"):
                location_id = row["location_id"]
            else:
                location_data = data_transformer.transform_location_row(row)
                if not location_data.get("city") or not location_data.get("governorate"):
                    continue
                try:
                    location = database_store.store_location(location_data)
                    location_id = location.location_id
                except Exception as e:
                    log.debug("Failed to store location for shelter: %s", e)
                    continue

            transformed = data_transformer.transform_shelter_row(row)
            if not transformed.get("shelter_name"):
                continue
            transformed["location_id"] = location_id
            try:
                database_store.store_shelter(transformed, location_id)
                count += 1
            except Exception as e:
                log.debug("Failed to store shelter: %s", e)

    elif entity_type == "location":
        for row in rows:
            transformed = data_transformer.transform_location_row(row)
            if not transformed.get("city") or not transformed.get("governorate"):
                continue
            try:
                database_store.store_location(transformed)
                count += 1
            except Exception as e:
                log.debug("Failed to store location: %s", e)

    elif entity_type == "shelter_need":
        for row in rows:
            shelter_id = row.get("shelter_id")
            if not shelter_id:
                continue
            transformed = data_transformer.transform_shelter_need_row(row, shelter_id)
            if not transformed.get("need_type"):
                continue
            try:
                database_store.store_shelter_need(transformed, shelter_id)
                count += 1
            except Exception as e:
                log.debug("Failed to store shelter need: %s", e)

    elif entity_type == "service_availability":
        for row in rows:
            transformed = data_transformer.transform_service_availability_row(row)
            if not transformed.get("service_id") or not transformed.get("location_id"):
                continue
            try:
                database_store.store_service_availability(transformed)
                count += 1
            except Exception as e:
                log.debug("Failed to store service availability: %s", e)

    elif entity_type == "aid_match":
        for row in rows:
            transformed = data_transformer.transform_aid_match_row(row)
            if not transformed.get("service_id") or not transformed.get("need_id"):
                continue
            try:
                database_store.store_aid_match(transformed)
                count += 1
            except Exception as e:
                log.debug("Failed to store aid match: %s", e)

    else:
        log.warning("Unknown entity type: %s", entity_type)

    return count
