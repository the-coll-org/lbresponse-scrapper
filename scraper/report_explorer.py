"""Parse Power BI modelsAndExploration response to extract report structure."""

import json
import logging

log = logging.getLogger(__name__)


class ReportExplorer:
    def __init__(self, exploration_data: dict):
        self._data = exploration_data
        model = exploration_data["models"][0]
        self.model_id = model["id"]
        self.db_name = model["dbName"]
        raw = exploration_data["exploration"]
        exploration = json.loads(raw) if isinstance(raw, str) else raw
        self.report_id = exploration.get("report", {}).get("objectId", "")
        self._sections = exploration.get("sections", [])

    def list_pages(self) -> list[dict]:
        pages = []
        for section in self._sections:
            pages.append(
                {
                    "name": section.get("name", ""),
                    "display_name": section.get("displayName", ""),
                    "visual_count": len(section.get("visualContainers", [])),
                }
            )
        return pages

    def get_queryable_visuals(self, schema_entities: set | None = None) -> list[dict]:
        """Extract visuals with a prototypeQuery, optionally filtered by valid schema entities."""
        visuals = []
        seen_queries = set()

        for section in self._sections:
            page_name = section.get("displayName", section.get("name", "unknown"))
            for vc in section.get("visualContainers", []):
                try:
                    config = json.loads(vc.get("config", "{}"))
                except (json.JSONDecodeError, TypeError):
                    continue

                sq = config.get("singleVisual", {})
                proto = sq.get("prototypeQuery")
                if not proto:
                    continue

                visual_type = sq.get("visualType", "unknown")
                entities = self._extract_entities(proto)
                query_key = json.dumps(proto, sort_keys=True)

                if query_key in seen_queries:
                    continue
                seen_queries.add(query_key)

                if schema_entities:
                    invalid = entities - schema_entities
                    if invalid:
                        log.warning(
                            "Skipping visual on page '%s' — stale entities: %s",
                            page_name,
                            invalid,
                        )
                        continue

                selects = proto.get("Select", [])
                visuals.append(
                    {
                        "page": page_name,
                        "visual_type": visual_type,
                        "prototype_query": proto,
                        "entities": entities,
                        "select_count": len(selects),
                    }
                )

        return visuals

    @staticmethod
    def _extract_entities(proto_query: dict) -> set[str]:
        entities = set()
        for item in proto_query.get("From", []):
            entity = item.get("Entity")
            if entity:
                entities.add(entity)
        return entities
