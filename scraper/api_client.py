"""HTTP client for Power BI public report API."""

from typing import Any, cast

import requests

from config import REQUEST_TIMEOUT


class PowerBIClient:
    def __init__(self, cluster_url: str, resource_key: str):
        self.session = requests.Session()
        self.base_url = cluster_url
        self.session.headers.update(
            {
                "X-PowerBI-ResourceKey": resource_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def get_models_and_exploration(self) -> dict[str, Any]:
        url = f"{self.base_url}/public/reports/modelsAndExploration"
        params = {"preferReadOnlySession": "true"}
        resp = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return cast("dict[str, Any]", resp.json())

    def get_conceptual_schema(self, model_id: int, db_name: str) -> dict[str, Any]:
        url = f"{self.base_url}/public/reports/conceptualschema"
        payload = {
            "modelIds": [model_id],
            "userPreferredLocale": "en-US",
            "databaseName": db_name,
        }
        resp = self.session.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return cast("dict[str, Any]", resp.json())

    def post_query_data(self, payload: dict) -> dict[str, Any]:
        url = f"{self.base_url}/public/reports/querydata"
        resp = self.session.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return cast("dict[str, Any]", resp.json())
