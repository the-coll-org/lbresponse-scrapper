"""Parse Power BI embed URLs and resolve API cluster endpoints."""

import base64
import json
import re
from urllib.parse import parse_qs, urlparse

import requests


def parse_embed_url(url: str) -> dict:
    """Extract resource_key, tenant_id from a Power BI public embed URL."""
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    token_b64 = params["r"][0]
    padding = 4 - len(token_b64) % 4
    if padding != 4:
        token_b64 += "=" * padding
    token = json.loads(base64.b64decode(token_b64))
    return {
        "resource_key": token["k"],
        "tenant_id": token["t"],
        "cluster_id": token.get("c"),
    }


def resolve_cluster_url(embed_url: str) -> str:
    """Fetch the embed page HTML and extract the API cluster URL."""
    resp = requests.get(embed_url, timeout=30)
    resp.raise_for_status()
    match = re.search(r"resolvedClusterUri\s*[=:]\s*[\"']([^\"']+)[\"']", resp.text)
    if not match:
        match = re.search(r"clusterUri\s*[=:]\s*[\"']([^\"']+)[\"']", resp.text)
    if not match:
        raise RuntimeError("Could not find cluster URI in embed page HTML")
    cluster_url = match.group(1).rstrip("/")
    cluster_url = cluster_url.replace("-redirect", "-api")
    return cluster_url
