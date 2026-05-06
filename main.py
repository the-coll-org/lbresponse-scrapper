#!/usr/bin/env python3
"""Power BI public report scraper with Firebase storage and periodic scheduling."""

import argparse
import logging
import os
import time
from datetime import UTC, datetime
from uuid import uuid4

import schedule

from config import (
    OUTPUT_DIR,
    POWERBI_EMBED_URL,
    SCHEDULE_INTERVAL_HOURS,
)
from scraper.api_client import PowerBIClient
from scraper.data_processor import process_visual_data
from scraper.database_store import export_entities_snapshot
from scraper.dsr_parser import extract_select_names, parse_dsr_response
from scraper.embed_url import parse_embed_url, resolve_cluster_url
from scraper.firebase_store import clear_visual_data, mirror_entities, store_visual_data
from scraper.logging_config import scrape_id_var, setup_logging
from scraper.query_builder import build_query_payload
from scraper.report_explorer import ReportExplorer

setup_logging()
log = logging.getLogger("powerbi-scraper")


def _is_allowed_visual(visual: dict) -> bool:
    """Keep only the Service Mapping table and its slicers.

    The Power BI report has many visuals (health pages, A3 matrices, pivot
    tables, etc.) but the UI only consumes the canonical Service Mapping
    table for organization rows and its sector slicer for the Type of
    Service filter. Everything else is noise that pollutes the snapshot.
    """
    page = (visual.get("page") or "").strip().lower().replace("_", " ")
    visual_type = (visual.get("visual_type") or "").strip()
    if not page.startswith("service mapping"):
        return False
    return visual_type in {"tableEx", "slicer"}


def scrape_report(
    embed_url: str,
    to_firebase: bool = True,
    to_csv: bool = False,
    to_database: bool = True,
):
    """Run a full scrape of the Power BI report."""
    log.info("Starting scrape of %s", embed_url)

    token = parse_embed_url(embed_url)
    log.info(
        "Resource key: %s..., tenant: %s",
        token["resource_key"][:8],
        token["tenant_id"][:8],
    )

    cluster_url = resolve_cluster_url(embed_url)
    log.info("Resolved cluster API: %s", cluster_url)

    client = PowerBIClient(cluster_url, token["resource_key"])

    exploration = client.get_models_and_exploration()
    explorer = ReportExplorer(exploration)
    log.info("Report ID: %s, Model ID: %d", explorer.report_id, explorer.model_id)

    pages = explorer.list_pages()
    log.info("Found %d pages: %s", len(pages), [p["display_name"] for p in pages])

    schema_resp = client.get_conceptual_schema(explorer.model_id, explorer.db_name)
    schema_entities = set()
    for model_schema in schema_resp.get("models", []):
        for entity in model_schema.get("entities", []):
            schema_entities.add(entity.get("name", ""))
    log.info("Schema entities: %s", schema_entities)

    visuals = explorer.get_queryable_visuals(schema_entities)
    log.info("Found %d queryable visuals", len(visuals))

    visuals = [v for v in visuals if _is_allowed_visual(v)]
    log.info(
        "Filtered to %d allowed visual(s) on Service Mapping page",
        len(visuals),
    )

    total_rows = 0
    for i, visual in enumerate(visuals):
        entity_label = "_".join(sorted(visual["entities"]))
        visual_name = f"{visual['page']}_{entity_label}_{visual['visual_type']}"
        log.info(
            "[%d/%d] Scraping visual: %s (%d selects)",
            i + 1,
            len(visuals),
            visual_name,
            visual["select_count"],
        )

        all_rows = []
        restart_tokens = None
        select_names = extract_select_names(visual["prototype_query"])

        while True:
            payload = build_query_payload(
                prototype_query=visual["prototype_query"],
                model_id=explorer.model_id,
                db_name=explorer.db_name,
                report_id=explorer.report_id,
                select_count=visual["select_count"],
                restart_tokens=restart_tokens,
            )
            response = client.post_query_data(payload)
            rows, restart_tokens = parse_dsr_response(response, select_names)

            if not rows:
                break

            all_rows.extend(rows)
            log.info("  Fetched %d rows (total so far: %d)", len(rows), len(all_rows))

            if not restart_tokens:
                break

        if not all_rows:
            log.warning("  No data returned for visual %s", visual_name)
            continue

        log.info("  Total rows: %d", len(all_rows))
        total_rows += len(all_rows)

        if to_csv:
            _export_csv(visual_name, all_rows)

        if to_firebase:
            try:
                clear_visual_data(visual_name)
                store_visual_data(
                    visual_name=visual_name,
                    page_name=visual["page"],
                    rows=all_rows,
                    entities=visual["entities"],
                )
            except Exception:
                log.exception("  Failed to store in Firebase for %s", visual_name)

        if to_database:
            try:
                result = process_visual_data(
                    visual_name=visual_name,
                    rows=all_rows,
                    entities=visual["entities"],
                    to_database=True,
                )
                storage_summary = ", ".join(
                    f"{count} {etype}s" for etype, count in result["stored"].items()
                )
                if storage_summary:
                    log.info("  Stored to database: %s", storage_summary)
                if result["errors"]:
                    for error in result["errors"]:
                        log.warning("  %s", error)
            except Exception:
                log.exception("  Failed to store in database for %s", visual_name)

    if to_firebase and to_database:
        try:
            snapshot = export_entities_snapshot()
            mirror_entities(snapshot)
        except Exception:
            log.exception("Failed to mirror ER snapshot to Firebase")

    log.info(
        "Scrape complete. Total rows across all visuals: %d at %s",
        total_rows,
        datetime.now(UTC).isoformat(),
    )
    return total_rows


def _export_csv(visual_name: str, rows: list[dict]):
    import pandas as pd

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    safe_name = visual_name.replace("/", "_").replace(" ", "_")[:100]
    path = os.path.join(OUTPUT_DIR, f"{safe_name}.csv")
    df = pd.DataFrame(rows)
    df.to_csv(path, index=False)
    log.info("  Exported CSV: %s (%d rows)", path, len(rows))


def run_once(args):
    token = scrape_id_var.set(str(uuid4()))
    try:
        scrape_report(
            embed_url=args.url,
            to_firebase=not args.no_firebase,
            to_csv=args.csv,
            to_database=not args.no_database,
        )
    finally:
        scrape_id_var.reset(token)


def run_scheduled(args):
    interval = args.interval or SCHEDULE_INTERVAL_HOURS

    def job():
        token = scrape_id_var.set(str(uuid4()))
        try:
            scrape_report(
                embed_url=args.url,
                to_firebase=not args.no_firebase,
                to_csv=args.csv,
                to_database=not args.no_database,
            )
        except Exception:
            log.exception("Scrape failed")
        finally:
            scrape_id_var.reset(token)

    log.info("Running initial scrape...")
    job()

    log.info("Scheduling scrape every %d hour(s)", interval)
    schedule.every(interval).hours.do(job)

    while True:
        schedule.run_pending()
        time.sleep(60)


def main():
    parser = argparse.ArgumentParser(description="Power BI public report scraper")
    parser.add_argument(
        "--url",
        default=POWERBI_EMBED_URL,
        help="Power BI embed URL (default: configured Lebanon response URL)",
    )
    parser.add_argument("--csv", action="store_true", help="Also export to CSV")
    parser.add_argument("--no-firebase", action="store_true", help="Skip Firebase storage")
    parser.add_argument("--no-database", action="store_true", help="Skip database storage")

    sub = parser.add_subparsers(dest="command")

    sub.add_parser("once", help="Run a single scrape")

    sched_parser = sub.add_parser("schedule", help="Run on a periodic schedule")
    sched_parser.add_argument(
        "--interval",
        type=int,
        default=None,
        help=f"Hours between scrapes (default: {SCHEDULE_INTERVAL_HOURS})",
    )

    args = parser.parse_args()

    if args.command == "schedule":
        run_scheduled(args)
    else:
        run_once(args)


if __name__ == "__main__":
    main()
