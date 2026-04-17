"""Consolidate data into silver_fpl_fantasy_stats and silver_fpl_player_stats.

Extracts from silver_gw and bronze data to populate both tables.
"""

from __future__ import annotations

import logging
from typing import Any

from dotenv import load_dotenv
from supabase import create_client

from src.utils.data_cleaning import clean_and_flag_record

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_all_silver_gw(client: Any) -> list[dict[str, Any]]:
    """Fetch all records from silver_gw."""
    all_records = []
    page_size = 1000
    offset = 0

    while True:
        result = (
            client.table("silver_gw")
            .select("*")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        if not result.data:
            break
        all_records.extend(result.data)
        offset += page_size
        if len(result.data) < page_size:
            break

    return all_records


def transform_to_fantasy_stats(record: dict[str, Any]) -> dict[str, Any]:
    """Extract fantasy stats from silver_gw record."""
    return {
        "player_id": record.get("player_id"),
        "season": record.get("season"),
        "gameweek": record.get("gameweek"),
        # Use value (post-GW price) - now_cost is mostly empty for historical
        "now_cost": record.get("value"),
        # Ownership data (from GW)
        "value": record.get("value"),
        "selected": record.get("selected"),
        "transfers_in": record.get("transfers_in"),
        "transfers_out": record.get("transfers_out"),
        # Player state fields - will be NULL for historical, filled by live updates
        "chance_of_playing_next_round": None,
        "chance_of_playing_this_round": None,
        "news": None,
        "status": None,
        "form": None,
        "selected_by_percent": None,
        "in_dreamteam": None,
        "removed": None,
        "special": None,
        "corners_and_indirect_freekicks_order": None,
        "direct_freekicks_order": None,
        "penalties_order": None,
    }


def transform_to_player_stats(record: dict[str, Any]) -> dict[str, Any]:
    """Extract player stats from silver_gw record."""
    # Remove fantasy/ownership columns
    record.pop("value", None)
    record.pop("selected", None)
    record.pop("transfers_in", None)
    record.pop("transfers_out", None)
    record.pop("transfers_balance", None)
    record.pop("created_at", None)
    record.pop("updated_at", None)
    record.pop("data_quality_score", None)
    record.pop("is_incomplete", None)
    record.pop("missing_fields", None)

    # Add quality flags
    record = clean_and_flag_record(record, category="vaastav_gw")

    return record


def main() -> None:
    """Main entry point."""
    load_dotenv()

    import os

    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_KEY", "")
    client = create_client(url, key)

    logger.info("Fetching silver_gw data...")
    gw_records = get_all_silver_gw(client)
    logger.info(f"Found {len(gw_records)} GW records")

    # Transform to fantasy stats
    logger.info("Transforming to fantasy stats...")
    fantasy_stats = []
    for record in gw_records:
        fantasy_stats.append(transform_to_fantasy_stats(record))

    # Transform to player stats
    logger.info("Transforming to player stats...")
    player_stats = []
    for record in gw_records:
        player_stats.append(transform_to_player_stats(record))

    # Upload fantasy stats
    logger.info(f"Upserting {len(fantasy_stats)} fantasy stats records...")
    chunk_size = 500
    for i in range(0, len(fantasy_stats), chunk_size):
        chunk = fantasy_stats[i : i + chunk_size]
        client.table("silver_fpl_fantasy_stats").upsert(chunk).execute()
        logger.info(f"Uploaded {i + len(chunk)}/{len(fantasy_stats)}")

    # Upload player stats
    logger.info(f"Upserting {len(player_stats)} player stats records...")
    for i in range(0, len(player_stats), chunk_size):
        chunk = player_stats[i : i + chunk_size]
        client.table("silver_fpl_player_stats").upsert(chunk).execute()
        logger.info(f"Uploaded {i + len(chunk)}/{len(player_stats)}")

    logger.info("Done!")


if __name__ == "__main__":
    main()
