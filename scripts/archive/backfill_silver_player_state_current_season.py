"""Backfill silver_player_state for current season from bronze_fpl_gw.

Extracts available dynamic fields (value, selected, transfers) per GW.
Note: Full state (news, chance_of_playing, etc.) only available from FPL API going forward.
"""

from __future__ import annotations

import logging
from typing import Any

from dotenv import load_dotenv
from supabase import create_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_fpl_gw_data(client: Any, season: str) -> list[dict[str, Any]]:
    """Fetch bronze_fpl_gw data for current season."""
    all_records = []
    page_size = 1000
    offset = 0

    while True:
        result = (
            client.table("bronze_fpl_gw")
            .select("*")
            .eq("season", season)
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


def transform_to_player_state(record: dict[str, Any]) -> dict[str, Any]:
    """Transform bronze_fpl_gw to silver_player_state format."""
    return {
        "id": record.get("element"),
        "season": record.get("season"),
        "gameweek": record.get("round"),
        # Available from bronze_fpl_gw
        "now_cost": record.get("value"),
        # All other fields not available in bronze_fpl_gw - set to None
        "chance_of_playing_next_round": None,
        "chance_of_playing_this_round": None,
        "news": None,
        "status": None,
        "in_dreamteam": None,
        "removed": None,
        "special": None,
        "corners_and_indirect_freekicks_order": None,
        "direct_freekicks_order": None,
        "penalties_order": None,
        "form": None,
        "selected_by_percent": None,
        "transfers_in": record.get("transfers_in"),
        "transfers_out": record.get("transfers_out"),
    }


def main() -> None:
    """Main entry point."""
    load_dotenv()

    import os

    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_KEY", "")
    client = create_client(url, key)

    season = "2025-26"
    logger.info(f"Fetching bronze_fpl_gw data for {season}...")
    gw_records = get_fpl_gw_data(client, season)
    logger.info(f"Found {len(gw_records)} player-GW records")

    # Transform to player state
    player_states = []
    for record in gw_records:
        player_states.append(transform_to_player_state(record))

    logger.info(f"Created {len(player_states)} player state records")

    # Upload in chunks - upsert to handle potential duplicates
    logger.info("Upserting to silver_player_state...")
    chunk_size = 500
    for i in range(0, len(player_states), chunk_size):
        chunk = player_states[i : i + chunk_size]
        client.table("silver_player_state").upsert(chunk).execute()
        logger.info(f"Uploaded {i + len(chunk)}/{len(player_states)}")

    logger.info(f"Done! Backfilled {season} GWs from bronze_fpl_gw")


if __name__ == "__main__":
    main()
