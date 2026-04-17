"""Merge silver_player_state into silver_fpl_fantasy_stats.

This populates the player state fields (chance_of_playing, status, news, etc.)
from silver_player_state into silver_fpl_fantasy_stats.
"""

from __future__ import annotations

import logging
from typing import Any

from dotenv import load_dotenv
from supabase import create_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_all_player_state(client: Any) -> list[dict[str, Any]]:
    """Fetch all records from silver_player_state."""
    all_records = []
    page_size = 1000
    offset = 0

    while True:
        result = (
            client.table("silver_player_state")
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


def main() -> None:
    """Merge player state into fantasy stats."""
    load_dotenv()

    import os

    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_KEY", "")
    client = create_client(url, key)

    logger.info("Fetching silver_player_state data...")
    player_state_records = get_all_player_state(client)
    logger.info(f"Found {len(player_state_records)} player state records")

    # Transform to fantasy stats format
    fantasy_updates = []
    for record in player_state_records:
        # Map to fantasy stats format
        update = {
            "player_id": record.get("id"),
            "season": record.get("season"),
            "gameweek": record.get("gameweek"),
            "now_cost": record.get("now_cost"),
            "chance_of_playing_next_round": record.get("chance_of_playing_next_round"),
            "chance_of_playing_this_round": record.get("chance_of_playing_this_round"),
            "news": record.get("news"),
            "status": record.get("status"),
            "in_dreamteam": record.get("in_dreamteam"),
            "removed": record.get("removed"),
            "special": record.get("special"),
            "corners_and_indirect_freekicks_order": record.get(
                "corners_and_indirect_freekicks_order"
            ),
            "direct_freekicks_order": record.get("direct_freekicks_order"),
            "penalties_order": record.get("penalties_order"),
            "form": record.get("form"),
            "selected_by_percent": record.get("selected_by_percent"),
            "transfers_in": record.get("transfers_in"),
            "transfers_out": record.get("transfers_out"),
        }
        fantasy_updates.append(update)

    logger.info(
        f"Upserting {len(fantasy_updates)} records to silver_fpl_fantasy_stats..."
    )

    chunk_size = 500
    for i in range(0, len(fantasy_updates), chunk_size):
        chunk = fantasy_updates[i : i + chunk_size]
        client.table("silver_fpl_fantasy_stats").upsert(chunk).execute()
        logger.info(f"Uploaded {i + len(chunk)}/{len(fantasy_updates)}")

    logger.info("Done!")


if __name__ == "__main__":
    main()
