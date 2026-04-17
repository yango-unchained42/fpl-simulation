"""Backfill silver_player_state from bronze_vaastav_players.

Uses season-end snapshot as GW38 state. For current season, this will be
overwritten with live GW data each week.
"""

from __future__ import annotations

import logging
from typing import Any

from dotenv import load_dotenv
from supabase import create_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Dynamic fields to extract (GW-specific)
DYNAMIC_FIELDS = [
    "now_cost",
    "chance_of_playing_next_round",
    "chance_of_playing_this_round",
    "news",
    "status",
    "in_dreamteam",
    "removed",
    "special",
    "corners_and_indirect_freekicks_order",
    "direct_freekicks_order",
    "penalties_order",
    "form",
    "selected_by_percent",
    # GW-specific transfers (not cumulative)
    "transfers_in_event",
    "transfers_out_event",
]


def get_all_bronze_players(client: Any) -> list[dict[str, Any]]:
    """Fetch all records from bronze_vaastav_players."""
    all_records = []
    page_size = 1000
    offset = 0

    while True:
        result = (
            client.table("bronze_vaastav_players")
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


def transform_to_player_state(record: dict[str, Any]) -> dict[str, Any]:
    """Transform bronze player to silver_player_state format (GW38 snapshot)."""
    state = {
        "id": record.get("id"),
        "season": record.get("season"),
        "gameweek": 38,  # Season-end snapshot
    }

    # Add dynamic fields
    for field in DYNAMIC_FIELDS:
        if field in record:
            value = record[field]
            # Handle type conversions
            if field in ["in_dreamteam", "removed", "special"]:
                state[field] = bool(value) if value is not None else None
            elif field in [
                "corners_and_indirect_freekicks_order",
                "direct_freekicks_order",
                "penalties_order",
            ]:
                state[field] = int(value) if value is not None else None
            else:
                state[field] = value

    # Rename event columns to simple names
    if "transfers_in_event" in state:
        state["transfers_in"] = state.pop("transfers_in_event")
    if "transfers_out_event" in state:
        state["transfers_out"] = state.pop("transfers_out_event")

    return state


def main() -> None:
    """Main entry point."""
    load_dotenv()

    import os

    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_KEY", "")
    client = create_client(url, key)

    logger.info("Fetching bronze_vaastav_players data...")
    bronze_records = get_all_bronze_players(client)
    logger.info(f"Found {len(bronze_records)} records")

    # Transform to player state
    player_states = []
    for record in bronze_records:
        player_states.append(transform_to_player_state(record))

    logger.info(f"Created {len(player_states)} player state records (GW38)")

    # Upload in chunks - use upsert to handle existing data
    logger.info("Upserting to silver_player_state...")
    chunk_size = 1000
    for i in range(0, len(player_states), chunk_size):
        chunk = player_states[i : i + chunk_size]
        client.table("silver_player_state").upsert(chunk).execute()
        logger.info(f"Uploaded {i + len(chunk)}/{len(player_states)}")

    logger.info(
        "Done! Note: This is season-end (GW38) data. For current season, update each GW from live API."
    )


if __name__ == "__main__":
    main()
