"""Update silver_player_state from FPL API for current season.

Run this each GW to capture the player state before each gameweek.
Usage: python scripts/update_silver_player_state.py --gw 5
"""

from __future__ import annotations

import argparse
import logging
from typing import Any

import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FPL_API_URL = "https://fantasy.premierleague.com/api/bootstrap-static/"


def fetch_fpl_players() -> list[dict[str, Any]]:
    """Fetch current player data from FPL API."""
    response = requests.get(FPL_API_URL, timeout=60)
    response.raise_for_status()
    data = response.json()
    return data.get("elements", [])


def transform_to_player_state(
    player: dict[str, Any], season: str, gameweek: int
) -> dict[str, Any]:
    """Transform FPL API player to silver_player_state format."""
    return {
        "id": player.get("id"),
        "season": season,
        "gameweek": gameweek,
        "now_cost": player.get("now_cost"),
        "chance_of_playing_next_round": player.get("chance_of_playing_next_round"),
        "chance_of_playing_this_round": player.get("chance_of_playing_this_round"),
        "news": player.get("news"),
        "status": player.get("status"),
        "in_dreamteam": player.get("in_dreamteam"),
        "removed": player.get("removed"),
        "special": player.get("special"),
        "corners_and_indirect_freekicks_order": player.get(
            "corners_and_indirect_freekicks_order"
        ),
        "direct_freekicks_order": player.get("direct_freekicks_order"),
        "penalties_order": player.get("penalties_order"),
        "form": player.get("form"),
        "selected_by_percent": player.get("selected_by_percent"),
        "transfers_in": player.get("transfers_in"),
        "transfers_out": player.get("transfers_out"),
    }


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update silver_player_state from FPL API"
    )
    parser.add_argument("--gw", type=int, required=True, help="Gameweek number (1-38)")
    parser.add_argument(
        "--season",
        type=str,
        default="2025-26",
        help="Season in format YYYY-YY",
    )
    args = parser.parse_args()

    from dotenv import load_dotenv

    load_dotenv()

    import os

    from supabase import create_client

    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_KEY", "")
    client = create_client(url, key)

    logger.info(f"Fetching FPL data for GW {args.gw} of {args.season}...")
    players = fetch_fpl_players()
    logger.info(f"Found {len(players)} players")

    # Transform to player state
    player_states = [
        transform_to_player_state(p, args.season, args.gw) for p in players
    ]

    # Upload - upsert to handle re-runs
    logger.info("Upserting to silver_player_state...")
    chunk_size = 500
    for i in range(0, len(player_states), chunk_size):
        chunk = player_states[i : i + chunk_size]
        client.table("silver_player_state").upsert(chunk).execute()
        logger.info(f"Uploaded {i + len(chunk)}/{len(player_states)}")

    logger.info(f"Done! Updated GW {args.gw} for {args.season}")


if __name__ == "__main__":
    main()
