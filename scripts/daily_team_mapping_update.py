"""Daily team mapping update for Silver layer.

This script:
1. Derives Understat team IDs by matching players between FPL/Vaastav and Understat
2. Updates silver_team_mapping with understat_team_id
3. Regenerates silver_player_mapping with proper Understat matching

Run as part of daily_silver_update.py or separately:
    python scripts/daily_team_mapping_update.py
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from difflib import SequenceMatcher
from typing import Any

from dotenv import load_dotenv

from src.data.database import get_supabase_client
from src.silver.player_mapping import build_all_season_mappings, upload_to_supabase
from src.utils.supabase_utils import fetch_all_by_filter, fetch_seasonal_records

load_dotenv()

logger = logging.getLogger(__name__)


def name_similarity(a: str, b: str) -> float:
    """Calculate similarity between two names."""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def derive_all_season_team_mappings(client: Any) -> dict[str, dict[str, int]]:
    """Derive Understat team ID mapping for all seasons at once.

    Loads all data in bulk, processes in memory, returns mapping for all seasons.

    Returns:
        Dict mapping season -> {team_name: understat_team_id}
    """
    # Load all understat player mappings for all seasons at once
    # Use filter-based approach: fetch by season, it's more efficient
    seasons = ["2021-22", "2022-23", "2023-24", "2024-25", "2025-26"]

    start = time.time()
    understat_by_season: dict[str, list[dict[str, Any]]] = {}
    for season in seasons:
        data = fetch_all_by_filter(
            client,
            "bronze_understat_player_mappings",
            select_cols="season,understat_team_id,understat_player_name",
            filter_col="season",
            filter_val=season,
        )
        understat_by_season[season] = data
    logger.info(f"  Loaded Understat data: {time.time() - start:.2f}s")

    # Build: (season, understat_team_id) -> [player_names]
    us_teams: dict[tuple[str, int], list[str]] = defaultdict(list)
    for season, records in understat_by_season.items():
        for r in records:
            us_team_id = r.get("understat_team_id")
            name = r.get("understat_player_name")
            if us_team_id and name:
                us_teams[(season, us_team_id)].append(name.lower())

    logger.info(f"  Loaded {len(us_teams)} Understat teams across all seasons")

    # Load all vaastav player data - use fetch_seasonal_records for efficiency
    start = time.time()
    vaastav_by_season = fetch_seasonal_records(
        client,
        "bronze_vaastav_player_history_gw",
        select_cols="season,name,team",
        season_col="season",
        seasons=seasons,
    )
    logger.info(f"  Loaded Vaastav data: {time.time() - start:.2f}s")

    # Build: (season, team_name) -> [player_names]
    src_teams: dict[tuple[str, str], list[str]] = defaultdict(list)
    for season, records in vaastav_by_season.items():
        for r in records:
            team_name = r.get("team", "")
            name = r.get("name", "")
            if team_name and name:
                src_teams[(season, team_name)].append(name.lower())

    logger.info(f"  Loaded {len(src_teams)} source teams across all seasons")

    # Now derive mappings for all (season, team) combinations
    threshold = 0.6
    all_mappings: dict[str, dict[str, int]] = defaultdict(dict)

    for (season, src_team), src_names in src_teams.items():
        # Find best matching Understat team for this season
        best_match = None
        best_score = 0

        for (us_season, us_team_id), us_names in us_teams.items():
            if us_season != season:
                continue

            matches = sum(
                1
                for sn in src_names
                for un in us_names
                if name_similarity(sn, un) > threshold
            )
            if matches > best_score:
                best_score = matches
                best_match = us_team_id

        if best_match:
            all_mappings[season][src_team] = best_match

    logger.info(f"  Derived mappings for {len(all_mappings)} seasons")
    return all_mappings


def batch_update_silver_team_mapping(
    client: Any, mappings: dict[str, dict[str, int]]
) -> int:
    """Batch update silver_team_mapping with all mappings at once.

    Loads existing silver_team_mapping once, performs updates in memory,
    then batch updates back to database.
    """
    # Load all silver_team_mapping entries - use simpler query (only ~100 rows)
    all_teams = (
        client.table("silver_team_mapping")
        .select(
            "unified_team_id,season,vaastav_team_name,fpl_team_name,understat_team_id"
        )
        .execute()
        .data
        or []
    )

    # Build lookup: (season, team_name) -> unified_team_id
    team_lookup: dict[tuple[str, str], str] = {}
    for r in all_teams:
        season = r.get("season")
        unified_id = r.get("unified_team_id")
        if not season or not unified_id:
            continue
        # Prefer vaastav_team_name, fall back to fpl_team_name
        if r.get("vaastav_team_name"):
            team_lookup[(season, r["vaastav_team_name"])] = unified_id
        elif r.get("fpl_team_name"):
            team_lookup[(season, r["fpl_team_name"])] = unified_id
            team_lookup[(season, r["fpl_team_name"])] = unified_id

    # Build updates list: [(unified_team_id, understat_team_id), ...]
    updates: list[tuple[str, int]] = []
    for season, season_mappings in mappings.items():
        for team_name, us_team_id in season_mappings.items():
            key = (season, team_name)
            if key in team_lookup:
                updates.append((team_lookup[key], us_team_id))

    # Batch update
    for unified_id, us_team_id in updates:
        client.table("silver_team_mapping").update(
            {"understat_team_id": us_team_id}
        ).eq("unified_team_id", unified_id).execute()

    return len(updates)


def run() -> None:
    """Main entry point."""
    logger.info("Starting daily team mapping update...")

    client = get_supabase_client()
    if client is None:
        logger.error("Failed to connect to Supabase")
        return

    # Step 1: Derive all team mappings for all seasons (single bulk load)
    all_mappings = derive_all_season_team_mappings(client)

    # Step 2: Batch update silver_team_mapping (single bulk update)
    updated = batch_update_silver_team_mapping(client, all_mappings)
    logger.info(f"Updated {updated} team mappings")

    # Note: Player mappings are now regenerated in daily_silver_update.py directly
    # This avoids running them twice and potential timeouts

    logger.info("Daily team mapping update complete!")


if __name__ == "__main__":
    run()
