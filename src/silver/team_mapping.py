"""Team mapping module for Silver layer.

Creates unified team identity mapping across FPL, Vaastav, and Understat sources.
Uses correct bronze tables as sources.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

import polars as pl
from dotenv import load_dotenv

from src.data.database import get_supabase_client, read_from_supabase
from src.utils.supabase_utils import fetch_seasonal_records

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Seasons to process
# FPL: only 2025-26 (current season)
# Vaastav: 2021-22 to 2024-25 (historical)
# Understat: all seasons

SEASONS = ["2021-22", "2022-23", "2023-24", "2024-25", "2025-26"]

# Team name mapping: FPL/Vaastav name -> Understat name
TEAM_NAME_NORMALIZATION: dict[str, str] = {
    # FPL/Vaastav -> Understat
    "Man City": "Manchester City",
    "Man Utd": "Manchester United",
    "Newcastle": "Newcastle United",
    "Spurs": "Tottenham",
    "Wolves": "Wolverhampton Wanderers",
    "Nott'm Forest": "Nottingham Forest",
    "Bournemouth": "Bournemouth",
    "Sheffield Utd": "Sheffield United",
}


def get_supabase():
    """Get Supabase client."""
    client = get_supabase_client()
    if client is None:
        raise ValueError("Failed to connect to Supabase")
    return client


def load_fpl_teams(season: str) -> list[dict[str, Any]]:
    """Load FPL teams for a season."""
    client = get_supabase()

    # FPL only has current season data
    if season != "2025-26":
        logger.debug(f"FPL only has 2025-26 data, skipping {season}")
        return []

    teams = fetch_seasonal_records(
        client,
        "bronze_fpl_teams",
        select_cols="id,name,season",
        season_col="season",
        seasons=[season],
    )

    if season not in teams:
        logger.warning(f"No FPL teams found for season {season}")
        return []

    return [
        {"id": t["id"], "name": t["name"], "season": t["season"]} for t in teams[season]
    ]


def load_vaastav_teams(season: str) -> list[dict[str, Any]]:
    """Load unique team names from Vaastav fixtures."""
    client = get_supabase()

    # Vaastav has data for 2021-22 to 2024-25
    if season not in ["2021-22", "2022-23", "2023-24", "2024-25"]:
        logger.debug(f"Vaastav doesn't have data for {season}")
        return []

    teams = fetch_seasonal_records(
        client,
        "bronze_vaastav_fixtures",
        select_cols="team_h,team_a,season",
        season_col="season",
        seasons=[season],
    )

    if season not in teams:
        logger.warning(f"No Vaastav data for season {season}")
        return []

    # Extract unique teams from both home and away
    team_set = set()
    for match in teams[season]:
        if match.get("team_h"):
            team_set.add(match["team_h"])
        if match.get("team_a"):
            team_set.add(match["team_a"])

    return [{"name": t, "season": season} for t in sorted(team_set)]


def load_understat_teams(season: str) -> list[dict[str, Any]]:
    """Load unique team names from Understat match stats."""
    client = get_supabase()

    teams = fetch_seasonal_records(
        client,
        "bronze_understat_match_stats",
        select_cols="home_team,away_team,season",
        season_col="season",
        seasons=[season],
    )

    if season not in teams:
        logger.warning(f"No Understat data for season {season}")
        return []

    # Extract unique teams from both home and away
    team_set = set()
    for match in teams[season]:
        if match.get("home_team"):
            team_set.add(match["home_team"])
        if match.get("away_team"):
            team_set.add(match["away_team"])

    return [{"name": t, "season": season} for t in sorted(team_set)]


def normalize_team_name(name: str, source: str) -> str:
    """Normalize team name to Understat format."""
    if source == "understat":
        return name
    return TEAM_NAME_NORMALIZATION.get(name, name)


def build_season_mappings(season: str) -> list[dict[str, Any]]:
    """Build team mappings for a single season."""
    logger.info(f"Processing season: {season}")

    results = []

    # Determine the primary source based on season
    is_current_season = season == "2025-26"

    if is_current_season:
        # Current season: FPL is truth
        fpl_teams = load_fpl_teams(season)
        understat_teams = load_understat_teams(season)

        logger.info(f"  FPL: {len(fpl_teams)} teams")
        logger.info(f"  Understat: {len(understat_teams)} teams")

        # Build understat name lookup
        understat_lookup = {t["name"]: t["name"] for t in understat_teams}

        for team in fpl_teams:
            fpl_name = team["name"]
            fpl_id = team["id"]

            # Find normalized understat name
            understat_name = normalize_team_name(fpl_name, "fpl")
            # Check if understat has this team
            if understat_name not in understat_lookup and fpl_name in understat_lookup:
                understat_name = understat_lookup.get(fpl_name, fpl_name)

            results.append(
                {
                    "season": season,
                    "fpl_team_id": fpl_id,
                    "fpl_team_name": fpl_name,
                    "understat_team_name": understat_name,
                    "unified_team_id": str(uuid.uuid4()),
                    "source": "fpl",
                    "confidence_score": 1.0,
                }
            )
    else:
        # Historical season: Vaastav is truth
        vaastav_teams = load_vaastav_teams(season)
        understat_teams = load_understat_teams(season)

        logger.info(f"  Vaastav: {len(vaastav_teams)} teams")
        logger.info(f"  Understat: {len(understat_teams)} teams")

        # Build understat name lookup
        understat_lookup = {t["name"]: t["name"] for t in understat_teams}

        for team in vaastav_teams:
            vaastav_name = team["name"]

            # Find normalized understat name
            understat_name = normalize_team_name(vaastav_name, "vaastav")
            # Check if understat has this team
            if (
                understat_name not in understat_lookup
                and vaastav_name in understat_lookup
            ):
                understat_name = understat_lookup.get(vaastav_name, understat_name)

            results.append(
                {
                    "season": season,
                    "vaastav_team_name": vaastav_name,
                    "understat_team_name": understat_name,
                    "unified_team_id": str(uuid.uuid4()),
                    "source": "vaastav",
                    "confidence_score": 1.0,
                }
            )

    return results


def run() -> None:
    """Main entry point."""
    logger.info("Starting team mapping generation...")

    all_mappings = []

    for season in SEASONS:
        mappings = build_season_mappings(season)
        all_mappings.extend(mappings)

    if not all_mappings:
        logger.error("No team mappings generated, aborting")
        return

    logger.info(f"Total team mappings: {len(all_mappings)}")

    # Upload to Supabase
    client = get_supabase()

    # Clear existing data
    for season in SEASONS:
        client.table("silver_team_mapping").delete().eq("season", season).execute()

    # Upload in batches
    batch_size = 100
    for i in range(0, len(all_mappings), batch_size):
        chunk = all_mappings[i : i + batch_size]
        client.table("silver_team_mapping").insert(chunk).execute()

    logger.info(f"Team mapping complete! Uploaded {len(all_mappings)} records")

    # Print statistics
    print("\n=== silver_team_mapping Statistics ===")
    print("Season   | Total Teams | Vaastav Teams | FPL Teams | Understat Teams")
    print("-" * 65)

    for season in SEASONS:
        teams_in_season = [t for t in all_mappings if t["season"] == season]
        total = len(teams_in_season)
        vaastav = sum(1 for t in teams_in_season if t.get("vaastav_team_name"))
        fpl = sum(1 for t in teams_in_season if t.get("fpl_team_name"))
        understat = sum(1 for t in teams_in_season if t.get("understat_team_name"))
        print(f"{season} | {total:10} | {vaastav:12} | {fpl:9} | {understat:13}")


if __name__ == "__main__":
    run()
