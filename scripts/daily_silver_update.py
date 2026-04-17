"""Daily Silver layer update.

Consolidates all Bronze and API data into Silver tables:
- FPL fantasy stats (from bronze)
- FPL player stats (from bronze)
- FPL player state (from FPL API)
- Fixtures (from bronze)
- Understat player stats (from bronze)
- Understat match stats (from bronze + aggregated player stats)

Usage: python scripts/daily_silver_update.py --gw 32
"""

from __future__ import annotations

import argparse
import logging
import time
from collections import defaultdict
from typing import Any

from dotenv import load_dotenv
from supabase import create_client

from src.utils.data_cleaning import clean_and_flag_record
from src.utils.supabase_utils import fetch_all_paginated

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BATCH_SIZE = 500
FPL_API_URL = "https://fantasy.premierleague.com/api/bootstrap-static/"

# Columns to aggregate from player stats to match stats
PLAYER_STATS_COLS = [
    "shots",
    "xa",
    "key_passes",
    "yellow_cards",
    "red_cards",
]


def get_supabase():
    """Initialize Supabase client."""
    import os

    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_KEY", "")
    return create_client(url, key)


def truncate_table(table_name: str) -> None:
    """Truncate a table via CLI - much faster than DELETE on large tables.

    Requires SUPABASE_ACCESS_TOKEN environment variable to be set.
    Falls back to logging a warning if not available.
    """
    import os
    import subprocess

    token = os.getenv("SUPABASE_ACCESS_TOKEN")
    if not token:
        logger.warning(
            f"    No SUPABASE_ACCESS_TOKEN - skipping truncate for {table_name}"
        )
        return

    logger.info(f"    Truncating {table_name}...")
    result = subprocess.run(
        [
            "supabase",
            "db",
            "query",
            "--linked",
            f"TRUNCATE {table_name} RESTART IDENTITY CASCADE;",
        ],
        capture_output=True,
        text=True,
        env={**os.environ, "SUPABASE_ACCESS_TOKEN": token},
    )
    if result.returncode != 0:
        logger.warning(f"    Truncate failed for {table_name}: {result.stderr}")
    else:
        logger.info(f"    Truncated {table_name}")


# ==================== FPL Data ====================


# Columns that exist in silver_fpl_fantasy_stats (ownership data only)
FPL_FANTASY_STATS_COLS = [
    "value",
    "selected",
    "transfers_in",
    "transfers_out",
    "now_cost",
    "chance_of_playing_next_round",
    "chance_of_playing_this_round",
    "news",
    "status",
    "form",
    "selected_by_percent",
    "in_dreamteam",
    "removed",
    "corners_and_indirect_freekicks_order",
    "direct_freekicks_order",
    "penalties_order",
    "data_quality_score",
    "is_incomplete",
    "missing_fields",
    "season",
    "gameweek",
    # UUID columns (added during transform)
    "unified_player_id",
    "match_id",
    # Redundant columns to drop (now using UUIDs)
    "player_id",  # -> unified_player_id
    "element",  # same as player_id
]


def update_fpl_fantasy_stats(client: Any) -> bool:
    """Update silver_fpl_fantasy_stats from bronze (ownership data only).

    Resolves unified_player_id and match_id from mappings.
    """
    logger.info("  Updating FPL fantasy stats from bronze...")

    # Load player mappings for UUID resolution
    player_lookup: dict[tuple[str, int], str] = {}
    for r in fetch_all_paginated(
        client,
        "silver_player_mapping",
        select_cols="season,fpl_id,unified_player_id",
    ):
        season = r.get("season")
        unified_id = r.get("unified_player_id")
        if season and unified_id and r.get("fpl_id"):
            player_lookup[(season, int(r["fpl_id"]))] = unified_id

    # Load match mappings - need to find match for each player's gameweek
    # Build lookup: (season, fpl_fixture_id) -> match_id
    match_lookup: dict[tuple[str, int], str] = {}
    for r in fetch_all_paginated(
        client, "silver_match_mapping", select_cols="season,fpl_fixture_id,match_id"
    ):
        if r.get("season") and r.get("fpl_fixture_id") and r.get("match_id"):
            match_lookup[(r["season"], int(r["fpl_fixture_id"]))] = r["match_id"]

    logger.info(
        f"    Loaded {len(player_lookup)} player lookups, {len(match_lookup)} match lookups"
    )

    # Truncate before load to avoid duplicates
    truncate_table("silver_fpl_fantasy_stats")

    # Fetch GW stats for season/gameweek context
    gw_result = (
        client.table("bronze_fpl_gw").select("element, round, fixture").execute()
    )
    gw_data = gw_result.data

    # Build player -> latest gameweek and fixture lookup
    player_gw_fixture = {}
    for record in gw_data:
        player_id = record.get("element")
        gw = record.get("round")
        fixture = record.get("fixture")
        if player_id and gw:
            if (
                player_id not in player_gw_fixture
                or gw > player_gw_fixture[player_id][0]
            ):
                player_gw_fixture[player_id] = (gw, fixture)

    # Fetch ownership data
    players_result = client.table("bronze_fpl_players").select("*").execute()
    players_data = players_result.data

    if not players_data:
        logger.info("    No bronze FPL players data")
        return False

    # Transform - ownership data with UUID resolution
    transformed = []
    for record in players_data:
        player_id = record.get("id")
        season = "2025-26"  # FPL data is current season only

        # Get latest gameweek and fixture for this player
        gw_fixture = player_gw_fixture.get(player_id)
        latest_gw = gw_fixture[0] if gw_fixture else None
        fixture = gw_fixture[1] if gw_fixture and len(gw_fixture) > 1 else None

        # Build record with ownership + context
        filtered = {k: v for k, v in record.items() if k in FPL_FANTASY_STATS_COLS}
        filtered["season"] = season
        filtered["gameweek"] = latest_gw

        # Resolve UUIDs
        filtered["unified_player_id"] = player_lookup.get((season, int(player_id)))
        if fixture:
            filtered["match_id"] = match_lookup.get((season, int(fixture)))

        # Drop redundant source-specific columns
        filtered.pop("player_id", None)
        filtered.pop("element", None)

        transformed.append(clean_and_flag_record(filtered, category="gw"))

    for i in range(0, len(transformed), BATCH_SIZE):
        chunk = transformed[i : i + BATCH_SIZE]
        client.table("silver_fpl_fantasy_stats").upsert(chunk).execute()

    logger.info(f"    Updated {len(transformed)} fantasy stats (with UUIDs resolved)")
    return True

    logger.info(f"    Updated {len(transformed)} fantasy stats")
    return True


# Columns for silver_unified_player_stats (combines FPL + Understat)
UNIFIED_PLAYER_STATS_COLS = [
    "player_id",
    "season",
    "gameweek",
    "team_id",
    "position",
    "position_id",
    "game_id",
    # Core match stats (FPL)
    "total_points",
    "goals_scored",
    "assists",
    "clean_sheets",
    "goals_conceded",
    "starts",
    # Minutes (Understat - more accurate)
    "minutes",
    # Expected stats (Understat - superior)
    "xg",
    "xa",
    "xg_chain",
    "xg_buildup",
    # FPL expected (fallback)
    "expected_goals",
    "expected_assists",
    "expected_goal_involvements",
    "expected_goals_conceded",
    # Shot/Creative (Understat)
    "shots",
    "key_passes",
    # Discipline (FPL)
    "yellow_cards",
    "red_cards",
    "own_goals",
    "penalties_saved",
    "penalties_missed",
    # Bonus/BPS (FPL)
    "bonus",
    "bps",
    # ICT (FPL)
    "influence",
    "creativity",
    "threat",
    "ict_index",
    # Defensive (FPL)
    "tackles",
    "clearances_blocks_interceptions",
    "recoveries",
    "defensive_contribution",
    "saves",
    # Match context (IDs only)
    "was_home",
    "opponent_team_id",
    "fixture_id",
    "kickoff_time",
    "home_score",
    "away_score",
    # Data quality
    "data_quality_score",
    "is_incomplete",
    "missing_fields",
]


def update_unified_player_stats(client: Any) -> bool:
    """Update silver_unified_player_stats by merging FPL + Understat data.

    Join key: (player_id, season, match_date)
    Uses player_mapping to translate Understat IDs to FPL IDs
    """
    logger.info("  Updating unified player stats (FPL + Understat)...")

    page_size = 1000

    # Step 1: Build player ID mapping (multiple sources)
    # Try in order: fpl_id → understat_id, then vaastav_id → understat_id
    understat_to_fpl: dict[tuple, int] = {}  # (season, understat_id) → fpl_id
    offset = 0
    while True:
        player_result = (
            client.table("silver_player_mapping")
            .select("season, fpl_id, vaastav_id, understat_id")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        if not player_result.data:
            break
        for rec in player_result.data:
            season = rec.get("season")
            # Primary: fpl_id -> understat_id
            if season and rec.get("fpl_id") and rec.get("understat_id"):
                understat_to_fpl[(season, rec["understat_id"])] = rec["fpl_id"]
            # Fallback: vaastav_id -> understat_id
            if season and rec.get("vaastav_id") and rec.get("understat_id"):
                understat_to_fpl[(season, rec["understat_id"])] = rec["vaastav_id"]
        if len(player_result.data) < page_size:
            break
        offset += page_size
    logger.info(
        f"    Loaded {len(understat_to_fpl)} player mappings (Understat→FPL/vaastav)"
    )

    # Step 2: Build team ID mapping (understat_team_id → fpl_team_id)
    team_map: dict[tuple, int] = {}  # (season, understat_team_id) → fpl_team_id
    offset = 0
    while True:
        team_result = (
            client.table("silver_team_mapping")
            .select("season, fpl_team_id, understat_team_id")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        if not team_result.data:
            break
        for rec in team_result.data:
            if (
                rec.get("season")
                and rec.get("understat_team_id")
                and rec.get("fpl_team_id")
            ):
                key = (rec["season"], rec["understat_team_id"])
                team_map[key] = rec["fpl_team_id"]
        if len(team_result.data) < page_size:
            break
        offset += page_size
    logger.info(f"    Loaded {len(team_map)} team mappings")

    # Step 3: Build Understat match date lookup (game_id → (date, season))
    match_dates: dict[int, tuple[str, str]] = {}
    offset = 0
    while True:
        match_result = (
            client.table("silver_understat_match_stats")
            .select("game_id, date, season")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        if not match_result.data:
            break
        for rec in match_result.data:
            if rec.get("game_id") and rec.get("date"):
                match_dates[rec["game_id"]] = (str(rec["date"]), rec.get("season"))
        if len(match_result.data) < page_size:
            break
        offset += page_size
    logger.info(f"    Loaded {len(match_dates)} match dates")

    # Step 4: Get FPL player stats - build key
    fpl_data = {}
    offset = 0
    while True:
        fpl_result = (
            client.table("silver_fpl_player_stats")
            .select("*")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        if not fpl_result.data:
            break
        for rec in fpl_result.data:
            player_id = rec.get("player_id")
            season = rec.get("season")
            kickoff = rec.get("kickoff_time", "")
            match_date = kickoff.split("T")[0] if kickoff else ""

            if player_id and season and match_date:
                key = (player_id, season, match_date)
                fpl_data[key] = rec
        logger.info(f"    Fetched FPL: {len(fpl_data)} records...")
        if len(fpl_result.data) < page_size:
            break
        offset += page_size

    # Step 5: Get Understat player stats - map player IDs and build key
    understat_data = {}
    offset = 0
    while True:
        understat_result = (
            client.table("silver_understat_player_stats")
            .select("*")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        if not understat_result.data:
            break
        for rec in understat_result.data:
            understat_pid = rec.get("player_id")
            season = rec.get("season")
            game_id = rec.get("game_id")

            # Map Understat player_id to FPL player_id (or vaastav_id)
            fpl_player_id = understat_to_fpl.get((season, understat_pid), understat_pid)

            # Get date from match
            date_season = match_dates.get(game_id)
            match_date = ""
            season_to_use = season
            if date_season:
                match_date, understat_season = date_season
                if understat_season:
                    season_to_use = understat_season

            if fpl_player_id and season_to_use and match_date:
                key = (fpl_player_id, season_to_use, match_date)
                understat_data[key] = rec
        logger.info(f"    Fetched Understat: {len(understat_data)} records...")
        if len(understat_result.data) < page_size:
            break
        offset += page_size

    if not fpl_data and not understat_data:
        logger.info("    No source data available")
        return False

    # Step 6: Merge
    unified_records = {}
    merged_count = 0

    # First add all FPL records
    for key, fpl_rec in fpl_data.items():
        unified = {k: v for k, v in fpl_rec.items() if k in UNIFIED_PLAYER_STATS_COLS}
        if "team_a_score" in fpl_rec:
            unified["away_score"] = fpl_rec.get("team_a_score")
        if "team_h_score" in fpl_rec:
            unified["home_score"] = fpl_rec.get("team_h_score")
        if "fixture" in fpl_rec:
            unified["fixture_id"] = fpl_rec.get("fixture")
        unified["player_id"] = key[0]
        unified_records[key] = unified

    # Merge Understat data
    understat_only_count = 0
    for key, under_rec in understat_data.items():
        if key in unified_records:
            merged_count += 1
            unified = unified_records[key]
            for col in [
                "xg",
                "xa",
                "xg_chain",
                "xg_buildup",
                "shots",
                "key_passes",
                "game_id",
            ]:
                if (
                    col in under_rec
                    and under_rec[col] is not None
                    and unified.get(col) is None
                ):
                    unified[col] = under_rec[col]
            if "minutes" in under_rec and under_rec["minutes"] is not None:
                if (
                    unified.get("minutes") is None
                    or under_rec["minutes"] > unified["minutes"]
                ):
                    unified["minutes"] = under_rec["minutes"]
        else:
            understat_only_count += 1
            unified = {}
            for col in [
                "xg",
                "xa",
                "xg_chain",
                "xg_buildup",
                "shots",
                "key_passes",
                "minutes",
                "game_id",
            ]:
                if col in under_rec and under_rec[col] is not None:
                    unified[col] = under_rec[col]
            unified["player_id"] = key[0]
            unified_records[key] = unified

    logger.info(
        f"    Merged: {merged_count} FPL+Understat, {understat_only_count} Understat-only"
    )

    # Stats
    both_count = sum(
        1
        for k, v in unified_records.items()
        if v.get("xg") is not None and v.get("total_points") is not None
    )
    fpl_only = sum(
        1
        for k, v in unified_records.items()
        if v.get("total_points") is not None and v.get("xg") is None
    )
    under_only = sum(
        1
        for k, v in unified_records.items()
        if v.get("xg") is not None and v.get("total_points") is None
    )
    logger.info(
        f"    Both: {both_count}, FPL only: {fpl_only}, Understat only: {under_only}"
    )
    logger.info(
        f"    Match rate: {both_count / (both_count + fpl_only + under_only) * 100:.1f}%"
    )

    # Step 7: Transform and upload
    transformed = []
    for key, record in unified_records.items():
        cleaned = clean_and_flag_record(record, category="merged")
        cleaned["player_id"] = key[0]
        cleaned["season"] = key[1]
        cleaned["gameweek"] = record.get("gameweek", 0)
        transformed.append(cleaned)

    for i in range(0, len(transformed), BATCH_SIZE):
        chunk = transformed[i : i + BATCH_SIZE]
        client.table("silver_unified_player_stats").upsert(chunk).execute()

    logger.info(f"    Updated {len(transformed)} unified player stats")
    return True


# Columns that exist in silver_fpl_player_stats
FPL_PLAYER_STATS_COLS = [
    "player_id",
    "season",
    "gameweek",
    "source",
    "total_points",
    "minutes",
    "goals_scored",
    "assists",
    "clean_sheets",
    "goals_conceded",
    "expected_goals",
    "expected_assists",
    "expected_goal_involvements",
    "expected_goals_conceded",
    "bonus",
    "bps",
    "influence",
    "creativity",
    "threat",
    "ict_index",
    "own_goals",
    "penalties_saved",
    "penalties_missed",
    "yellow_cards",
    "red_cards",
    "saves",
    "starts",
    "clearances_blocks_interceptions",
    "recoveries",
    "tackles",
    "defensive_contribution",
    "was_home",
    "kickoff_time",
    "team_a_score",
    "team_h_score",
    "data_quality_score",
    "is_incomplete",
    "missing_fields",
    # UUID columns (added during transform)
    # Note: opponent_unified_team_id is derived from match_id + was_home
    "unified_player_id",
    "match_id",
    # Redundant columns to drop during transform (now using UUIDs)
    "player_id",  # -> unified_player_id
    "fixture",  # -> match_id
    "opponent_team",  # can derive from match_id + was_home
]


def update_fpl_player_stats(client: Any) -> bool:
    """Update silver_fpl_player_stats from bronze WITH UUID resolution.

    Processes both FPL (current season) and Vaastav (historical seasons) data.
    - Resolves unified_player_id from player_mapping
    - Resolves match_id from match_mapping
    - Drops redundant source-specific columns (player_id, fixture, opponent_team)
    """
    logger.info("  Updating FPL player stats from bronze...")

    # Truncate table to avoid duplicates from multiple runs
    # DELETE is too slow on large tables, TRUNCATE is instant
    truncate_table("silver_fpl_player_stats")

    # Load mappings ONCE - use fetch_all_paginated for complete data
    # Lookup by both fpl_id AND vaastav_id for 100% resolution
    player_lookup: dict[tuple[str, int], str] = {}
    for r in fetch_all_paginated(
        client,
        "silver_player_mapping",
        select_cols="season,fpl_id,vaastav_id,unified_player_id",
    ):
        season = r.get("season")
        unified_id = r.get("unified_player_id")
        if season and unified_id:
            if r.get("fpl_id"):
                player_lookup[(season, int(r["fpl_id"]))] = unified_id
            if r.get("vaastav_id"):
                player_lookup[(season, int(r["vaastav_id"]))] = unified_id

    # Match lookup: both FPL and Vaastav fixture IDs
    match_lookup: dict[tuple[str, int], str] = {}
    for r in fetch_all_paginated(
        client,
        "silver_match_mapping",
        select_cols="season,fpl_fixture_id,vaastav_fixture_id,match_id",
    ):
        if r.get("season") and r.get("match_id"):
            if r.get("fpl_fixture_id"):
                match_lookup[(r["season"], int(r["fpl_fixture_id"]))] = r["match_id"]
            if r.get("vaastav_fixture_id"):
                match_lookup[(r["season"], int(r["vaastav_fixture_id"]))] = r[
                    "match_id"
                ]

    logger.info(
        f"    Loaded {len(player_lookup)} player lookups, {len(match_lookup)} match lookups"
    )

    # ==== Process FPL data (current season: 2025-26) ====
    fpl_bronze = fetch_all_paginated(client, "bronze_fpl_gw", select_cols="*")
    if fpl_bronze:
        logger.info(f"    Processing {len(fpl_bronze)} FPL bronze records...")

        transformed = []
        for record in fpl_bronze:
            season = record.get("season")
            element = record.get("element")  # FPL player ID
            fixture = record.get("fixture")  # FPL fixture ID
            was_home = record.get("was_home")

            record.pop("updated_at", None)

            # Filter to expected columns
            filtered = {k: v for k, v in record.items() if k in FPL_PLAYER_STATS_COLS}

            # Map FPL-specific fields
            if "element" in record:
                filtered["player_id"] = record["element"]
            if "round" in record:
                filtered["gameweek"] = record["round"]

            filtered["source"] = "fpl"

            # Drop redundant source-specific columns (now using UUIDs)
            filtered.pop("opponent_team", None)
            filtered.pop("fixture", None)

            # Resolve UUIDs during transform (JOIN in memory)
            if season and element:
                filtered["unified_player_id"] = player_lookup.get(
                    (season, int(element))
                )
            if season and fixture:
                filtered["match_id"] = match_lookup.get((season, int(fixture)))

            # Drop old identifier - we're using UUID now
            filtered.pop("player_id", None)

            transformed.append(clean_and_flag_record(filtered, category="gw"))

        # Save to silver
        for i in range(0, len(transformed), BATCH_SIZE):
            chunk = transformed[i : i + BATCH_SIZE]
            client.table("silver_fpl_player_stats").upsert(chunk).execute()

        logger.info(
            f"    Updated {len(transformed)} FPL player stats (with UUIDs resolved)"
        )

    # ==== Process Vaastav data (historical seasons) ====
    vaastav_bronze = fetch_all_paginated(
        client, "bronze_vaastav_player_history_gw", select_cols="*"
    )
    if vaastav_bronze:
        logger.info(f"    Processing {len(vaastav_bronze)} Vaastav bronze records...")

        transformed = []
        for record in vaastav_bronze:
            season = record.get("season")
            player_id = record.get("player_id")  # Vaastav player ID
            fixture = record.get("fixture")  # Vaastav fixture ID
            was_home = record.get("was_home")

            record.pop("updated_at", None)

            # Filter to expected columns (use only columns that exist in the schema)
            filtered = {k: v for k, v in record.items() if k in FPL_PLAYER_STATS_COLS}

            # Vaastav already has player_id and gameweek in correct format
            filtered["source"] = "vaastav"

            # Drop redundant source-specific columns (now using UUIDs)
            filtered.pop("opponent_team", None)
            filtered.pop("fixture", None)

            # Resolve UUIDs during transform (JOIN in memory)
            if season and player_id:
                filtered["unified_player_id"] = player_lookup.get(
                    (season, int(player_id))
                )
            if season and fixture:
                filtered["match_id"] = match_lookup.get((season, int(fixture)))

            # Drop old identifier - we're using UUID now
            filtered.pop("player_id", None)

            transformed.append(clean_and_flag_record(filtered, category="vaastav_gw"))

        # Save to silver
        for i in range(0, len(transformed), BATCH_SIZE):
            chunk = transformed[i : i + BATCH_SIZE]
            client.table("silver_fpl_player_stats").upsert(chunk).execute()

        logger.info(
            f"    Updated {len(transformed)} Vaastav player stats (with UUIDs resolved)"
        )

    return True


def merge_player_state_to_fantasy_stats(client: Any) -> bool:
    """Merge silver_player_state into silver_fpl_fantasy_stats.

    Note: This function is deprecated. Player state is now loaded directly
    in update_fpl_fantasy_stats from bronze_fpl_players.
    """
    logger.info(
        "  Merging player state into fantasy stats (deprecated - data loaded in update_fpl_fantasy_stats)..."
    )
    return True


# ==================== Fixtures ====================


# Columns that exist in silver_fixtures (updated - no more team_h/team_a IDs)
FIXTURE_COLS = [
    "id",  # Keep original fixture ID - it's the PK
    "event",
    "finished",
    "started",
    "team_h_score",
    "team_a_score",
    "kickoff_time",
    "team_h_difficulty",
    "team_a_difficulty",
    "pulse_id",
    "season",
    "code",
    "source",
    "data_quality_score",
    "is_incomplete",
    "missing_fields",
    # Defensive metrics (aggregated from player stats)
    "team_h_tackles",
    "team_a_tackles",
    "team_h_clearances_blocks_interceptions",
    "team_a_clearances_blocks_interceptions",
    "team_h_recoveries",
    "team_a_recoveries",
    "team_h_defensive_contribution",
    "team_a_defensive_contribution",
    "team_h_saves",
    "team_a_saves",
    # UUID columns (added during transform)
    "match_id",
    "home_unified_team_id",
    "away_unified_team_id",
]

# Defensive columns to aggregate from player stats
DEFENSIVE_COLS = [
    "tackles",
    "clearances_blocks_interceptions",
    "recoveries",
    "defensive_contribution",
    "saves",
]


def update_fixtures(client: Any) -> bool:
    """Update silver_fixtures from all sources (FPL + Vaastav + Understat).

    Resolves match_id and team UUIDs (home/away).
    Includes current season (FPL) and historical (Vaastav).
    """
    logger.info("  Updating fixtures...")

    # Truncate before load to avoid duplicates
    truncate_table("silver_fixtures")

    # Load team mappings for UUID resolution
    # Key: (season, fpl_team_id) -> unified_team_id
    # Also build: (season, vaastav_team_name) -> unified_team_id
    team_lookup: dict[tuple[str, int], str] = {}
    vaastav_team_lookup: dict[tuple[str, str], str] = {}
    for r in fetch_all_paginated(
        client,
        "silver_team_mapping",
        select_cols="season,fpl_team_id,vaastav_team_name,unified_team_id",
    ):
        if r.get("season") and r.get("unified_team_id"):
            if r.get("fpl_team_id"):
                team_lookup[(r["season"], int(r["fpl_team_id"]))] = r["unified_team_id"]
            if r.get("vaastav_team_name"):
                vaastav_team_lookup[(r["season"], r["vaastav_team_name"])] = r[
                    "unified_team_id"
                ]

    # Load match mappings
    # Key: (season, fpl_fixture_id) -> match_id
    # Also: (season, vaastav_fixture_id) -> match_id
    match_lookup: dict[tuple[str, int], str] = {}
    vaastav_match_lookup: dict[tuple[str, int], str] = {}
    for r in fetch_all_paginated(
        client,
        "silver_match_mapping",
        select_cols="season,fpl_fixture_id,vaastav_fixture_id,match_id",
    ):
        if r.get("season") and r.get("match_id"):
            if r.get("fpl_fixture_id"):
                match_lookup[(r["season"], int(r["fpl_fixture_id"]))] = r["match_id"]
            if r.get("vaastav_fixture_id"):
                vaastav_match_lookup[(r["season"], int(r["vaastav_fixture_id"]))] = r[
                    "match_id"
                ]

    logger.info(
        f"    Loaded {len(team_lookup)} team lookups, {len(match_lookup)} match lookups"
    )

    # Step 1: Load FPL fixtures (current season)
    fpl_fixtures = client.table("bronze_fpl_fixtures").select("*").execute().data or []
    logger.info(f"    Loaded {len(fpl_fixtures)} FPL fixtures")

    # Step 2: Load Vaastav fixtures (historical seasons)
    vaastav_fixtures = fetch_all_paginated(
        client, "bronze_vaastav_fixtures", select_cols="*"
    )
    logger.info(f"    Loaded {len(vaastav_fixtures)} Vaastav fixtures")

    # Combine all fixtures
    all_fixtures = []

    # Process FPL fixtures
    for record in fpl_fixtures:
        fixture_id = record.get("id")
        team_h = record.get("team_h")
        team_a = record.get("team_a")
        season = record.get("season", "2025-26")

        cleaned = {k: v for k, v in record.items() if k in FIXTURE_COLS}
        cleaned.pop("updated_at", None)
        cleaned.pop("team_h", None)
        cleaned.pop("team_a", None)
        cleaned["source"] = "fpl"
        cleaned["match_id"] = match_lookup.get((season, fixture_id))
        cleaned["home_unified_team_id"] = team_lookup.get((season, team_h))
        cleaned["away_unified_team_id"] = team_lookup.get((season, team_a))
        all_fixtures.append(cleaned)

    # Process Vaastav fixtures
    for record in vaastav_fixtures:
        fixture_id = record.get("id")
        team_h = record.get("team_h")
        team_a = record.get("team_a")
        season = record.get("season", "2025-26")

        # Vaastav uses team names directly
        cleaned = {
            "id": fixture_id,
            "season": season,
            "event": record.get("event"),
            "kickoff_time": record.get("kickoff_time"),
            "finished": record.get("finished"),
            "started": record.get("started"),
            "team_h_score": record.get("team_h_score"),
            "team_a_score": record.get("team_a_score"),
            "source": "vaastav",
            "match_id": vaastav_match_lookup.get((season, fixture_id)),
            "home_unified_team_id": vaastav_team_lookup.get((season, team_h)),
            "away_unified_team_id": vaastav_team_lookup.get((season, team_a)),
        }
        # Add empty defensive metrics (not available in vaastav)
        for col in [
            "team_h_tackles",
            "team_a_tackles",
            "team_h_clearances_blocks_interceptions",
            "team_a_clearances_blocks_interceptions",
            "team_h_recoveries",
            "team_a_recoveries",
            "team_h_defensive_contribution",
            "team_a_defensive_contribution",
            "team_h_saves",
            "team_a_saves",
        ]:
            cleaned[col] = 0
        all_fixtures.append(cleaned)

    logger.info(f"    Total fixtures: {len(all_fixtures)}")

    # Upload to silver
    if all_fixtures:
        for i in range(0, len(all_fixtures), 500):
            batch = all_fixtures[i : i + 500]
            client.table("silver_fixtures").upsert(batch).execute()
        logger.info(f"    Uploaded {len(all_fixtures)} fixtures to silver_fixtures")
        return True

    logger.info("    No fixtures to upload")
    return False


# ==================== Understat Data ====================


# Columns that exist in silver_understat_shots
UNDERSTAT_SHOTS_COLS = [
    "id",
    "game_id",
    "player_id",
    "team_id",
    "assist_player_id",
    "assist_player",
    "xg",
    "location_x",
    "location_y",
    "minute",
    "body_part",
    "situation",
    "result",
    "date",
    "season",
    "league_id",
    "season_id",
    "shot_id",
    "data_quality_score",
    "is_incomplete",
    "missing_fields",
]


def update_understat_shots(client: Any) -> bool:
    """Update silver_understat_shots from bronze (raw shot events, PL only).

    Resolves unified_player_id and unified_team_id for FK references.
    Excludes 2020-21 season (no player mappings available).
    """
    logger.info(
        "  Updating Understat shots (Premier League only, excluding 2020-21)..."
    )

    # Load mappings ONCE for UUID resolution
    # Key: (season, understat_player_id) -> unified_player_id
    player_lookup: dict[tuple[str, int], str] = {}
    for r in fetch_all_paginated(
        client,
        "silver_player_mapping",
        select_cols="season,understat_id,unified_player_id",
    ):
        if r.get("season") and r.get("understat_id") and r.get("unified_player_id"):
            player_lookup[(r["season"], int(r["understat_id"]))] = r[
                "unified_player_id"
            ]

    # Key: (season, understat_team_id) -> unified_team_id
    team_lookup: dict[tuple[str, int], str] = {}
    for r in fetch_all_paginated(
        client,
        "silver_team_mapping",
        select_cols="season,understat_team_id,unified_team_id",
    ):
        if r.get("season") and r.get("understat_team_id") and r.get("unified_team_id"):
            team_lookup[(r["season"], int(r["understat_team_id"]))] = r[
                "unified_team_id"
            ]

    logger.info(
        f"    Loaded {len(player_lookup)} player lookups, {len(team_lookup)} team lookups"
    )

    # Load ALL shot data from bronze (all seasons) - filter to PL only, exclude 2020-21
    all_shots = fetch_all_paginated(
        client, "bronze_understat_shots", filters={"league_id": "1"}, select_cols="*"
    )

    # Filter out 2020-21 (no player mappings available)
    all_shots = [s for s in all_shots if s.get("season") != "2020-21"]

    if not all_shots:
        logger.info("    No bronze Understat shots data (after filtering)")
        return False

    logger.info(
        f"    Processing {len(all_shots)} PL shots from seasons 2021-22 onwards..."
    )

    # Transform - filter to known columns and resolve UUIDs
    transformed = []
    for record in all_shots:
        filtered = {k: v for k, v in record.items() if k in UNDERSTAT_SHOTS_COLS}
        filtered.pop("updated_at", None)

        # Resolve UUIDs
        season = record.get("season", "")
        player_id = record.get("player_id")
        team_id = record.get("team_id")

        filtered["unified_player_id"] = player_lookup.get((season, player_id))
        filtered["unified_team_id"] = team_lookup.get((season, team_id))

        transformed.append(clean_and_flag_record(filtered, category="vaastav_gw"))

    # Upload to silver
    for i in range(0, len(transformed), BATCH_SIZE):
        chunk = transformed[i : i + BATCH_SIZE]
        client.table("silver_understat_shots").upsert(chunk).execute()
        logger.info(f"    Uploaded {i + len(chunk)}/{len(transformed)}")

    logger.info(f"    Updated {len(transformed)} shot records with UUIDs")
    return True


def update_understat_player_stats(client: Any) -> bool:
    """Update silver_understat_player_stats from bronze.

    Resolves unified_player_id and unified_team_id for FK references.
    Excludes 2020-21 season (no mappings available).
    """
    logger.info("  Updating Understat player stats (excluding 2020-21)...")

    # Load mappings ONCE for UUID resolution
    player_lookup: dict[tuple[str, int], str] = {}
    for r in fetch_all_paginated(
        client,
        "silver_player_mapping",
        select_cols="season,understat_id,unified_player_id",
    ):
        if r.get("season") and r.get("understat_id") and r.get("unified_player_id"):
            player_lookup[(r["season"], int(r["understat_id"]))] = r[
                "unified_player_id"
            ]

    team_lookup: dict[tuple[str, int], str] = {}
    for r in fetch_all_paginated(
        client,
        "silver_team_mapping",
        select_cols="season,understat_team_id,unified_team_id",
    ):
        if r.get("season") and r.get("understat_team_id") and r.get("unified_team_id"):
            team_lookup[(r["season"], int(r["understat_team_id"]))] = r[
                "unified_team_id"
            ]

    # Load match mappings for match_id (use understat_game_id for Understat matches)
    match_lookup: dict[tuple[str, int], str] = {}
    for r in fetch_all_paginated(
        client, "silver_match_mapping", select_cols="season,understat_game_id,match_id"
    ):
        if r.get("season") and r.get("understat_game_id") and r.get("match_id"):
            match_lookup[(r["season"], int(r["understat_game_id"]))] = r["match_id"]

    logger.info(
        f"    Loaded {len(player_lookup)} player, {len(team_lookup)} team, {len(match_lookup)} match lookups"
    )

    # Fetch all bronze data
    all_bronze_data = fetch_all_paginated(
        client, "bronze_understat_player_stats", select_cols="*"
    )

    # Filter out 2020-21 (no mappings available)
    all_bronze_data = [r for r in all_bronze_data if r.get("season") != "2020-21"]

    if not all_bronze_data:
        logger.info("    No bronze Understat player stats (after filtering)")
        return False

    allowed = {
        "player_id",
        "gameweek",
        "game_id",
        "team_id",
        "position",
        "position_id",
        "minutes",
        "goals",
        "assists",
        "shots",
        "xg",
        "xa",
        "xg_chain",
        "xg_buildup",
        "key_passes",
        "own_goals",
        "yellow_cards",
        "red_cards",
        "season",
        "league_id",
        "season_id",
    }

    transformed = []
    for record in all_bronze_data:
        cleaned = {k: v for k, v in record.items() if k in allowed}
        cleaned.pop("updated_at", None)

        # Resolve UUIDs
        season = record.get("season", "")
        player_id = record.get("player_id")
        team_id = record.get("team_id")
        game_id = record.get("game_id")

        cleaned["unified_player_id"] = player_lookup.get((season, player_id))
        cleaned["unified_team_id"] = team_lookup.get((season, team_id))
        cleaned["match_id"] = match_lookup.get((season, game_id))

        transformed.append(clean_and_flag_record(cleaned, category="vaastav_gw"))

    for i in range(0, len(transformed), BATCH_SIZE):
        chunk = transformed[i : i + BATCH_SIZE]
        client.table("silver_understat_player_stats").upsert(chunk).execute()

    logger.info(f"    Updated {len(transformed)} Understat player stats with UUIDs")
    return True


def update_understat_match_stats(client: Any) -> bool:
    """Update silver_understat_match_stats from bronze + aggregated player stats.

    Resolves unified_team_id for home and away teams.
    """
    logger.info("  Updating Understat match stats...")

    # Load team mappings ONCE
    team_lookup: dict[tuple[str, int], str] = {}
    for r in fetch_all_paginated(
        client,
        "silver_team_mapping",
        select_cols="season,understat_team_id,unified_team_id",
    ):
        if r.get("season") and r.get("understat_team_id") and r.get("unified_team_id"):
            team_lookup[(r["season"], int(r["understat_team_id"]))] = r[
                "unified_team_id"
            ]

    logger.info(f"    Loaded {len(team_lookup)} team lookups")

    # Load match stats from bronze
    bronze_matches = fetch_all_paginated(
        client, "bronze_understat_match_stats", select_cols="*"
    )

    # Filter out 2020-21 (no mappings available)
    bronze_matches = [r for r in bronze_matches if r.get("season") != "2020-21"]

    if not bronze_matches:
        logger.info("    No bronze Understat match stats (after filtering)")
        return False

    # Load player stats for aggregation (already filtered in player_stats function)
    all_player_stats = fetch_all_paginated(
        client,
        "bronze_understat_player_stats",
        select_cols="game_id,team_id,shots,xa,key_passes,yellow_cards,red_cards",
    )

    # Aggregate player stats by team + game
    aggregated: dict[tuple[int, int], dict[str, int | float]] = defaultdict(
        lambda: {col: 0 for col in PLAYER_STATS_COLS}
    )
    for record in all_player_stats:
        key = (record["game_id"], record["team_id"])
        for col in PLAYER_STATS_COLS:
            aggregated[key][col] += record.get(col, 0) or 0

    # Merge match data with aggregated player stats
    allowed_match_cols = {
        "game_id",
        "date",
        "season",
        "home_team_id",
        "away_team_id",
        "home_team",
        "away_team",
        "home_goals",
        "away_goals",
        "home_xg",
        "away_xg",
        "home_np_xg",
        "away_np_xg",
        "home_np_xg_difference",
        "away_np_xg_difference",
        "home_ppda",
        "away_ppda",
        "home_deep_completions",
        "away_deep_completions",
        "home_expected_points",
        "away_expected_points",
        "home_points",
        "away_points",
        "away_team_code",
        "home_team_code",
        "league_id",
    }

    final_updates = []
    for record in bronze_matches:
        game_id = record["game_id"]
        home_team_id = record["home_team_id"]
        away_team_id = record["away_team_id"]

        # Base match data
        combined = {k: v for k, v in record.items() if k in allowed_match_cols}
        combined.pop("updated_at", None)

        # Add aggregated player stats
        home_key = (game_id, home_team_id)
        away_key = (game_id, away_team_id)
        home_stats = aggregated.get(home_key, {})
        away_stats = aggregated.get(away_key, {})

        combined["home_shots"] = home_stats.get("shots", 0)
        combined["home_xa"] = home_stats.get("xa", 0)
        combined["home_key_passes"] = home_stats.get("key_passes", 0)
        combined["home_yellow_cards"] = home_stats.get("yellow_cards", 0)
        combined["home_red_cards"] = home_stats.get("red_cards", 0)
        combined["away_shots"] = away_stats.get("shots", 0)
        combined["away_xa"] = away_stats.get("xa", 0)
        combined["away_key_passes"] = away_stats.get("key_passes", 0)
        combined["away_yellow_cards"] = away_stats.get("yellow_cards", 0)
        combined["away_red_cards"] = away_stats.get("red_cards", 0)

        # Resolve UUIDs for team references
        season = record.get("season", "")
        combined["home_unified_team_id"] = team_lookup.get((season, home_team_id))
        combined["away_unified_team_id"] = team_lookup.get((season, away_team_id))

        combined = clean_and_flag_record(combined, category="vaastav_gw")
        final_updates.append(combined)

    for i in range(0, len(final_updates), BATCH_SIZE):
        chunk = final_updates[i : i + BATCH_SIZE]
        client.table("silver_understat_match_stats").upsert(chunk).execute()

    logger.info(f"    Updated {len(final_updates)} match stats with UUIDs")
    return True


# ==================== Main ====================


def main() -> None:
    """Run daily silver layer update."""
    import os
    import sys

    # Add parent directory to path for scripts imports
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from scripts.populate_silver_match_mapping import populate_match_mapping
    from scripts.daily_team_mapping_update import run as run_team_mapping

    parser = argparse.ArgumentParser(description="Daily Silver Layer Update")
    parser.add_argument("--skip-fpl", action="store_true", help="Skip FPL data updates")
    parser.add_argument(
        "--skip-understat", action="store_true", help="Skip Understat updates"
    )
    parser.add_argument(
        "--skip-fixtures", action="store_true", help="Skip fixtures updates"
    )
    parser.add_argument(
        "--skip-match-mapping", action="store_true", help="Skip match mapping update"
    )
    parser.add_argument(
        "--skip-team-mapping", action="store_true", help="Skip team mapping update"
    )
    args = parser.parse_args()

    load_dotenv()

    if not os.getenv("SUPABASE_URL"):
        logger.error("SUPABASE_URL not set")
        return

    logger.info("Starting daily Silver layer update...")

    client = get_supabase()
    updated = False

    # First: Update team mapping (unified team UUIDs + understat_team_id)
    if not args.skip_team_mapping:
        run_team_mapping()
        logger.info("  ✓ Team mapping updated")
    else:
        logger.info("  Skipping team mapping update")

    # Second: Update match_mapping (unified fixture keys)
    if not args.skip_match_mapping:
        if populate_match_mapping(client):
            updated = True
        logger.info("  ✓ Match mapping updated (unified fixture keys)")
    else:
        logger.info("  Skipping match mapping update")

    # Third: Regenerate player mappings (ensures understat_id is populated for all matches)
    # This is critical for UUID resolution in understat data
    if not args.skip_team_mapping:
        logger.info("  Regenerating player mappings...")
        from src.silver.player_mapping import (
            build_all_season_mappings,
            upload_to_supabase,
        )

        mappings = build_all_season_mappings()
        if not mappings.is_empty():
            # Upload in batches to avoid timeout
            records = mappings.to_dicts()
            for i in range(0, len(records), 500):
                batch = records[i : i + 500]
                client.table("silver_player_mapping").upsert(batch).execute()
            logger.info(f"  ✓ Player mappings updated ({mappings.height} entries)")
        else:
            logger.warning("  ⚠ No player mappings generated")
    else:
        logger.info("  Skipping player mapping update (since team mapping skipped)")

    # FPL Data
    if not args.skip_fpl:
        if update_fpl_fantasy_stats(client):
            updated = True
        if update_fpl_player_stats(client):
            updated = True
        # UUIDs are now resolved in update_fpl_player_stats (step above)
    else:
        logger.info("  Skipping FPL data updates")

    # Fixtures
    if not args.skip_fixtures:
        if update_fixtures(client):
            updated = True
    else:
        logger.info("  Skipping fixtures updates")

    # Understat Data
    if not args.skip_understat:
        if update_understat_shots(client):
            updated = True
        if update_understat_player_stats(client):
            updated = True
        if update_understat_match_stats(client):
            updated = True
    else:
        logger.info("  Skipping Understat updates")

    # Unified Player Stats (merged FPL + Understat)
    if update_unified_player_stats(client):
        updated = True

    if updated:
        logger.info("✓ Silver layer update complete!")
    else:
        logger.info("No changes to Silver layer")


if __name__ == "__main__":
    main()
