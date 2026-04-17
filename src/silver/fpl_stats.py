"""Silver layer — FPL data consolidation.

Transforms Bronze FPL data into Silver tables with UUID resolution.
"""

from __future__ import annotations

import logging
from typing import Any

import polars as pl

from src.config import BATCH_SIZE, CURRENT_SEASON
from src.utils.data_cleaning import clean_and_flag_record
from src.utils.supabase_utils import fetch_all_paginated

logger = logging.getLogger(__name__)


def _load_player_lookup(client: Any) -> dict[tuple[str, int], str]:
    """Load season+player_id → unified_player_id mapping."""
    lookup: dict[tuple[str, int], str] = {}
    for r in fetch_all_paginated(
        client,
        "silver_player_mapping",
        select_cols="season,fpl_id,unified_player_id",
    ):
        season = r.get("season")
        uid = r.get("unified_player_id")
        fpl_id = r.get("fpl_id")
        if season and uid and fpl_id:
            lookup[(season, int(fpl_id))] = uid
    return lookup


def _load_match_lookup(client: Any) -> dict[tuple[str, int], str]:
    """Load season+fpl_fixture_id → match_id mapping."""
    lookup: dict[tuple[str, int], str] = {}
    for r in fetch_all_paginated(
        client, "silver_match_mapping", select_cols="season,fpl_fixture_id,match_id"
    ):
        if r.get("season") and r.get("fpl_fixture_id") and r.get("match_id"):
            lookup[(r["season"], int(r["fpl_fixture_id"]))] = r["match_id"]
    return lookup


def _truncate_table(client: Any, table_name: str) -> None:
    """Truncate a Silver table before reload."""
    import os
    import subprocess

    token = os.getenv("SUPABASE_ACCESS_TOKEN")
    if not token:
        logger.warning(f"  No SUPABASE_ACCESS_TOKEN — skipping truncate for {table_name}")
        return

    result = subprocess.run(
        ["supabase", "db", "query", "--linked", f"TRUNCATE {table_name} CASCADE;"],
        capture_output=True,
        text=True,
        env={**os.environ, "SUPABASE_ACCESS_TOKEN": token},
    )
    if result.returncode != 0:
        logger.warning(f"  Truncate failed for {table_name}: {result.stderr}")
    else:
        logger.info(f"  Truncated {table_name}")


# Columns for silver_fpl_fantasy_stats
FANTASY_STATS_COLS = [
    "value", "selected", "transfers_in", "transfers_out", "now_cost",
    "chance_of_playing_next_round", "chance_of_playing_this_round",
    "news", "status", "form", "selected_by_percent", "in_dreamteam",
    "removed", "corners_and_indirect_freekicks_order",
    "direct_freekicks_order", "penalties_order",
    "data_quality_score", "is_incomplete", "missing_fields",
    "season", "gameweek", "unified_player_id", "match_id",
]


def update_fpl_fantasy_stats(client: Any, season: str = CURRENT_SEASON) -> bool:
    """Update silver_fpl_fantasy_stats from bronze (ownership data).

    Resolves unified_player_id and match_id from mappings.
    """
    logger.info("  Updating FPL fantasy stats from bronze...")

    player_lookup = _load_player_lookup(client)
    match_lookup = _load_match_lookup(client)
    logger.info(f"    Loaded {len(player_lookup)} player, {len(match_lookup)} match lookups")

    _truncate_table(client, "silver_fpl_fantasy_stats")

    # Fetch GW stats for gameweek context
    gw_result = client.table("bronze_fpl_gw").select("element, round, fixture").execute()
    player_gw_fixture: dict[int, tuple] = {}
    for record in gw_result.data:
        pid = record.get("element")
        gw = record.get("round")
        fixture = record.get("fixture")
        if pid and gw:
            if pid not in player_gw_fixture or gw > player_gw_fixture[pid][0]:
                player_gw_fixture[pid] = (gw, fixture)

    # Fetch and transform player data
    players_result = client.table("bronze_fpl_players").select("*").execute()
    if not players_result.data:
        logger.info("    No bronze FPL players data")
        return False

    transformed = []
    for record in players_result.data:
        player_id = record.get("id")
        gw_fixture = player_gw_fixture.get(player_id)
        latest_gw = gw_fixture[0] if gw_fixture else None
        fixture = gw_fixture[1] if gw_fixture and len(gw_fixture) > 1 else None

        filtered = {k: v for k, v in record.items() if k in FANTASY_STATS_COLS}
        filtered["season"] = season
        filtered["gameweek"] = latest_gw
        filtered["unified_player_id"] = player_lookup.get((season, int(player_id)))
        if fixture:
            filtered["match_id"] = match_lookup.get((season, int(fixture)))

        filtered.pop("player_id", None)
        filtered.pop("element", None)
        transformed.append(clean_and_flag_record(filtered, category="gw"))

    for i in range(0, len(transformed), BATCH_SIZE):
        client.table("silver_fpl_fantasy_stats").upsert(
            transformed[i : i + BATCH_SIZE]
        ).execute()

    logger.info(f"    Updated {len(transformed)} fantasy stats")
    return True


# Columns for silver_fpl_player_stats
PLAYER_STATS_COLS = [
    "player_id", "season", "gameweek", "team_id", "position", "position_id",
    "game_id", "total_points", "goals_scored", "assists", "clean_sheets",
    "goals_conceded", "starts", "minutes", "expected_goals", "expected_assists",
    "expected_goal_involvements", "expected_goals_conceded",
    "yellow_cards", "red_cards", "own_goals", "penalties_saved",
    "penalties_missed", "bonus", "bps", "influence", "creativity",
    "threat", "ict_index", "tackles", "clearances_blocks_interceptions",
    "recoveries", "defensive_contribution", "saves", "was_home",
    "opponent_team_id", "fixture_id", "kickoff_time", "home_score", "away_score",
    "data_quality_score", "is_incomplete", "missing_fields",
    "unified_player_id", "match_id",
]


def update_fpl_player_stats(client: Any, season: str = CURRENT_SEASON) -> bool:
    """Update silver_fpl_player_stats from bronze_fpl_gw with UUID resolution."""
    logger.info("  Updating FPL player stats from bronze...")

    player_lookup = _load_player_lookup(client)
    match_lookup = _load_match_lookup(client)

    _truncate_table(client, "silver_fpl_player_stats")

    # Fetch all GW data
    all_gw = []
    offset = 0
    while True:
        result = (
            client.table("bronze_fpl_gw")
            .select("*")
            .eq("season", season)
            .range(offset, offset + 999)
            .execute()
        )
        if not result.data:
            break
        all_gw.extend(result.data)
        if len(result.data) < 1000:
            break
        offset += 1000

    if not all_gw:
        logger.info("    No GW data")
        return False

    transformed = []
    for rec in all_gw:
        player_id = rec.get("element")
        fixture_id = rec.get("fixture")

        # Build record with selected columns
        filtered = {}
        for col in PLAYER_STATS_COLS:
            if col in rec:
                filtered[col] = rec[col]

        # Resolve UUIDs
        if player_id:
            filtered["unified_player_id"] = player_lookup.get((season, int(player_id)))
        if fixture_id:
            filtered["match_id"] = match_lookup.get((season, int(fixture_id)))
            filtered["fixture_id"] = fixture_id

        # Map scores
        filtered["home_score"] = rec.get("team_h_score")
        filtered["away_score"] = rec.get("team_a_score")
        filtered["season"] = season

        # Clean and append
        filtered.pop("element", None)
        filtered.pop("fixture", None)
        transformed.append(clean_and_flag_record(filtered, category="gw"))

    for i in range(0, len(transformed), BATCH_SIZE):
        client.table("silver_fpl_player_stats").upsert(
            transformed[i : i + BATCH_SIZE]
        ).execute()

    logger.info(f"    Updated {len(transformed)} player stats")
    return True
