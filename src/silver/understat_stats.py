"""Silver layer — Understat data consolidation.

Transforms Bronze Understat data into Silver tables with UUID resolution.
"""

from __future__ import annotations

import logging
from typing import Any

from src.config import BATCH_SIZE, CURRENT_SEASON
from src.utils.safe_upsert import truncate_table
from src.utils.supabase_utils import fetch_all_paginated

logger = logging.getLogger(__name__)


def _load_player_lookup(client: Any) -> dict[tuple[str, int], str]:
    """Load season+understat_id → unified_player_id mapping."""
    lookup: dict[tuple[str, int], str] = {}
    for r in fetch_all_paginated(
        client,
        "silver_player_mapping",
        select_cols="season,understat_id,unified_player_id",
    ):
        season = r.get("season")
        uid = r.get("unified_player_id")
        us_id = r.get("understat_id")
        if season and uid and us_id:
            lookup[(season, int(us_id))] = uid
    return lookup


def _load_match_lookup_by_understat(client: Any) -> dict[tuple[str, int], str]:
    """Load season+understat_game_id → match_id mapping."""
    lookup: dict[tuple[str, int], str] = {}
    for r in fetch_all_paginated(
        client, "silver_match_mapping", select_cols="season,understat_game_id,match_id"
    ):
        if r.get("season") and r.get("understat_game_id") and r.get("match_id"):
            lookup[(r["season"], int(r["understat_game_id"]))] = r["match_id"]
    return lookup


def update_understat_player_stats(client: Any, season: str = CURRENT_SEASON) -> bool:
    """Update silver_understat_player_stats from bronze with UUID resolution."""
    logger.info("  Updating Understat player stats...")

    player_lookup = _load_player_lookup(client)
    match_lookup = _load_match_lookup_by_understat(client)

    truncate_table(client, "silver_understat_player_stats")

    # Fetch bronze data
    all_data = []
    offset = 0
    while True:
        result = (
            client.table("bronze_understat_player_stats")
            .select("*")
            .eq("season", season)
            .range(offset, offset + 999)
            .execute()
        )
        if not result.data:
            break
        all_data.extend(result.data)
        if len(result.data) < 1000:
            break
        offset += 1000

    if not all_data:
        logger.info("    No Understat player stats")
        return False

    transformed = []
    for rec in all_data:
        us_pid = rec.get("player_id")
        game_id = rec.get("game_id")

        unified_id = None
        if us_pid:
            unified_id = player_lookup.get((season, int(us_pid)))

        match_id = None
        if game_id:
            match_id = match_lookup.get((season, int(game_id)))

        # Pass through all bronze columns + UUID resolution
        filtered = dict(rec)  # Copy all fields
        filtered["unified_player_id"] = unified_id
        filtered["match_id"] = match_id
        filtered["season"] = season

        # Clean up fields that shouldn't be in silver
        filtered.pop("updated_at", None)
        filtered.pop("created_at", None)

        # Only include if we have UUID resolution
        if unified_id and match_id:
            transformed.append(filtered)

    for i in range(0, len(transformed), BATCH_SIZE):
        client.table("silver_understat_player_stats").upsert(
            transformed[i : i + BATCH_SIZE]
        ).execute()

    logger.info(f"    Updated {len(transformed)} Understat player stats")
    return True


def update_understat_match_stats(client: Any, season: str = CURRENT_SEASON) -> bool:
    """Update silver_understat_match_stats from bronze with UUID resolution."""
    logger.info("  Updating Understat match stats...")

    match_lookup = _load_match_lookup_by_understat(client)

    # Load team mapping for UUID resolution
    team_lookup: dict[tuple[str, str], str] = {}
    for r in fetch_all_paginated(
        client,
        "silver_team_mapping",
        select_cols="season,understat_team_id,unified_team_id",
    ):
        if r.get("season") and r.get("understat_team_id") and r.get("unified_team_id"):
            team_lookup[(r["season"], str(r["understat_team_id"]))] = r[
                "unified_team_id"
            ]

    truncate_table(client, "silver_understat_match_stats")

    # Fetch bronze data
    result = (
        client.table("bronze_understat_match_stats")
        .select("*")
        .eq("season", season)
        .execute()
    )

    if not result.data:
        logger.info("    No Understat match stats")
        return False

    transformed = []
    for rec in result.data:
        game_id = rec.get("game_id")
        match_id = match_lookup.get((season, int(game_id))) if game_id else None

        home_id_str = str(rec.get("home_team_id") or rec.get("home_id", ""))
        away_id_str = str(rec.get("away_team_id") or rec.get("away_id", ""))

        filtered = {
            "match_id": match_id,
            "game_id": game_id,
            "season": season,
            "date": rec.get("date"),
            "home_team_id": team_lookup.get((season, home_id_str)),
            "away_team_id": team_lookup.get((season, away_id_str)),
            "home_team": rec.get("home_team"),
            "away_team": rec.get("away_team"),
            "home_team_code": rec.get("home_team_code"),
            "away_team_code": rec.get("away_team_code"),
            "home_goals": rec.get("home_goals") or rec.get("h_goals"),
            "away_goals": rec.get("away_goals") or rec.get("a_goals"),
            "home_xg": rec.get("home_xg") or rec.get("h_xg"),
            "away_xg": rec.get("away_xg") or rec.get("a_xg"),
            "home_np_xg": rec.get("home_np_xg"),
            "away_np_xg": rec.get("away_np_xg"),
            "home_np_xg_difference": rec.get("home_np_xg_difference"),
            "away_np_xg_difference": rec.get("away_np_xg_difference"),
            "home_ppda": rec.get("home_ppda"),
            "away_ppda": rec.get("away_ppda"),
            "home_deep_completions": rec.get("home_deep_completions"),
            "away_deep_completions": rec.get("away_deep_completions"),
            "home_expected_points": rec.get("home_expected_points"),
            "away_expected_points": rec.get("away_expected_points"),
            "home_points": rec.get("home_points"),
            "away_points": rec.get("away_points"),
            "home_shots": rec.get("home_shots"),
            "away_shots": rec.get("away_shots"),
            "home_xa": rec.get("home_xa"),
            "away_xa": rec.get("away_xa"),
            "home_key_passes": rec.get("home_key_passes"),
            "away_key_passes": rec.get("away_key_passes"),
            "home_yellow_cards": rec.get("home_yellow_cards"),
            "away_yellow_cards": rec.get("away_yellow_cards"),
            "home_red_cards": rec.get("away_red_cards"),
            "away_red_cards": rec.get("away_red_cards"),
            "league_id": rec.get("league_id"),
            "season_id": rec.get("season_id"),
        }

        # Remove None values so DB handles defaults
        filtered = {k: v for k, v in filtered.items() if v is not None}

        if match_id:
            transformed.append(filtered)

    for i in range(0, len(transformed), BATCH_SIZE):
        client.table("silver_understat_match_stats").upsert(
            transformed[i : i + BATCH_SIZE]
        ).execute()

    logger.info(f"    Updated {len(transformed)} Understat match stats")
    return True


def update_understat_shots(client: Any, season: str = CURRENT_SEASON) -> bool:
    """Update bronze_understat_shots — pass-through with season filter.

    Shots don't need UUID resolution (they're at event level).
    This just ensures the bronze table has the current season data.
    """
    logger.info("  Understat shots — already in bronze, skipping silver copy")
    return False
