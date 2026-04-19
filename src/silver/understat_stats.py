"""Silver layer — Understat data consolidation.

Transforms Bronze Understat data into Silver tables with UUID resolution.
"""

from __future__ import annotations

import logging
from typing import Any

from src.config import BATCH_SIZE, CURRENT_SEASON
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


def _truncate_table(client: Any, table_name: str) -> None:
    """Truncate a Silver table before reload."""
    import os
    import subprocess

    token = os.getenv("SUPABASE_ACCESS_TOKEN")
    if not token:
        return

    try:
        result = subprocess.run(
            ["supabase", "db", "query", "--linked", f"TRUNCATE {table_name} CASCADE;"],
            capture_output=True,
            text=True,
            env={**os.environ, "SUPABASE_ACCESS_TOKEN": token},
        )
        if result.returncode != 0:
            logger.warning(f"  Truncate failed for {table_name}: {result.stderr}")
    except FileNotFoundError:
        logger.debug(
            f"  supabase CLI not available — skipping truncate for {table_name}"
        )


def update_understat_player_stats(client: Any, season: str = CURRENT_SEASON) -> bool:
    """Update silver_understat_player_stats from bronze with UUID resolution."""
    logger.info("  Updating Understat player stats...")

    player_lookup = _load_player_lookup(client)
    match_lookup = _load_match_lookup_by_understat(client)

    _truncate_table(client, "silver_understat_player_stats")

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

    _truncate_table(client, "silver_understat_match_stats")

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

        home_id_str = str(rec.get("home_id", ""))
        away_id_str = str(rec.get("away_id", ""))

        filtered = {
            "match_id": match_id,
            "season": season,
            "home_team_id": team_lookup.get((season, home_id_str)),
            "away_team_id": team_lookup.get((season, away_id_str)),
            "home_goals": rec.get("h_goals"),
            "away_goals": rec.get("a_goals"),
            "home_xg": rec.get("home_xg") or rec.get("h_xg"),
            "away_xg": rec.get("away_xg") or rec.get("a_xg"),
            "date": rec.get("date"),
            "game_id": game_id,
        }

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
