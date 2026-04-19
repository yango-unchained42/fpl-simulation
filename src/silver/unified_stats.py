"""Silver layer — Unified player stats (merged FPL + Understat).

Creates the main feature source table by joining FPL player stats with
Understat player stats on (player_id, match_id).
"""

from __future__ import annotations

import logging
from typing import Any

from src.config import BATCH_SIZE, CURRENT_SEASON
<<<<<<< HEAD
from src.utils.safe_upsert import truncate_table
=======
>>>>>>> origin/main

logger = logging.getLogger(__name__)


<<<<<<< HEAD
=======
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


>>>>>>> origin/main
# Understat columns to pull into unified stats
UNDERSTAT_COLS = [
    "xg",
    "xa",
    "xg_chain",
    "xg_buildup",
    "shots",
    "key_passes",
    "minutes",
]


def update_unified_player_stats(client: Any, season: str = CURRENT_SEASON) -> bool:
    """Update silver_unified_player_stats by merging FPL + Understat.

    Join key: (unified_player_id, match_id)
    FPL stats are the base; Understat fills in xG/xA/shots where available.
    """
    logger.info("  Building unified player stats (FPL + Understat)...")

    truncate_table(client, "silver_unified_player_stats")

    # Fetch FPL player stats
    fpl_data: dict[tuple, dict] = {}
    offset = 0
    while True:
        result = (
            client.table("silver_fpl_player_stats")
            .select("*")
            .eq("season", season)
            .range(offset, offset + 999)
            .execute()
        )
        if not result.data:
            break
        for rec in result.data:
            pid = rec.get("unified_player_id")
            mid = rec.get("match_id")
            if pid and mid:
                fpl_data[(pid, mid)] = rec
        if len(result.data) < 1000:
            break
        offset += 1000

    logger.info(f"    Loaded {len(fpl_data)} FPL player stats")

    # Fetch Understat player stats
    understat_data: dict[tuple, dict] = {}
    offset = 0
    while True:
        result = (
            client.table("silver_understat_player_stats")
            .select("*")
            .eq("season", season)
            .range(offset, offset + 999)
            .execute()
        )
        if not result.data:
            break
        for rec in result.data:
            pid = rec.get("unified_player_id")
            mid = rec.get("match_id")
            if pid and mid:
                understat_data[(pid, mid)] = rec
        if len(result.data) < 1000:
            break
        offset += 1000

    logger.info(f"    Loaded {len(understat_data)} Understat player stats")

    if not fpl_data and not understat_data:
        logger.info("    No source data available")
        return False

    # Merge
    unified_records: list[dict] = []
    merged_count = 0
    understat_only = 0

    # Start with FPL data, enrich with Understat
    for key, fpl_rec in fpl_data.items():
        unified = {
            "unified_player_id": key[0],
            "match_id": key[1],
            "season": season,
            "gameweek": fpl_rec.get("gameweek"),
            "team_id": fpl_rec.get("team_id"),
            "position": fpl_rec.get("position"),
            # FPL stats
            "total_points": fpl_rec.get("total_points"),
            "minutes": fpl_rec.get("minutes"),
            "goals_scored": fpl_rec.get("goals_scored"),
            "assists": fpl_rec.get("assists"),
            "clean_sheets": fpl_rec.get("clean_sheets"),
            "goals_conceded": fpl_rec.get("goals_conceded"),
            "starts": fpl_rec.get("starts"),
            "expected_goals": fpl_rec.get("expected_goals"),
            "expected_assists": fpl_rec.get("expected_assists"),
            "expected_goal_involvements": fpl_rec.get("expected_goal_involvements"),
            "expected_goals_conceded": fpl_rec.get("expected_goals_conceded"),
            # Discipline
            "yellow_cards": fpl_rec.get("yellow_cards"),
            "red_cards": fpl_rec.get("red_cards"),
            "own_goals": fpl_rec.get("own_goals"),
            "penalties_saved": fpl_rec.get("penalties_saved"),
            "penalties_missed": fpl_rec.get("penalties_missed"),
            # Bonus
            "bonus": fpl_rec.get("bonus"),
            "bps": fpl_rec.get("bps"),
            # ICT
            "influence": fpl_rec.get("influence"),
            "creativity": fpl_rec.get("creativity"),
            "threat": fpl_rec.get("threat"),
            "ict_index": fpl_rec.get("ict_index"),
            # Defensive
            "tackles": fpl_rec.get("tackles"),
            "clearances_blocks_interceptions": fpl_rec.get(
                "clearances_blocks_interceptions"
            ),
            "recoveries": fpl_rec.get("recoveries"),
            "defensive_contribution": fpl_rec.get("defensive_contribution"),
            "saves": fpl_rec.get("saves"),
            # Match context
            "was_home": fpl_rec.get("was_home"),
            "opponent_team_id": fpl_rec.get("opponent_team_id"),
            "fixture_id": fpl_rec.get("fixture_id"),
            "kickoff_time": fpl_rec.get("kickoff_time"),
            "home_score": fpl_rec.get("home_score"),
            "away_score": fpl_rec.get("away_score"),
            # Quality
            "data_quality_score": fpl_rec.get("data_quality_score"),
            "is_incomplete": fpl_rec.get("is_incomplete", False),
        }

        # Enrich with Understat if available
        if key in understat_data:
            merged_count += 1
            us_rec = understat_data[key]
            for col in UNDERSTAT_COLS:
                us_val = us_rec.get(col)
                if us_val is not None:
                    # Prefer Understat minutes if more complete
                    if col == "minutes" and unified.get("minutes"):
                        unified["minutes"] = max(unified["minutes"], us_val)
                    else:
                        unified[col] = us_val

        unified_records.append(unified)

    # Add Understat-only records (no FPL match)
    for key, us_rec in understat_data.items():
        if key not in fpl_data:
            understat_only += 1
            unified_records.append(
                {
                    "unified_player_id": key[0],
                    "match_id": key[1],
                    "season": season,
                    "gameweek": us_rec.get("gameweek"),
                    "minutes": us_rec.get("minutes"),
                    "goals_scored": us_rec.get("goals"),
                    "assists": us_rec.get("assists"),
                    "xg": us_rec.get("xg"),
                    "xa": us_rec.get("xa"),
                    "xg_chain": us_rec.get("xg_chain"),
                    "xg_buildup": us_rec.get("xg_buildup"),
                    "shots": us_rec.get("shots"),
                    "key_passes": us_rec.get("key_passes"),
                    "position": us_rec.get("position"),
                }
            )

    logger.info(
        f"    Merged: {merged_count} FPL+Understat, {understat_only} Understat-only, "
        f"{len(fpl_data) - merged_count} FPL-only"
    )

    # Upload in batches
    for i in range(0, len(unified_records), BATCH_SIZE):
        client.table("silver_unified_player_stats").upsert(
            unified_records[i : i + BATCH_SIZE]
        ).execute()

    logger.info(f"    Uploaded {len(unified_records)} unified player stats")
    return True
