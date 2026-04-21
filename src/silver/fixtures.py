"""Silver layer — Fixtures consolidation.

Transforms Bronze fixtures into Silver fixtures with UUID resolution.
"""

from __future__ import annotations

import logging
from typing import Any

from src.config import BATCH_SIZE, CURRENT_SEASON
from src.utils.safe_upsert import truncate_table
from src.utils.supabase_utils import fetch_all_paginated

logger = logging.getLogger(__name__)


def _load_team_lookup(client: Any, season: str) -> dict[tuple[str, int], str]:
    """Load (source, source_team_id) → unified_team_id mapping."""
    lookup: dict[tuple[str, int], str] = {}
    for r in fetch_all_paginated(
        client,
        "silver_team_mapping",
        select_cols="season,fpl_team_id,unified_team_id",
        filters={"season": season},
    ):
        if r.get("fpl_team_id") and r.get("unified_team_id"):
            lookup[(season, int(r["fpl_team_id"]))] = r["unified_team_id"]
    return lookup


def update_fixtures(client: Any, season: str = CURRENT_SEASON) -> bool:
    """Update silver_fixtures from bronze_fpl_fixtures with UUID resolution."""
    logger.info("  Updating silver fixtures...")

    team_lookup = _load_team_lookup(client, season)

    # Load match mapping
    match_lookup: dict[tuple[str, int], str] = {}
    for r in fetch_all_paginated(
        client,
        "silver_match_mapping",
        select_cols="season,fpl_fixture_id,match_id",
        filters={"season": season},
    ):
        if r.get("fpl_fixture_id") and r.get("match_id"):
            match_lookup[(season, int(r["fpl_fixture_id"]))] = r["match_id"]

    truncate_table(client, "silver_fixtures")

    # Fetch bronze fixtures
    all_fixtures = []
    offset = 0
    while True:
        result = (
            client.table("bronze_fpl_fixtures")
            .select("*")
            .eq("season", season)
            .range(offset, offset + 999)
            .execute()
        )
        if not result.data:
            break
        all_fixtures.extend(result.data)
        if len(result.data) < 1000:
            break
        offset += 1000

    if not all_fixtures:
        logger.info("    No fixtures data")
        return False

    transformed = []
    for rec in all_fixtures:
        fixture_id = rec.get("id")
        home_fpl = rec.get("team_h")
        away_fpl = rec.get("team_a")
        gw = rec.get("event")

        match_id = match_lookup.get((season, fixture_id)) if fixture_id else None

        filtered = {
            "id": fixture_id,
            "match_id": match_id,
            "season": season,
            "event": gw,
            "home_unified_team_id": (
                team_lookup.get((season, home_fpl)) if home_fpl else None
            ),
            "away_unified_team_id": (
                team_lookup.get((season, away_fpl)) if away_fpl else None
            ),
            "kickoff_time": rec.get("kickoff_time"),
            "team_h_score": rec.get("team_h_score"),
            "team_a_score": rec.get("team_a_score"),
            "team_h_difficulty": rec.get("team_h_difficulty"),
            "team_a_difficulty": rec.get("team_a_difficulty"),
            "pulse_id": rec.get("pulse_id"),
            "code": rec.get("code"),
            "finished": rec.get("finished", False),
            "source": "fpl",
            "started": rec.get("started", False),
        }

        # Remove None values (let DB handle defaults for nullable columns)
        filtered = {k: v for k, v in filtered.items() if v is not None}

        if match_id:
            transformed.append(filtered)

    for i in range(0, len(transformed), BATCH_SIZE):
        client.table("silver_fixtures").upsert(
            transformed[i : i + BATCH_SIZE]
        ).execute()

    logger.info(f"    Updated {len(transformed)} fixtures")

    # Aggregate defensive stats from player stats
    _update_fixture_defensive_stats(client, season)

    return True


def _update_fixture_defensive_stats(client: Any, season: str) -> None:
    """Aggregate per-team defensive stats from player stats into fixtures."""
    logger.info("    Aggregating fixture defensive stats...")

    # Fetch all player stats for this season
    player_stats = fetch_all_paginated(
        client,
        "silver_fpl_player_stats",
        select_cols="match_id,was_home,tackles,clearances_blocks_interceptions,recoveries,defensive_contribution,saves",
        filters={"season": season},
    )

    if not player_stats:
        logger.info("    No player stats to aggregate")
        return

    # Aggregate by (match_id, was_home)
    from collections import defaultdict

    agg: dict[tuple, dict] = defaultdict(
        lambda: {
            "tackles": 0,
            "cbi": 0,
            "recoveries": 0,
            "def_con": 0,
            "saves": 0,
        }
    )
    for rec in player_stats:
        key = (rec["match_id"], rec["was_home"])
        agg[key]["tackles"] += rec.get("tackles") or 0
        agg[key]["cbi"] += rec.get("clearances_blocks_interceptions") or 0
        agg[key]["recoveries"] += rec.get("recoveries") or 0
        agg[key]["def_con"] += rec.get("defensive_contribution") or 0
        agg[key]["saves"] += rec.get("saves") or 0

    # Update fixtures with aggregated stats
    updated = 0
    for (match_id, was_home), stats in agg.items():
        # was_home=True → team_h stats, was_home=False → team_a stats
        prefix = "team_h" if was_home else "team_a"
        update_data = {
            f"{prefix}_tackles": stats["tackles"],
            f"{prefix}_clearances_blocks_interceptions": stats["cbi"],
            f"{prefix}_recoveries": stats["recoveries"],
            f"{prefix}_defensive_contribution": stats["def_con"],
            f"{prefix}_saves": stats["saves"],
        }

        try:
            client.table("silver_fixtures").update(update_data).eq(
                "match_id", match_id
            ).execute()
            updated += 1
        except Exception as e:
            logger.warning(f"    Failed to update fixture {match_id}: {e}")

    logger.info(f"    Updated defensive stats for {updated} fixture sides")
