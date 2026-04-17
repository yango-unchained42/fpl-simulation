"""UUID resolution for silver layer - single function for all resolutions.

Standard pattern: bulk fetch + lookup + batch update
"""

from __future__ import annotations

from typing import Any

from src.utils.supabase_utils import fetch_all_paginated


def resolve_all_uuids(
    client: Any,
    target_table: str = "silver_fpl_player_stats",
) -> dict[str, int]:
    """Resolve all UUIDs in a table using mapping tables.

    Resolves:
    - unified_player_id: via player_mapping (fpl_id or vaastav_id)
    - match_id: via match_mapping (fpl_fixture_id or vaastav_fixture_id)
    - opponent_unified_team_id: CAN BE DERIVED at query time via
      match_id + was_home from silver_match_mapping

    Returns:
        Dict with counts of resolved IDs per type
    """
    print(f"  Resolving all UUIDs in {target_table}...")

    results = {"match_id": 0, "unified_player_id": 0}

    # === 1. Load all mappings once ===

    # Player mapping: (season, fpl_id) or (season, vaastav_id) -> unified_player_id
    player_map_data = fetch_all_paginated(
        client,
        "silver_player_mapping",
        select_cols="season,fpl_id,vaastav_id,unified_player_id",
    )
    player_lookup = {}
    for r in player_map_data:
        season = r.get("season")
        unified_id = r.get("unified_player_id")
        if season and unified_id:
            if r.get("fpl_id"):
                player_lookup[(season, r["fpl_id"])] = unified_id
            if r.get("vaastav_id"):
                player_lookup[(season, r["vaastav_id"])] = unified_id

    # Match mapping: (season, fixture) -> (match_id, home_team, away_team)
    match_map_data = fetch_all_paginated(
        client,
        "silver_match_mapping",
        select_cols="season,fpl_fixture_id,vaastav_fixture_id,match_id,home_unified_team_id,away_unified_team_id",
    )
    match_lookup = {}
    for r in match_map_data:
        season = r.get("season")
        match_id = r.get("match_id")
        if season and match_id:
            fix_id = r.get("fpl_fixture_id") or r.get("vaastav_fixture_id")
            if fix_id:
                match_lookup[(season, fix_id)] = {
                    "match_id": match_id,
                    "home_team": r.get("home_unified_team_id"),
                    "away_team": r.get("away_unified_team_id"),
                }

    print(f"    Lookups: {len(player_lookup)} players, {len(match_lookup)} matches")

    # === 2. Load target data ===
    stats_data = fetch_all_paginated(
        client,
        target_table,
        select_cols="season,gameweek,player_id,fixture,unified_player_id,match_id",
    )

    if not stats_data:
        print("    No data found")
        return results

    print(f"    Stats records: {len(stats_data)}")

    # === 3. Build unique updates per (season, fixture) ===
    fixture_updates = {}  # (season, fixture) -> {match_id, home_team, away_team}
    player_updates = {}  # (season, player_id) -> unified_player_id

    for r in stats_data:
        season = r.get("season")
        fixture = r.get("fixture")
        player_id = r.get("player_id")

        # Match resolution
        if season and fixture:
            if fixture not in fixture_updates:
                match_data = match_lookup.get((season, fixture))
                if match_data:
                    fixture_updates[(season, fixture)] = match_data

        # Player resolution
        if season and player_id:
            if (season, player_id) not in player_updates:
                unified = player_lookup.get((season, player_id))
                if unified:
                    player_updates[(season, player_id)] = unified

    print(f"    Fixture updates needed: {len(fixture_updates)}")
    print(f"    Player updates needed: {len(player_updates)}")

    # === 4. Batch updates - only include UUID fields in upsert ===

    # We need keys to identify records, then set UUIDs

    # Match updates: identify by (season, fixture)
    for (season, fixture), data in fixture_updates.items():
        client.table(target_table).update({"match_id": data["match_id"]}).eq(
            "season", season
        ).eq("fixture", fixture).execute()
    results["match_id"] = len(fixture_updates)

    # Player updates: identify by (season, player_id)
    for (season, player_id), unified_id in player_updates.items():
        client.table(target_table).update({"unified_player_id": unified_id}).eq(
            "season", season
        ).eq("player_id", player_id).execute()
    results["unified_player_id"] = len(player_updates)

    print(f"    Resolved: {results}")
    return results
