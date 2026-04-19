#!/usr/bin/env python3
"""Check match_mapping coverage by season."""

import os
from collections import defaultdict

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Get all match_mapping data
result = (
    client.table("match_mapping")
    .select(
        "season, match_source, fpl_fixture_id, vaastav_fixture_id, understat_game_id"
    )
    .execute()
)

# Group by season and match_source
by_season_source = defaultdict(
    lambda: {"fpl": 0, "vaastav": 0, "understat": 0, "both": 0}
)
by_season = defaultdict(
    lambda: {"all": 0, "matched": 0, "fpl_id": 0, "vaastav_id": 0, "understat_id": 0}
)

for r in result.data:
    season = r.get("season", "")
    src = r.get("match_source", "")
    by_season_source[season][src] = by_season_source[season].get(src, 0) + 1
    by_season[season]["all"] += 1

    if r.get("fpl_fixture_id"):
        by_season[season]["fpl_id"] += 1
    if r.get("vaastav_fixture_id"):
        by_season[season]["vaastav_id"] += 1
    if r.get("understat_game_id"):
        by_season[season]["understat_id"] += 1

    # "matched" = has any two source IDs
    id_count = sum(
        [
            1
            for x in [
                r.get("fpl_fixture_id"),
                r.get("vaastav_fixture_id"),
                r.get("understat_game_id"),
            ]
            if x
        ]
    )
    if id_count >= 2:
        by_season[season]["matched"] += 1

print("=== Coverage by Season ===\n")
print(
    f"{'Season':<12} {'Total':<8} {'FPL':<8} {'Vaastav':<10} {'Understat':<10} {'Matched':<10}"
)
print("-" * 60)

for season in sorted(by_season.keys()):
    s = by_season[season]
    print(
        f"{season:<12} {s['all']:<8} {s['fpl_id']:<8} {s['vaastav_id']:<10} {s['understat_id']:<10} {s['matched']:<10}"
    )

# Calculate overall match rate for seasons with multiple sources
print("\n=== Match Rate Analysis ===")
total_matched = sum(s["matched"] for s in by_season.values())
total_games = sum(s["all"] for s in by_season.values())
overall_rate = (total_matched / total_games * 100) if total_games > 0 else 0
print(f"Overall: {total_matched}/{total_games} matched = {overall_rate:.1f}%")

# Seasons with Understat data
seasons_with_us = [s for s, d in by_season.items() if d["understat_id"] > 0]
if seasons_with_us:
    games_in_matched_seasons = sum(by_season[s]["all"] for s in seasons_with_us)
    matched_in_matched_seasons = sum(by_season[s]["matched"] for s in seasons_with_us)
    rate = (
        (matched_in_matched_seasons / games_in_matched_seasons * 100)
        if games_in_matched_seasons > 0
        else 0
    )
    print(
        f"In seasons with Understat: {matched_in_matched_seasons}/{games_in_matched_seasons} = {rate:.1f}%"
    )
