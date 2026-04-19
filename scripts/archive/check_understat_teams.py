#!/usr/bin/env python3
"""Check Understat team identifiers."""

import os

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Get Understat teams for 2021-22
result = (
    client.table("silver_understat_match_stats")
    .select("home_team, home_team_code, away_team, away_team_code")
    .eq("season", "2021-22")
    .limit(10)
    .execute()
)
print("Understat teams (sample):")
for r in result.data:
    print(
        f"  {r['home_team']} ({r['home_team_code']}) vs {r['away_team']} ({r['away_team_code']})"
    )

# Get FPL team IDs for same season
result = (
    client.table("silver_fixtures")
    .select("team_h, team_a")
    .eq("season", "2021-22")
    .limit(10)
    .execute()
)
print("\nFPL team IDs (sample):")
for r in result.data:
    print(f"  {r['team_h']} vs {r['team_a']}")
