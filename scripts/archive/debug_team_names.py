#!/usr/bin/env python3
"""Debug team name matching."""

import os

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Get unique FPL team names
result = (
    client.table("silver_fixtures").select("team_h").eq("season", "2021-22").execute()
)
fpl_teams = sorted(set(r.get("team_h") for r in result.data if r.get("team_h")))
print("FPL teams:", fpl_teams[:15])

# Get unique Understat team names
result = (
    client.table("silver_understat_match_stats")
    .select("home_team")
    .eq("season", "2021-22")
    .execute()
)
us_teams = sorted(set(r.get("home_team") for r in result.data if r.get("home_team")))
print("\nUnderstat teams:", us_teams[:15])

# Check specific match
print("\nSpecific check: Arsenal vs Chelsea")
result = (
    client.table("silver_fixtures")
    .select("team_h, team_a, kickoff_time")
    .eq("season", "2021-22")
    .eq("team_h", "Arsenal")
    .execute()
)
print(
    "FPL Arsenal home:",
    [(r.get("team_a"), r.get("kickoff_time").split("T")[0]) for r in result.data[:3]],
)

result = (
    client.table("silver_understat_match_stats")
    .select("home_team, away_team, date")
    .eq("season", "2021-22")
    .eq("home_team", "Arsenal")
    .execute()
)
print(
    "US Arsenal home:", [(r.get("away_team"), r.get("date")) for r in result.data[:3]]
)
