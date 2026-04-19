#!/usr/bin/env python3
"""Debug why Understat isn't matching."""

import os

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Get some FPL team names
print("FPL team names from 2021-22:")
result = (
    client.table("silver_fixtures").select("team_h").eq("season", "2021-22").execute()
)
fpl_teams = set(r.get("team_h") for r in result.data if r.get("team_h"))
for t in sorted(fpl_teams)[:10]:
    print(f"  {t}")

# Get Understat team names
print("\nUnderstat team names from 2021-22:")
result = (
    client.table("silver_understat_match_stats")
    .select("home_team")
    .eq("season", "2021-22")
    .execute()
)
us_teams = set(r.get("home_team") for r in result.data if r.get("home_team"))
for t in sorted(us_teams)[:10]:
    print(f"  {t}")

# Find overlap
common = fpl_teams & us_teams
print(f"\nCommon: {len(common)} teams")
print(f"  {sorted(common)[:10]}")
