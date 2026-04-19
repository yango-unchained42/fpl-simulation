#!/usr/bin/env python3
"""Debug date formats."""

import os

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Get FPL date format
result = (
    client.table("silver_fixtures")
    .select("kickoff_time")
    .eq("season", "2021-22")
    .limit(1)
    .execute()
)
print("FPL kickoff_time:", result.data[0].get("kickoff_time") if result.data else "N/A")

# Get Understat date format
result = (
    client.table("silver_understat_match_stats")
    .select("date")
    .eq("season", "2021-22")
    .limit(1)
    .execute()
)
print("Understat date:", result.data[0].get("date") if result.data else "N/A")

# Same match example
result = (
    client.table("silver_fixtures")
    .select("kickoff_time, team_h, team_a")
    .eq("season", "2021-22")
    .eq("team_h", "Arsenal")
    .limit(1)
    .execute()
)
print("\nFPL Arsenal home:", result.data[0] if result.data else "N/A")

result = (
    client.table("silver_understat_match_stats")
    .select("date, home_team")
    .eq("season", "2021-22")
    .eq("home_team", "Arsenal")
    .limit(1)
    .execute()
)
print("Understat Arsenal home:", result.data[0] if result.data else "N/A")
