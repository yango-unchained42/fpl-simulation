#!/usr/bin/env python3
"""Check existing team mappings."""

from dotenv import load_dotenv
import os
from supabase import create_client

load_dotenv()
client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Get team mappings
result = (
    client.table("silver_team_mapping")
    .select("vaastav_team_name, understat_team_name, fpl_team_name")
    .limit(20)
    .execute()
)
print("Existing team mappings:")
for r in result.data:
    print(
        f"  vaastav: {r.get('vaastav_team_name')} -> understat: {r.get('understat_team_name')}"
    )
