#!/usr/bin/env python3
"""Check team mapping tables."""

import os

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Check team mapping table
result = client.table("silver_team_mapping").select("*").limit(10).execute()
print(f"silver_team_mapping rows: {len(result.data)}")
if result.data:
    print("Sample:", result.data[:3])
else:
    print("Empty!")

# What tables exist for teams?
print("\nChecking tables with 'team' in name:")
for t in [
    "silver_team_mapping",
    "bronze_team_mapping",
    "team_mapping",
    "teams",
    "fpl_teams",
]:
    try:
        result = client.table(t).select("id").limit(1).execute()
        count = client.table(t).select("id", count="exact").execute()
        print(f"  {t}: exists ({count.count} rows)")
    except Exception as e:
        print(f"  {t}: NOT FOUND - {e}")
