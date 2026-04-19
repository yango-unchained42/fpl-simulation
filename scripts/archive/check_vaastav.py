#!/usr/bin/env python3
"""Check bronze_vaastav_fixtures structure."""

import os

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Check vaastav - just get first 3 to see columns
result = client.table("bronze_vaastav_fixtures").select("*").limit(3).execute()
print("bronze_vaastav_fixtures:")
if result.data:
    print("Columns:", result.data[0].keys())
    print("\nSample:")
    for r in result.data[:3]:
        print(
            f"  id={r.get('id')}, season={r.get('season')}, date={r.get('date', r.get('kickoff_time', 'N/A'))}"
        )

# Get total count
count = client.table("bronze_vaastav_fixtures").select("id", count="exact").execute()
print(f"\nTotal rows: {count.count}")

# Get unique seasons
seasons_result = client.table("bronze_vaastav_fixtures").select("season").execute()
seasons = set(r.get("season") for r in seasons_result.data if r.get("season"))
print(f"Seasons: {sorted(seasons)}")
