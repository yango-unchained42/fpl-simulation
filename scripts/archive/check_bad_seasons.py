#!/usr/bin/env python3
"""Find where bad season data is coming from."""

import os

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Check 2025-26 season data in each source
for table in [
    "silver_fixtures",
    "bronze_vaastav_fixtures",
    "silver_understat_match_stats",
]:
    try:
        result = (
            client.table(table).select("season, id").eq("season", "2025-26").execute()
        )
        if result.data:
            print(f"\n{table}:")
            print(f"  Count: {len(result.data)}")
            print(f"  Sample IDs: {[r.get('id') for r in result.data[:5]]}")
    except Exception as e:
        print(f"\n{table}: Error - {e}")

# Also check for any invalid-looking seasons
print("\n=== Total by season in silver_fixtures ===")
result = client.table("silver_fixtures").select("season").execute()
from collections import Counter

seasons = Counter(r.get("season") for r in result.data if r.get("season"))
for s, c in sorted(seasons.items()):
    print(f"  {s}: {c}")
