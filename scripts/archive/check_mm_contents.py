#!/usr/bin/env python3
"""Check what's in match_mapping."""

import os
from collections import Counter

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

result = client.table("match_mapping").select("season, match_source").execute()
print(f"Total rows: {len(result.data)}")

# Group by season
by_season = Counter(r.get("season") for r in result.data)
print(f"\nSeasons: {dict(by_season)}")

# For 2021-22 check if anything matches
result2 = (
    client.table("match_mapping")
    .select("match_date, fpl_fixture_id, understat_game_id")
    .eq("season", "2021-22")
    .execute()
)
print(f"\n2021-22 rows: {len(result2.data)}")
if result2.data:
    print(f"Sample: {result2.data[:5]}")
