#!/usr/bin/env python3
"""Check match results."""

import os
from collections import Counter

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Total counts
result = client.table("match_mapping").select("season, match_source").execute()
by_season = Counter(r.get("season") for r in result.data)
by_source = Counter(r.get("match_source") for r in result.data)

print(f"Total matches: {len(result.data)}")
print(f"By season: {dict(by_season)}")
print(f"By source: {dict(by_source)}")

# Understat match rate
total_us = sum(1 for r in result.data if r.get("understat_game_id"))
result2 = (
    client.table("match_mapping")
    .select("understat_game_id, fpl_fixture_id, vaastav_fixture_id")
    .execute()
)
matched_us = sum(
    1
    for r in result2.data
    if r.get("understat_game_id")
    and (r.get("fpl_fixture_id") or r.get("vaastav_fixture_id"))
)

print(f"\nUnderstat: {total_us}")
print(f"Matched: {matched_us} ({matched_us * 100 / total_us:.1f}%)")
