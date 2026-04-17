#!/usr/bin/env python3
"""Debug vaastav loading."""

import os

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Check vaastav fixtures
print("=== bronze_vaastav_fixtures ===")
result = (
    client.table("bronze_vaastav_fixtures").select("id, season, kickoff_time").execute()
)
print(f"Total: {len(result.data)}")

# Group by season
from collections import Counter

seasons = Counter(r.get("season") for r in result.data)
print(f"Seasons: {dict(seasons)}")

# Sample dates per season
for season in ["2021-22", "2022-23"]:
    dates = set()
    for r in result.data:
        if r.get("season") == season:
            kt = r.get("kickoff_time", "")
            if kt:
                dates.add(kt.split("T")[0])
    print(f"\n{season} dates ({len(dates)}):")
    print(f"  Sample: {sorted(dates)[:5]}")
