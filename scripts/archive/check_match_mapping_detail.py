#!/usr/bin/env python3
"""Check what's actually in match_mapping"""

import os
<<<<<<< HEAD
from collections import Counter
=======
from collections import Counter  # noqa: E402

from dotenv import load_dotenv
from supabase import create_client
>>>>>>> origin/main

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Get all with understat_game_id
result = (
    client.table("match_mapping")
    .select(
        "season, match_source, fpl_fixture_id, vaastav_fixture_id, understat_game_id"
    )
    .execute()
)

# Group by season and source
by_season = Counter()
matched_by_season = Counter()

for r in result.data:
    season = r.get("season", "")
    src = r.get("match_source", "")
    by_season[(season, src)] += 1

    if r.get("understat_game_id"):
        matched_by_season[season] += 1

print("=== match_mapping breakdown by season and source ===")
for (season, src), count in sorted(by_season.items()):
    print(f"{season} - {src}: {count}")

print("\n=== With understat_game_id by season ===")
for season, count in sorted(matched_by_season.items()):
    print(f"{season}: {count}")

# What are the dates for 2021-22?
print("\n=== 2021-22 dates in match_mapping with US ID ===")
result = (
    client.table("match_mapping")
    .select("match_date")
    .eq("season", "2021-22")
    .gt("understat_game_id", 0)
    .execute()
)
dates = [r.get("match_date") for r in result.data]
from collections import Counter  # noqa: E402

date_counts = Counter(dates)
print(f"Total: {len(dates)}, unique: {len(date_counts)}")
print(f"Sample dates: {sorted(dates)[:5]}")
