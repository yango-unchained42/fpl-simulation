#!/usr/bin/env python3
"""Check available data sources and seasons."""

from dotenv import load_dotenv
import os
from supabase import create_client

load_dotenv()
client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Check what tables exist
tables_to_check = [
    "silver_fixtures",
    "silver_vaastav_fixtures",
    "silver_understat_match_stats",
    "match_mapping",
    "bronze_fixtures",
    "bronze_vaastav_fixtures",
]

print("=== Available Fixture/Match Tables ===\n")

for table in tables_to_check:
    try:
        # Get count and seasons
        result = client.table(table).select("season").limit(1000).execute()
        if result.data:
            # Get unique seasons
            unique_seasons = set(
                r.get("season") for r in result.data if r.get("season")
            )
            print(f"{table}:")
            print(f"  Sample rows: {len(result.data)}")
            print(f"  Seasons: {sorted(unique_seasons)}")
        else:
            print(f"{table}: (empty)")
    except Exception as e:
        print(f"{table}: Error - {e}")

print("\n=== match_mapping breakdown ===\n")
result = client.table("match_mapping").select("match_source, season").execute()
by_source = {}
for r in result.data:
    src = r.get("match_source", "unknown")
    if src not in by_source:
        by_source[src] = set()
    by_source[src].add(r.get("season", ""))

for src, seasons in by_source.items():
    print(f"{src}: {sorted(seasons)}")
