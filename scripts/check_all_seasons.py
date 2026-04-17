#!/usr/bin/env python3
"""Check exact season counts in fixtures."""

from dotenv import load_dotenv
import os
from supabase import create_client
from collections import Counter

load_dotenv()
client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Check all seasons in silver_fixtures
result = client.table("silver_fixtures").select("season").execute()
seasons = Counter(r.get("season") for r in result.data)
print("silver_fixtures seasons:")
for s, c in sorted(seasons.items()):
    print(f"  {s}: {c}")
print(f"  Total: {sum(seasons.values())}")

# Check all seasons in bronze_vaastav_fixtures
result = client.table("bronze_vaastav_fixtures").select("season").execute()
seasons = Counter(r.get("season") for r in result.data)
print("\nbronze_vaastav_fixtures seasons:")
for s, c in sorted(seasons.items()):
    print(f"  {s}: {c}")
print(f"  Total: {sum(seasons.values())}")

# Check all seasons in silver_understat_match_stats
result = client.table("silver_understat_match_stats").select("season").execute()
seasons = Counter(r.get("season") for r in result.data)
print("\nsilver_understat_match_stats seasons:")
for s, c in sorted(seasons.items()):
    print(f"  {s}: {c}")
print(f"  Total: {sum(seasons.values())}")

# Check match_mapping
result = client.table("match_mapping").select("season").execute()
seasons = Counter(r.get("season") for r in result.data)
print("\nmatch_mapping seasons:")
for s, c in sorted(seasons.items()):
    print(f"  {s}: {c}")
print(f"  Total: {sum(seasons.values())}")
