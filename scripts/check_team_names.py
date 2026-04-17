#!/usr/bin/env python3
"""Check if FPL fixtures have team names."""

from dotenv import load_dotenv
import os
from supabase import create_client

load_dotenv()
client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Check FPL fixtures columns
result = client.table("silver_fixtures").select("*").limit(1).execute()
if result.data:
    print("silver_fixtures columns:", list(result.data[0].keys()))

# Check bronze_vaastav_fixtures
result = client.table("bronze_vaastav_fixtures").select("*").limit(1).execute()
if result.data:
    print("\nbronze_vaastav_fixtures columns:", list(result.data[0].keys()))

# Check Understat match stats columns
result = client.table("silver_understat_match_stats").select("*").limit(1).execute()
if result.data:
    print("\nsilver_understat_match_stats columns:", list(result.data[0].keys()))
