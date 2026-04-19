#!/usr/bin/env python3
"""Test insert with all IDs."""

import os

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Try inserting one complete record
record = [
    {
        "match_id": "a0000000-0000-0000-0000-000000000001",
        "season": "2021-22",
        "match_date": "2021-08-22",
        "fpl_fixture_id": 1,
        "vaastav_fixture_id": None,
        "understat_game_id": 12345,
        "home_score": 2,
        "away_score": 0,
        "match_source": "both",
    }
]

try:
    result = client.table("match_mapping").upsert(record).execute()
    print("Success:", result.data)
except Exception as e:
    print("Error:", e)
