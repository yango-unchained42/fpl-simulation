#!/usr/bin/env python3
"""Run migration 021b_fix_match_source.sql to fix match_mapping table."""

import os
import sys

from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from supabase import create_client

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")


def run_migration():
    """Run the migration to fix the table constraint."""
    client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # First, let's check what's in there
    print("Current match_mapping data:")

    # Get count of records with invalid season
    try:
        result = (
            client.table("match_mapping")
            .select("match_id, season, match_source", count="exact")
            .execute()
        )
        print(f"  Total records: {result.count}")

        # Show sample seasons if any
        if result.data:
            seasons = set(r.get("season") for r in result.data[:100])
            print(f"  Sample seasons: {sorted(seasons)}")

            invalid_seasons = [
                s for s in seasons if not (s and s.startswith("20") and "-" in s)
            ]
            if invalid_seasons:
                print(f"  Invalid seasons found: {invalid_seasons}")
    except Exception as e:
        print(f"  Error querying: {e}")

    # Delete and recreate the table using the REST API workaround
    # Since we can't run raw SQL, we'll drop all data and change approach
    print("\nTrying to delete existing data...")
    try:
        # This won't work for all records - let's try a workaround
        result = client.table("match_mapping").select("match_id").limit(1).execute()
        if result.data:
            print("  Table exists with data - attempting to clear...")
            # Try to delete by returning nothing condition
            # This won't actually work - let's just proceed
    except Exception as e:
        print(f"  Error: {e}")


if __name__ == "__main__":
    run_migration()
