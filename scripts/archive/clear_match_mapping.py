#!/usr/bin/env python3
"""Drop and recreate match_mapping table with new schema."""

import os

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# This won't work directly - we can't run DDL via postgREST
# But we need to delete existing rows to clear duplicates
print("Deleting all existing match_mapping rows...")

# Get all IDs first
result = client.table("match_mapping").select("match_id").execute()
ids = [r.get("match_id") for r in result.data if r.get("match_id")]

print(f"Found {len(ids)} existing rows - deleting in batches...")

# Delete all rows using a filter that matches everything
for i in range(0, len(ids), 1000):
    batch = ids[i : i + 1000]
    # Can't filter by list, so we need another approach
    pass

# Simpler approach - delete where match_id is not null (all rows)
client.table("match_mapping").delete().isnot_("match_id", None).execute()
print("Deleted all rows")
