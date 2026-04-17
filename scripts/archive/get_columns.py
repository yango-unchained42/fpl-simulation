#!/usr/bin/env python3
"""Get existing columns from DB."""

from dotenv import load_dotenv
import os
from supabase import create_client

load_dotenv()
client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Get one row to see what columns exist
result = client.table("match_mapping").select("*").limit(1).execute()
print("Existing columns in DB:")
if result.data:
    for k in sorted(result.data[0].keys()):
        print(f"  {k}")
