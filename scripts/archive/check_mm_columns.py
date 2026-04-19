#!/usr/bin/env python3
"""Check table columns."""

import os

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Check columns
result = client.table("match_mapping").select("*").limit(1).execute()
print("match_mapping columns:")
if result.data:
    for k, v in result.data[0].items():
        print(f"  {k}: {v}")
