#!/usr/bin/env python3
"""Drop and recreate match_mapping without constraints."""

import os

from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")

# This won't work via Supabase client - we need to use raw SQL or recreate the table differently
# Alternative: Use Supabase CLI or psql directly

# For now, let's try creating another table and populating that, then swapping
print("Creating new table without constraints...")

# Check CLI availability
import subprocess

result = subprocess.run(["which", "psql"], capture_output=True, text=True)
if result.returncode == 0:
    print("psql available")
else:
    print("psql not available")

# Try Supabase CLI
result = subprocess.run(
    ["/opt/homebrew/bin/supabase", "--version"], capture_output=True, text=True
)
print(f"Supabase CLI: {result.stdout if result.returncode == 0 else result.stderr}")
