#!/usr/bin/env python3
"""Debug date格式 mismatches."""

from dotenv import load_dotenv
import os
from supabase import create_client
from collections import defaultdict

load_dotenv()
client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Get FPL fixture dates for 2021-22
fpl_dates = set()
for rec in (
    client.table("silver_fixtures")
    .select("kickoff_time")
    .eq("season", "2021-22")
    .execute()
    .data
):
    kickoff = rec.get("kickoff_time", "")
    if kickoff:
        date = kickoff.split("T")[0]
        fpl_dates.add(date)

# Get Understat dates for 2021-22
us_dates = set()
for rec in (
    client.table("silver_understat_match_stats")
    .select("date")
    .eq("season", "2021-22")
    .execute()
    .data
):
    date = str(rec.get("date", ""))
    if date:
        us_dates.add(date)

print("=== 2021-22 Season ===")
print(f"\nFPL fixture dates ({len(fpl_dates)}):")
print(f"  Sample: {sorted(fpl_dates)[:5]}")

print(f"\nUnderstat dates ({len(us_dates)}):")
print(f"  Sample: {sorted(us_dates)[:5]}")

# Find overlap
overlap = fpl_dates & us_dates
print(f"\nExact overlap ({len(overlap)}):")
print(f"  {sorted(overlap)[:10]}")

# Check format
print(f"\n=== Date format check ===")
sample_fpl = list(fpl_dates)[0] if fpl_dates else ""
sample_us = list(us_dates)[0] if us_dates else ""
print(f"FPL sample: '{sample_fpl}' (type: {type(sample_fpl)})")
print(f"Understat sample: '{sample_us}' (type: {type(sample_us)})")

# Check all season overlap
print("\n=== Full season overlap ===")
for season in ["2020-21", "2021-22", "2022-23"]:
    fpl_d = set()
    us_d = set()

    for rec in (
        client.table("silver_fixtures")
        .select("kickoff_time")
        .eq("season", season)
        .execute()
        .data
    ):
        kt = rec.get("kickoff_time", "")
        if kt:
            fpl_d.add(kt.split("T")[0])

    for rec in (
        client.table("silver_understat_match_stats")
        .select("date")
        .eq("season", season)
        .execute()
        .data
    ):
        d = str(rec.get("date", ""))
        if d:
            us_d.add(d)

    overlap = fpl_d & us_d
    print(f"{season}: FPL={len(fpl_d)}, US={len(us_d)}, overlap={len(overlap)}")
