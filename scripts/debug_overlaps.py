#!/usr/bin/env python3
"""Debug matching logic."""

from dotenv import load_dotenv
import os
from supabase import create_client

load_dotenv()
client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# Count by season
for season in ["2021-22", "2022-23"]:
    print(f"\n=== {season} ===")

    # FPL
    fpl_result = (
        client.table("silver_fixtures")
        .select("kickoff_time")
        .eq("season", season)
        .execute()
    )
    fpl_dates = set()
    for r in fpl_result.data:
        kt = r.get("kickoff_time", "")
        if kt:
            d = kt.split("T")[0]
            fpl_dates.add(d)
    print(f"FPL fixtures: {len(fpl_result.data)}, unique dates: {len(fpl_dates)}")

    # Vaastav
    vaastav_result = (
        client.table("bronze_vaastav_fixtures")
        .select("kickoff_time")
        .eq("season", season)
        .execute()
    )
    vaastav_dates = set()
    for r in vaastav_result.data:
        kt = r.get("kickoff_time", "")
        if kt:
            d = kt.split("T")[0]
            vaastav_dates.add(d)
    print(
        f"Vaastav fixtures: {len(vaastav_result.data)}, unique dates: {len(vaastav_dates)}"
    )

    # Understat
    us_result = (
        client.table("silver_understat_match_stats")
        .select("date")
        .eq("season", season)
        .execute()
    )
    us_dates = set()
    for r in us_result.data:
        d = str(r.get("date", ""))
        if d:
            us_dates.add(d)
    print(f"Understat matches: {len(us_result.data)}, unique dates: {len(us_dates)}")

    # Overlaps
    print(f"\nFPL ∩ Understat: {len(fpl_dates & us_dates)}")
    print(f"Vaastav ∩ Understat: {len(vaastav_dates & us_dates)}")
    print(f"FPL ∩ Vaastav: {len(fpl_dates & vaastav_dates)}")
    print(f"All three: {len(fpl_dates & vaastav_dates & us_dates)}")
