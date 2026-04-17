#!/usr/bin/env python3
"""Populate match_mapping using silver_team_mapping for ID resolution.

Uses proper joins to silver_team_mapping to get unified_team_id instead of name matching.
- FPL fixtures: join on fpl_team_name (normalized from team ID)
- Vaastav fixtures: join on vaastav_team_name
- Understat: join on understat_team_name

This creates the unified fixture keys that ALL other silver layer scripts should use.
"""

from __future__ import annotations

import logging
import os
import uuid
from typing import Any

from dotenv import load_dotenv
from supabase import create_client

from src.utils.supabase_utils import fetch_all_by_filter

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BATCH_SIZE = 500


def get_supabase() -> Any:
    return create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))


def is_valid_season(season: str | None) -> bool:
    if not season:
        return False
    parts = str(season).split("-")
    if len(parts) != 2:
        return False
    try:
        start_year = int(parts[0])
        end_year = int(parts[1])
        if start_year < 2016 or start_year > 2026:
            return False
        return end_year == (start_year + 1) % 100
    except ValueError:
        return False


def load_team_mapping_lookup(client: Any) -> dict[str, str]:
    """Load silver_team_mapping once, build lookup by name type."""
    lookup: dict[str, str] = {}

    result = (
        client.table("silver_team_mapping")
        .select(
            "season, fpl_team_name, vaastav_team_name, understat_team_name, unified_team_id"
        )
        .execute()
    )

    for rec in result.data:
        season = rec.get("season")
        unified_id = rec.get("unified_team_id")

        # Add FPL team name lookup
        if rec.get("fpl_team_name"):
            lookup[f"fpl:{season}:{rec['fpl_team_name']}"] = unified_id

        # Add Vaastav team name lookup
        if rec.get("vaastav_team_name"):
            lookup[f"vaastav:{season}:{rec['vaastav_team_name']}"] = unified_id

        # Add Understat team name lookup
        if rec.get("understat_team_name"):
            lookup[f"understat:{season}:{rec['understat_team_name']}"] = unified_id

    logger.info(f"  Loaded {len(lookup)} team name → unified_team_id mappings")
    return lookup


def load_fpl_with_team_mapping(
    client: Any, team_lookup: dict[str, str | None]
) -> list[dict[str, Any]]:
    """Load FPL fixtures with unified_team_id via lookup."""
    teams = {
        t["id"]: t["name"]
        for t in fetch_all_by_filter(
            client,
            "bronze_fpl_teams",
            select_cols="id,name",
            filter_col="season",
            filter_val="2025-26",
        )
    }

    fixtures = []
    for rec in fetch_all_by_filter(client, "bronze_fpl_fixtures"):
        season = rec.get("season")
        if not is_valid_season(season):
            continue

        home_name = teams.get(rec.get("team_h"))
        away_name = teams.get(rec.get("team_a"))

        if not home_name or not away_name:
            continue

        # Try FPL lookup first (current season), then Vaastav (historical)
        home_id = team_lookup.get(f"fpl:{season}:{home_name}")
        away_id = team_lookup.get(f"fpl:{season}:{away_name}")

        if not home_id:
            home_id = team_lookup.get(f"vaastav:{season}:{home_name}")
        if not away_id:
            away_id = team_lookup.get(f"vaastav:{season}:{away_name}")

        if home_id and away_id:
            fixtures.append(
                {
                    "fpl_fixture_id": rec.get("id"),
                    "season": season,
                    "kickoff_time": rec.get("kickoff_time"),
                    "home_unified_team_id": home_id,
                    "away_unified_team_id": away_id,
                    "home_score": rec.get("team_h_score"),
                    "away_score": rec.get("team_a_score"),
                }
            )

    return fixtures


def load_vaastav_with_team_mapping(
    client: Any, team_lookup: dict[str, str | None]
) -> list[dict[str, Any]]:
    """Load Vaastav fixtures with unified_team_id via lookup."""
    fixtures = []

    for season in ["2021-22", "2022-23", "2023-24", "2024-25"]:
        for rec in fetch_all_by_filter(
            client, "bronze_vaastav_fixtures", filter_col="season", filter_val=season
        ):
            season = rec.get("season")
            if not season or not is_valid_season(season):
                continue

            home_name = rec.get("team_h")
            away_name = rec.get("team_a")

            if not home_name or not away_name:
                continue

            home_id = team_lookup.get(f"vaastav:{season}:{home_name}")
            away_id = team_lookup.get(f"vaastav:{season}:{away_name}")

            if home_id and away_id:
                fixtures.append(
                    {
                        "vaastav_fixture_id": rec.get("id"),
                        "season": season,
                        "kickoff_time": rec.get("kickoff_time"),
                        "home_unified_team_id": home_id,
                        "away_unified_team_id": away_id,
                    }
                )

    return fixtures


def load_understat_with_team_mapping(
    client: Any, team_lookup: dict[str, str | None]
) -> list[dict[str, Any]]:
    """Load Understat matches with unified_team_id via lookup."""
    matches = []

    for data_season in [
        "2020-21",
        "2021-22",
        "2022-23",
        "2023-24",
        "2024-25",
        "2025-26",
    ]:
        for rec in fetch_all_by_filter(
            client,
            "bronze_understat_match_stats",
            filter_col="season",
            filter_val=data_season,
        ):
            season_val = rec.get("season")
            if not season_val or not is_valid_season(season_val):
                continue

            home_name = rec.get("home_team")
            away_name = rec.get("away_team")

            if not home_name or not away_name:
                continue

            home_id = team_lookup.get(f"understat:{season_val}:{home_name}")
            away_id = team_lookup.get(f"understat:{season_val}:{away_name}")

            if home_id and away_id:
                matches.append(
                    {
                        "understat_game_id": rec.get("game_id"),
                        "season": season_val,
                        "date": rec.get("date"),
                        "home_unified_team_id": home_id,
                        "away_unified_team_id": away_id,
                        "home_score": rec.get("home_goals"),
                        "away_score": rec.get("away_goals"),
                    }
                )

    return matches


def populate_match_mapping(client: Any) -> bool:
    """Populate match_mapping using silver_team_mapping for ID resolution."""
    logger.info("Populating match_mapping...")

    # Clear existing data
    for season in ["2021-22", "2022-23", "2023-24", "2024-25", "2025-26"]:
        client.table("silver_match_mapping").delete().eq("season", season).execute()
    logger.info("  Cleared existing data")

    # Load team mapping lookup ONCE
    team_lookup = load_team_mapping_lookup(client)

    # Load from each source with unified_team_id
    fpl_fixtures = load_fpl_with_team_mapping(client, team_lookup)
    logger.info(f"  Loaded {len(fpl_fixtures)} FPL fixtures")

    vaastav_fixtures = load_vaastav_with_team_mapping(client, team_lookup)
    logger.info(f"  Loaded {len(vaastav_fixtures)} Vaastav fixtures")

    understat_matches = load_understat_with_team_mapping(client, team_lookup)
    logger.info(f"  Loaded {len(understat_matches)} Understat matches")

    # Merge by (season, home_unified_team_id, away_unified_team_id)
    matches: dict[str, dict[str, Any]] = {}

    for f in fpl_fixtures:
        key = (f["season"], f["home_unified_team_id"], f["away_unified_team_id"])
        if key not in matches:
            matches[key] = {
                "season": f["season"],
                "home_unified_team_id": f["home_unified_team_id"],
                "away_unified_team_id": f["away_unified_team_id"],
            }
        if f.get("fpl_fixture_id"):
            matches[key]["fpl_fixture_id"] = f["fpl_fixture_id"]
        if f.get("kickoff_time"):
            matches[key]["match_date"] = str(f["kickoff_time"]).split("T")[0][:10]
        if f.get("home_score") is not None and "home_score" not in matches[key]:
            matches[key]["home_score"] = f["home_score"]
            matches[key]["away_score"] = f["away_score"]

    for v in vaastav_fixtures:
        key = (v["season"], v["home_unified_team_id"], v["away_unified_team_id"])
        if key not in matches:
            matches[key] = {
                "season": v["season"],
                "home_unified_team_id": v["home_unified_team_id"],
                "away_unified_team_id": v["away_unified_team_id"],
            }
        if v.get("vaastav_fixture_id"):
            matches[key]["vaastav_fixture_id"] = v["vaastav_fixture_id"]
        if v.get("kickoff_time") and "match_date" not in matches[key]:
            matches[key]["match_date"] = str(v["kickoff_time"]).split("T")[0][:10]
        if v.get("home_score") is not None and "home_score" not in matches[key]:
            matches[key]["home_score"] = v["home_score"]
            matches[key]["away_score"] = v["away_score"]

    for m in understat_matches:
        key = (m["season"], m["home_unified_team_id"], m["away_unified_team_id"])
        if key not in matches:
            matches[key] = {
                "season": m["season"],
                "home_unified_team_id": m["home_unified_team_id"],
                "away_unified_team_id": m["away_unified_team_id"],
            }
        if m.get("understat_game_id"):
            matches[key]["understat_game_id"] = m["understat_game_id"]
        if m.get("date") and "match_date" not in matches[key]:
            matches[key]["match_date"] = str(m["date"]).split("T")[0][:10]
        if m.get("home_score") is not None and "home_score" not in matches[key]:
            matches[key]["home_score"] = m["home_score"]
            matches[key]["away_score"] = m["away_score"]

    # Build records
    records = []
    stats = {"fpl": 0, "vaastav": 0, "understat": 0}

    for key, data in matches.items():
        record = {
            "match_id": str(uuid.uuid4()),
            "season": data["season"],
            "match_date": data.get("match_date", ""),
            "home_unified_team_id": data.get("home_unified_team_id"),
            "away_unified_team_id": data.get("away_unified_team_id"),
        }

        if data.get("fpl_fixture_id"):
            record["fpl_fixture_id"] = data["fpl_fixture_id"]
            stats["fpl"] += 1

        if data.get("vaastav_fixture_id"):
            record["vaastav_fixture_id"] = data["vaastav_fixture_id"]
            stats["vaastav"] += 1

        if data.get("understat_game_id"):
            record["understat_game_id"] = data["understat_game_id"]
            stats["understat"] += 1

        if data.get("home_score") is not None:
            record["home_score"] = data["home_score"]
            record["away_score"] = data["away_score"]

        # Skip records without match_date
        if not record.get("match_date"):
            continue

        if record.get("understat_game_id"):
            record["match_source"] = "understat"
        elif record.get("vaastav_fixture_id"):
            record["match_source"] = "vaastav"
        elif record.get("fpl_fixture_id"):
            record["match_source"] = "fpl"
        else:
            continue

        records.append(record)

    logger.info(f"  Total unique matches: {len(records)}")
    logger.info(
        f"    FPL: {stats['fpl']}, Vaastav: {stats['vaastav']}, Understat: {stats['understat']}"
    )

    if not records:
        logger.warning("  No records to upload")
        return False

    # Upload
    for i in range(0, len(records), BATCH_SIZE):
        chunk = records[i : i + BATCH_SIZE]
        try:
            client.table("silver_match_mapping").upsert(chunk).execute()
        except Exception as e:
            logger.error(f"    Batch error: {e}")

    logger.info(f"  ✓ Uploaded {len(records)} matches")
    return True


if __name__ == "__main__":
    populate_match_mapping(get_supabase())
