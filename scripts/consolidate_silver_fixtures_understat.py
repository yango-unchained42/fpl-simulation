"""Consolidate fixtures and understat data to Silver tables."""

from __future__ import annotations

import logging
from typing import Any

from dotenv import load_dotenv
from supabase import create_client

from src.utils.data_cleaning import clean_and_flag_record

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Fixtures transformation
def get_fpl_fixtures(client: Any) -> list[dict[str, Any]]:
    """Fetch all records from bronze_fpl_fixtures."""
    result = client.table("bronze_fpl_fixtures").select("*").execute()
    return result.data


def get_vaastav_fixtures(client: Any) -> list[dict[str, Any]]:
    """Fetch all records from bronze_vaastav_fixtures."""
    result = client.table("bronze_vaastav_fixtures").select("*").execute()
    return result.data


def transform_fpl_fixture(record: dict[str, Any]) -> dict[str, Any]:
    """Transform FPL fixture to silver format."""
    # Keep only columns that exist in silver schema
    allowed = {
        "id",
        "event",
        "team_h",
        "team_a",
        "finished",
        "started",
        "team_h_score",
        "team_a_score",
        "kickoff_time",
        "team_h_difficulty",
        "team_a_difficulty",
        "pulse_id",
        "season",
        "code",
    }
    record = {k: v for k, v in record.items() if k in allowed}
    record.pop("updated_at", None)
    record["source"] = "fpl"
    return clean_and_flag_record(record, category="vaastav_gw")


def transform_vaastav_fixture(record: dict[str, Any]) -> dict[str, Any]:
    """Transform Vaastav fixture to silver format."""
    # Keep only columns that exist in silver schema
    allowed = {
        "id",
        "event",
        "kickoff_time",
        "team_h",
        "team_a",
        "team_h_score",
        "team_a_score",
        "finished",
        "started",
        "season",
    }
    record = {k: v for k, v in record.items() if k in allowed}
    record.pop("updated_at", None)
    record["source"] = "vaastav"
    # Convert to text (team names as strings)
    record["team_h"] = str(record.get("team_h"))
    record["team_a"] = str(record.get("team_a"))
    record["team_h_difficulty"] = None
    record["team_a_difficulty"] = None
    record["pulse_id"] = None
    record["code"] = None
    return clean_and_flag_record(record, category="vaastav_gw")


# Understat transformation
def get_all_understat_player_stats(client: Any) -> list[dict[str, Any]]:
    """Fetch all records from bronze_understat_player_stats using pagination."""
    all_records = []
    page_size = 1000
    offset = 0

    while True:
        result = (
            client.table("bronze_understat_player_stats")
            .select("*")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        if not result.data:
            break
        all_records.extend(result.data)
        if len(result.data) < page_size:
            break
        offset += page_size

    return all_records


def get_all_understat_match_stats(client: Any) -> list[dict[str, Any]]:
    """Fetch all records from bronze_understat_match_stats using pagination."""
    all_records = []
    page_size = 1000
    offset = 0

    while True:
        result = (
            client.table("bronze_understat_match_stats")
            .select("*")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        if not result.data:
            break
        all_records.extend(result.data)
        if len(result.data) < page_size:
            break
        offset += page_size

    return all_records


def transform_understat_player(record: dict[str, Any]) -> dict[str, Any]:
    """Transform Understat player stats."""
    # Keep only columns that exist in silver schema
    allowed = {
        "player_id",
        "gameweek",
        "game_id",
        "team_id",
        "position",
        "position_id",
        "minutes",
        "goals",
        "assists",
        "shots",
        "xg",
        "xa",
        "xg_chain",
        "xg_buildup",
        "key_passes",
        "own_goals",
        "yellow_cards",
        "red_cards",
        "season",
        "league_id",
        "season_id",
    }
    record = {k: v for k, v in record.items() if k in allowed}
    record.pop("updated_at", None)
    return clean_and_flag_record(record, category="vaastav_gw")


def transform_understat_match(
    record: dict[str, Any],
) -> dict[str, Any] | None:
    """Transform Understat match stats."""
    # Keep only columns that exist in silver schema
    allowed = {
        "game_id",
        "date",
        "season",
        "home_team_id",
        "away_team_id",
        "home_team",
        "away_team",
        "home_goals",
        "away_goals",
        "home_xg",
        "away_xg",
        "home_np_xg",
        "away_np_xg",
        "home_np_xg_difference",
        "away_np_xg_difference",
        "home_ppda",
        "away_ppda",
        "home_deep_completions",
        "away_deep_completions",
        "home_expected_points",
        "away_expected_points",
        "home_points",
        "away_points",
        "away_team_code",
        "home_team_code",
        "league_id",
        "season_id",
    }
    record = {k: v for k, v in record.items() if k in allowed}
    record.pop("updated_at", None)

    # Skip records with null date
    if not record.get("date"):
        return None

    return clean_and_flag_record(record, category="vaastav_gw")


def main() -> None:
    """Main entry point."""
    load_dotenv()

    import os

    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_KEY", "")
    client = create_client(url, key)

    # Process fixtures
    logger.info("Processing FPL fixtures...")
    fpl_fixtures = get_fpl_fixtures(client)
    fpl_transformed = [transform_fpl_fixture(r) for r in fpl_fixtures]
    logger.info(f"  {len(fpl_transformed)} FPL fixtures")

    logger.info("Processing Vaastav fixtures...")
    vaastav_fixtures = get_vaastav_fixtures(client)
    vaastav_transformed = [transform_vaastav_fixture(r) for r in vaastav_fixtures]
    logger.info(f"  {len(vaastav_transformed)} Vaastav fixtures")

    # Upload fixtures
    all_fixtures = fpl_transformed + vaastav_transformed
    logger.info(f"Upserting {len(all_fixtures)} fixtures...")
    chunk_size = 500
    for i in range(0, len(all_fixtures), chunk_size):
        chunk = all_fixtures[i : i + chunk_size]
        client.table("silver_fixtures").upsert(chunk).execute()
        logger.info(f"  Uploaded {i + len(chunk)}/{len(all_fixtures)}")

    # Process Understat
    logger.info("Processing Understat player stats...")
    understat_player = get_all_understat_player_stats(client)
    understat_player_transformed = [
        transform_understat_player(r) for r in understat_player
    ]
    logger.info(f"  {len(understat_player_transformed)} records")

    logger.info("Processing Understat match stats...")
    understat_match = get_all_understat_match_stats(client)
    understat_match_transformed = [
        r
        for r in (transform_understat_match(r) for r in understat_match)
        if r is not None
    ]
    logger.info(
        f"  {len(understat_match_transformed)} records (filtered out null dates)"
    )

    # Upload Understat
    logger.info("Upserting Understat player stats...")
    for i in range(0, len(understat_player_transformed), chunk_size):
        chunk = understat_player_transformed[i : i + chunk_size]
        client.table("silver_understat_player_stats").upsert(chunk).execute()
        logger.info(f"  Uploaded {i + len(chunk)}/{len(understat_player_transformed)}")

    logger.info("Upserting Understat match stats...")
    for i in range(0, len(understat_match_transformed), chunk_size):
        chunk = understat_match_transformed[i : i + chunk_size]
        client.table("silver_understat_match_stats").upsert(chunk).execute()
        logger.info(f"  Uploaded {i + len(chunk)}/{len(understat_match_transformed)}")

    logger.info("Done!")


if __name__ == "__main__":
    main()
