"""Consolidate bronze data to unified silver_gw table.

Combines FPL and Vaastav GW data into a single table.
"""

from __future__ import annotations

import logging
from typing import Any

from dotenv import load_dotenv
from supabase import create_client

from src.utils.data_cleaning import clean_and_flag_record

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_fpl_gw_data(client: Any) -> list[dict[str, Any]]:
    """Fetch all records from bronze_fpl_gw."""
    all_records = []
    page_size = 1000
    offset = 0

    while True:
        result = (
            client.table("bronze_fpl_gw")
            .select("*")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        if not result.data:
            break
        all_records.extend(result.data)
        offset += page_size
        if len(result.data) < page_size:
            break

    return all_records


def get_vaastav_gw_data(client: Any) -> list[dict[str, Any]]:
    """Fetch all records from bronze_vaastav_player_history_gw."""
    all_records = []
    page_size = 1000
    offset = 0

    while True:
        result = (
            client.table("bronze_vaastav_player_history_gw")
            .select("*")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        if not result.data:
            break
        all_records.extend(result.data)
        offset += page_size
        if len(result.data) < page_size:
            break

    return all_records


def transform_fpl_gw(record: dict[str, Any]) -> dict[str, Any]:
    """Transform FPL GW record to unified schema."""
    record.pop("updated_at", None)

    # Map to unified schema - FPL uses 'element' for player ID
    record["player_id"] = record.pop("element", None)
    record["gameweek"] = record.pop("round", None)
    record["source"] = "fpl"

    # Remove columns not in unified schema
    record.pop("modified", None)
    record.pop("round", None)

    # Apply cleaning
    return clean_and_flag_record(record, category="gw")


def transform_vaastav_gw(record: dict[str, Any]) -> dict[str, Any]:
    """Transform Vaastav GW record to unified schema."""
    record.pop("updated_at", None)

    # Vaastav has extra columns not in unified schema - remove them
    record.pop("name", None)
    record.pop("position", None)
    record.pop("team", None)
    record.pop("xp", None)
    record.pop("unified_player_id", None)

    # gameweek already in correct format
    record["source"] = "vaastav"

    # Vaastav doesn't have these FPL-only fields - set to None
    record["clearances_blocks_interceptions"] = None
    record["recoveries"] = None
    record["tackles"] = None
    record["defensive_contribution"] = None
    record["transfers_balance"] = None

    # Apply cleaning
    return clean_and_flag_record(record, category="vaastav_gw")


def main() -> None:
    """Main entry point."""
    load_dotenv()

    import os

    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_KEY", "")
    client = create_client(url, key)

    # Process FPL data
    logger.info("Fetching FPL GW data...")
    fpl_records = get_fpl_gw_data(client)
    logger.info(f"Found {len(fpl_records)} FPL GW records")

    fpl_unified = []
    for record in fpl_records:
        fpl_unified.append(transform_fpl_gw(record))

    logger.info(f"Transformed {len(fpl_unified)} FPL records")

    # Filter out records without player_id
    fpl_unified = [r for r in fpl_unified if r.get("player_id") is not None]
    logger.info(f"After filtering null player_id: {len(fpl_unified)} FPL records")

    # Process Vaastav data
    logger.info("Fetching Vaastav GW data...")
    vaastav_records = get_vaastav_gw_data(client)
    logger.info(f"Found {len(vaastav_records)} Vaastav GW records")

    vaastav_unified = []
    for record in vaastav_records:
        vaastav_unified.append(transform_vaastav_gw(record))

    logger.info(f"Transformed {len(vaastav_unified)} Vaastav records")

    # Combine
    all_records = fpl_unified + vaastav_unified
    logger.info(f"Total records to upload: {len(all_records)}")

    # Upload in chunks
    logger.info("Upserting to silver_gw...")
    chunk_size = 500
    for i in range(0, len(all_records), chunk_size):
        chunk = all_records[i : i + chunk_size]
        client.table("silver_gw").upsert(chunk).execute()
        logger.info(f"Uploaded {i + len(chunk)}/{len(all_records)}")

    logger.info("Done!")


if __name__ == "__main__":
    main()
