"""Consolidate bronze_vaastav_player_history_gw to silver_vaastav_gw.

Adds data quality flags and cleans data using shared utilities.
"""

from __future__ import annotations

import logging
from typing import Any

from dotenv import load_dotenv
from supabase import create_client

from src.utils.data_cleaning import clean_and_flag_record

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_all_bronze_gw(client: Any) -> list[dict[str, Any]]:
    """Fetch all records from bronze_vaastav_player_history_gw with pagination."""
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


def add_data_quality_flags(record: dict[str, Any]) -> dict[str, Any]:
    """Add data quality flags and clean data using shared utility."""
    # Remove updated_at before cleaning (not in DB schema)
    record.pop("updated_at", None)

    # Apply full cleaning pipeline
    return clean_and_flag_record(record, category="vaastav_gw")


def main() -> None:
    """Main entry point."""
    load_dotenv()

    import os

    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_KEY", "")
    client = create_client(url, key)

    logger.info("Fetching bronze_vaastav_player_history_gw data...")
    bronze_records = get_all_bronze_gw(client)
    logger.info(f"Found {len(bronze_records)} records in Bronze")

    # Add quality flags
    silver_records = []
    for record in bronze_records:
        silver_records.append(add_data_quality_flags(record))

    # Upload in chunks - use upsert to handle existing data
    logger.info("Upserting to silver_vaastav_gw...")
    chunk_size = 500
    for i in range(0, len(silver_records), chunk_size):
        chunk = silver_records[i : i + chunk_size]
        client.table("silver_vaastav_gw").upsert(chunk).execute()
        logger.info(f"Uploaded {i + len(chunk)}/{len(silver_records)}")

    logger.info("Done!")


if __name__ == "__main__":
    main()
