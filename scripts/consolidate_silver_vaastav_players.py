"""Consolidate bronze_vaastav_players to silver_vaastav_players.

Adds data quality flags and preserves all columns from Bronze source.
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

from dotenv import load_dotenv
from supabase import create_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_all_bronze_players(client: Any) -> list[dict[str, Any]]:
    """Fetch all records from bronze_vaastav_players with pagination."""
    all_records = []
    page_size = 1000
    offset = 0

    while True:
        result = (
            client.table("bronze_vaastav_players")
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
    """Add data quality flags to a record."""
    # Check for missing critical fields
    missing_fields = []
    critical_fields = ["id", "web_name", "season", "total_points"]

    for field in critical_fields:
        if record.get(field) is None:
            missing_fields.append(field)

    # Calculate quality score based on completeness
    total_fields = len(record)
    non_null_fields = sum(1 for v in record.values() if v is not None)
    quality_score = non_null_fields / total_fields if total_fields > 0 else 0.0

    # Add flags
    record["unified_player_id"] = str(uuid4())
    record["data_quality_score"] = round(quality_score, 3)
    record["is_incomplete"] = len(missing_fields) > 0
    record["missing_fields"] = missing_fields if missing_fields else []
    record.pop("updated_at", None)  # Remove old updated_at

    return record


def main() -> None:
    """Main entry point."""
    load_dotenv()

    import os

    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_KEY", "")
    client = create_client(url, key)

    # Fetch all bronze data
    logger.info("Fetching bronze_vaastav_players data...")
    bronze_records = get_all_bronze_players(client)
    logger.info(f"Found {len(bronze_records)} records in Bronze")

    # Add quality flags
    silver_records = []
    for record in bronze_records:
        silver_records.append(add_data_quality_flags(record))

    # Upload in chunks
    logger.info("Uploading to silver_vaastav_players...")
    chunk_size = 1000
    for i in range(0, len(silver_records), chunk_size):
        chunk = silver_records[i : i + chunk_size]
        client.table("silver_vaastav_players").insert(chunk).execute()
        logger.info(f"Uploaded {i + len(chunk)}/{len(silver_records)}")

    logger.info("Done!")


if __name__ == "__main__":
    main()
