"""Safe Supabase operations with deduplication.

Provides utilities to prevent duplicates when writing to Supabase tables.
The core problem: Supabase upsert uses the PRIMARY KEY for conflict detection,
but if the PK is a generated UUID, every insert creates a new row even if the
business key (season, fpl_id) already exists.

Solution: Before upsert, deduplicate in-memory by business key, keeping the
row with the highest data_quality_score (or latest updated_at).
"""

from __future__ import annotations

import logging
from typing import Any

from src.config import BATCH_SIZE
from src.utils.supabase_utils import fetch_all_paginated

logger = logging.getLogger(__name__)


def deduplicate_by_key(
    records: list[dict],
    key_columns: list[str],
    score_column: str | None = None,
) -> list[dict]:
    """Deduplicate records by business key, keeping the best one.

    Args:
        records: List of records to deduplicate.
        key_columns: Columns that form the business key (e.g., ["season", "fpl_id"]).
        score_column: Column to use for selecting the best record (higher = better).
            If None, keeps the last occurrence.

    Returns:
        Deduplicated list of records.
    """
    seen: dict[tuple, dict] = {}

    for rec in records:
        key = tuple(rec.get(col) for col in key_columns)

        # Skip records with null keys
        if any(v is None for v in key):
            continue

        if key not in seen:
            seen[key] = rec
        elif score_column:
            existing_score = seen[key].get(score_column) or 0
            new_score = rec.get(score_column) or 0
            if new_score > existing_score:
                seen[key] = rec
        else:
            # Keep the last one
            seen[key] = rec

    deduped = list(seen.values())
    removed = len(records) - len(deduped)
    if removed > 0:
        logger.info(
            f"    Deduped {len(records)} → {len(deduped)} records (removed {removed})"
        )

    return deduped


def load_existing_keys(
    client: Any,
    table: str,
    key_columns: list[str],
    season: str | None = None,
) -> set[tuple]:
    """Load existing business keys from a table.

    Args:
        client: Supabase client.
        table: Table name.
        key_columns: Columns that form the business key.
        season: Optional season filter.

    Returns:
        Set of existing key tuples.
    """
    select_cols = ",".join(key_columns)
    filters = {"season": season} if season else None

    existing = fetch_all_paginated(
        client, table, select_cols=select_cols, filters=filters
    )
    keys = set()
    for rec in existing:
        key = tuple(rec.get(col) for col in key_columns)
        if all(v is not None for v in key):
            keys.add(key)
    return keys


def safe_upsert(
    client: Any,
    table: str,
    records: list[dict],
    business_key: list[str],
    season: str | None = None,
    score_column: str | None = "data_quality_score",
    skip_existing: bool = False,
) -> int:
    """Safely upsert records with deduplication.

    Steps:
    1. Deduplicate input records by business key
    2. Optionally skip records that already exist in the table
    3. Upsert in batches

    Args:
        client: Supabase client.
        table: Target table name.
        records: Records to upsert.
        business_key: Columns that form the business key.
        season: Season for existing key lookup.
        score_column: Column for selecting best duplicate (None = keep last).
        skip_existing: If True, skip records that already exist in the table.

    Returns:
        Number of records written.
    """
    if not records:
        return 0

    # Step 1: Deduplicate input
    records = deduplicate_by_key(records, business_key, score_column)

    # Step 2: Optionally skip existing
    if skip_existing:
        existing_keys = load_existing_keys(client, table, business_key, season)
        before = len(records)
        records = [
            r
            for r in records
            if tuple(r.get(c) for c in business_key) not in existing_keys
        ]
        skipped = before - len(records)
        if skipped > 0:
            logger.info(f"    Skipped {skipped} existing records in {table}")

    # Step 3: Upsert in batches
    written = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        try:
            client.table(table).upsert(batch).execute()
            written += len(batch)
        except Exception as e:
            logger.error(f"    Batch {i // BATCH_SIZE} failed: {e}")

    logger.info(f"    Upserted {written} records to {table}")
    return written


def clean_records_for_upload(
    records: list[dict],
    exclude_columns: list[str] | None = None,
) -> list[dict]:
    """Clean records before upload by removing server-managed columns.

    Args:
        records: Records to clean.
        exclude_columns: Columns to remove (default: created_at, updated_at, id).

    Returns:
        Cleaned records.
    """
    if exclude_columns is None:
        exclude_columns = ["created_at", "updated_at", "id"]

    cleaned = []
    for rec in records:
        clean = {k: v for k, v in rec.items() if k not in exclude_columns}
        # Remove None values so DB defaults are used
        clean = {k: v for k, v in clean.items() if v is not None}
        cleaned.append(clean)

    return cleaned
