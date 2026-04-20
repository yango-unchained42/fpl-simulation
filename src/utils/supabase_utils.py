"""Supabase utility functions for pagination and data access.

Handles the PostgREST 1000 row limit by using pagination strategies.

Standard Usage Pattern:
------------------------
1. fetch_all_paginated() - Bulk load tables (handles 1000 row limit)
2. Build lookups in memory (dict or Polars)
3. Join/transform data
4. Batch update back to Supabase

Example - UUID Resolution:
------------------------
    from src.utils.supabase_utils import fetch_all_paginated

    # 1. Load mapping table
    data = fetch_all_paginated(client, "silver_team_mapping",
                            select_cols="season,fpl_team_id,unified_team_id")
    lookup = {(r["season"], r["fpl_team_id"]): r["unified_team_id"] for r in data}

    # 2. Load target table needing update
    stats = fetch_all_paginated(client, "silver_fpl_player_stats",
                             select_cols="season,fixture,match_id")

    # 3. Join in memory
    updates = []
    for r in stats:
        key = (r["season"], r["fixture"])
        if key in lookup and not r.get("match_id"):
            updates.append({"match_id": lookup[key], **r})

    # 4. Batch update
    for rec in updates:
        client.table("silver_fpl_player_stats").update(
            {"match_id": rec["match_id"]}
        ).eq("season", rec["season"]).eq("fixture", rec["fixture"]).execute()

See Also:
--------
- src/silver.uuid_resolver - UUID resolution utilities
- src/silver.table_ops - Standard table operations
"""

from __future__ import annotations

from collections.abc import Generator
from typing import Any

# Default batch size to avoid hitting PostgREST limit
DEFAULT_BATCH_SIZE = 1000


def fetch_all_paginated(
    client: Any,
    table: str,
    select_cols: str = "*",
    filters: dict[str, Any] | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    order_by: str | None = None,
    order_desc: bool = False,
) -> list[dict[str, Any]]:
    """Fetch all records from a table with automatic pagination.

    Handles Supabase PostgREST default 1000 row limit by using range-based
    pagination. This is more reliable than filter-based pagination.

    Args:
        client: Supabase client
        table: Table name
        select_cols: Columns to select (comma-separated)
        filters: Optional filters as {column: value} dict
        batch_size: Records per request (default 1000)
        order_by: Column to order by
        order_desc: Order descending

    Returns:
        List of all records
    """
    all_records: list[dict[str, Any]] = []
    offset = 0

    while True:
        query = client.table(table).select(select_cols)

        # Apply filters
        if filters:
            for col, val in filters.items():
                if val is not None:
                    query = query.eq(col, val)

        # Apply ordering
        if order_by:
            query = (
                query.order(order_by, desc=order_desc)
                if order_desc
                else query.order(order_by)
            )

        # Apply pagination
        result = query.range(offset, offset + batch_size - 1).execute()

        if not result.data:
            break

        all_records.extend(result.data)

        if len(result.data) < batch_size:
            break

        offset += batch_size

    return all_records


def fetch_all_by_filter(
    client: Any,
    table: str,
    select_cols: str = "*",
    filter_col: str | None = None,
    filter_val: Any = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> list[dict[str, Any]]:
    """Fetch records using filter-based pagination.

    Uses .eq() filter to get records per season/value to bypass the
    1000 row limit. More reliable than range() for full table scans.

    Args:
        client: Supabase client
        table: Table name
        select_cols: Columns to select
        filter_col: Column to filter on
        filter_val: Value to filter by
        batch_size: Max records to return

    Returns:
        List of matching records
    """
    if filter_col and filter_val is not None:
        result = (
            client.table(table).select(select_cols).eq(filter_col, filter_val).execute()
        )
        return result.data

    # No filter - fetch all
    return fetch_all_paginated(client, table, select_cols, batch_size=batch_size)


def iter_paginated(
    client: Any,
    table: str,
    select_cols: str = "*",
    filters: dict[str, Any] | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> Generator[list[dict[str, Any]], None, None]:
    """Yield records in batches for memory-efficient processing.

    Use this when you need to process records in chunks without
    loading everything into memory.

    Args:
        client: Supabase client
        table: Table name
        select_cols: Columns to select
        filters: Optional filters
        batch_size: Records per yield

    Yields:
        Batches of records
    """
    offset = 0

    while True:
        query = client.table(table).select(select_cols)

        if filters:
            for col, val in filters.items():
                if val is not None:
                    query = query.eq(col, val)

        result = query.range(offset, offset + batch_size - 1).execute()

        if not result.data:
            break

        yield result.data

        if len(result.data) < batch_size:
            break

        offset += batch_size


def fetch_seasonal_records(
    client: Any,
    table: str,
    select_cols: str = "*",
    season_col: str = "season",
    seasons: list[str] | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch all records grouped by season.

    Uses filter-based approach to bypass 1000 row limit.
    Returns dict of {season: [records]}.

    Args:
        client: Supabase client
        table: Table name
        select_cols: Columns to select
        season_col: Season column name
        seasons: List of seasons to fetch (None = all distinct)
        batch_size: Max per season

    Returns:
        Dict mapping season to records
    """
    # If no seasons specified, get distinct seasons first
    if not seasons:
        season_result = client.table(table).select(season_col).execute()
        seasons = list(
            set(r.get(season_col) for r in season_result.data if r.get(season_col))
        )

    result: dict[str, list[dict[str, Any]]] = {}

    for season in seasons:
        records = fetch_all_by_filter(
            client,
            table,
            select_cols,
            filter_col=season_col,
            filter_val=season,
            batch_size=batch_size,
        )
        result[season] = records

    return result


def upsert_batched(
    client: Any,
    table: str,
    records: list[dict[str, Any]],
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    """Insert/update records in batches.

    Args:
        client: Supabase client
        table: Table name
        records: Records to upsert
        batch_size: Records per batch

    Returns:
        Total records processed
    """
    total = 0

    for i in range(0, len(records), batch_size):
        chunk = records[i : i + batch_size]
        client.table(table).upsert(chunk).execute()
        total += len(chunk)

    return total


def count_table(
    client: Any,
    table: str,
    filters: dict[str, Any] | None = None,
) -> int:
    """Count records in a table with optional filters.

    Uses .count('exact') to get accurate counts.

    Args:
        client: Supabase client
        table: Table name
        filters: Optional filters

    Returns:
        Record count
    """
    query = client.table(table).select("*", count="exact")

    if filters:
        for col, val in filters.items():
            if val is not None:
                query = query.eq(col, val)

    result = query.execute()
    return result.count if result.count is not None else 0
