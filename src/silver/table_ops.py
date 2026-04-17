"""Standard table operations for Supabase silver layer.

Provides reusable functions for:
- Bulk loading tables with pagination
- Bulk saving/updating tables
- UUID resolution via lookups
"""

from __future__ import annotations

from typing import Any

import polars as pl

from src.utils.supabase_utils import fetch_all_paginated


def load_table(
    client: Any,
    table: str,
    select_cols: str = "*",
) -> pl.DataFrame:
    """Load a table into Polars DataFrame.

    Args:
        client: Supabase client
        table: Table name
        select_cols: Comma-separated column names

    Returns:
        Polars DataFrame
    """
    data = fetch_all_paginated(client, table, select_cols=select_cols)
    return pl.DataFrame(data) if data else pl.DataFrame()


def save_table(
    client: Any,
    table: str,
    df: pl.DataFrame,
    on_conflict: str | None = None,
) -> int:
    """Save DataFrame to Supabase table.

    Args:
        client: Supabase client
        table: Table name
        df: Polars DataFrame
        on_conflict: Optional column for upsert conflict handling

    Returns:
        Number of rows saved
    """
    if df.is_empty():
        return 0

    records = df.to_dicts()
    for rec in records:
        client.table(table).upsert(rec).execute()

    return len(records)


def update_table_from_lookup(
    client: Any,
    table: str,
    lookup: dict[tuple[str, Any], str],
    join_keys: list[str],
    update_col: str,
) -> int:
    """Update a table by joining with a lookup dict.

    Args:
        client: Supabase client
        table: Table to update
        lookup: Dict of (key1, key2) -> value to join on
        join_keys: Column names to use as join keys [season, fixture]
        update_col: Column to update with lookup value

    Returns:
        Number of rows updated
    """
    # Load table data
    cols = ",".join(join_keys + [update_col])
    data = fetch_all_paginated(client, table, select_cols=cols)

    if not data:
        return 0

    # Build updates
    updates = []
    for r in data:
        key = tuple(r.get(k) for k in join_keys)
        new_val = lookup.get(key)

        if new_val and r.get(update_col) is None:
            updates.append({**r, update_col: new_val})

    if not updates:
        return 0

    # Batch update
    for rec in updates:
        client.table(table).update({update_col: rec[update_col]}).execute()

    return len(updates)


def resolve_uuids(
    client: Any,
    target_table: str,
    source_table: str,
    source_cols: str,
    target_join_cols: list[str],
    target_update_col: str,
) -> int:
    """Resolve UUIDs by joining tables in memory.

    Args:
        client: Supabase client
        target_table: Table to update
        source_table: Table with UUID mappings
        source_cols: Columns to fetch from source (e.g., "season,fpl_id,unified_id")
        target_join_cols: Columns to join on [season, id]
        target_update_col: Column to update

    Returns:
        Number of rows updated
    """
    # 1. Load source mappings
    source_data = fetch_all_paginated(client, source_table, select_cols=source_cols)

    # Build lookup dict
    lookup = {}
    for r in source_data:
        key = tuple(r.get(c) for c in target_join_cols)
        val = r.get(target_update_col)
        if key and val:
            lookup[key] = val

    if not lookup:
        return 0

    # 2. Use generic update function
    return update_table_from_lookup(
        client, target_table, lookup, target_join_cols, target_update_col
    )
