"""Supabase database client and operations.

Handles connection to Supabase (hosted Postgres) and provides
CRUD operations for all pipeline tables.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any

import polars as pl

if TYPE_CHECKING:
    from supabase import Client  # type: ignore[attr-defined]

logger = logging.getLogger(__name__)


def get_supabase_client(
    url: str | None = None, key: str | None = None
) -> "Client | None":
    """Create and return a Supabase client.

    Args:
        url: Supabase project URL. Falls back to st.secrets or env vars.
        key: Supabase anon key. Falls back to st.secrets or env vars.

    Returns:
        Supabase Client instance, or None if dependencies missing.
    """
    try:
        from supabase import (
            create_client,  # type: ignore[attr-defined]  # noqa: PLC0415
        )
    except ImportError:
        logger.warning("supabase package not installed")
        return None

    if url is None or key is None:
        # First, try loading from .env file for local development
        from dotenv import load_dotenv

        load_dotenv()

        # Check environment variables (these take precedence)
        url = url or os.getenv("SUPABASE_URL")
        key = key or os.getenv("SUPABASE_KEY")

    if not url or not key:
        logger.warning("Supabase credentials not found")
        return None

    return create_client(url, key)


def write_to_supabase(
    table: str,
    df: pl.DataFrame,
    client: "Client | None" = None,
    upsert: bool = False,
) -> bool:
    """Write a Polars DataFrame to a Supabase table.

    Args:
        table: Target table name.
        df: DataFrame to write.
        client: Supabase client. Created if not provided.
        upsert: Whether to upsert on conflict.

    Returns:
        True if successful, False otherwise.
    """
    if client is None:
        client = get_supabase_client()
    if client is None:
        logger.error("Cannot write to Supabase: no client available")
        return False

    records = df.to_dicts()
    try:
        if upsert:
            client.table(table).upsert(records).execute()
        else:
            client.table(table).insert(records).execute()
        logger.info("Wrote %d rows to %s", len(records), table)
        return True
    except Exception as e:
        logger.error("Failed to write to %s: %s", table, e)
        return False


def read_from_supabase(
    table: str,
    columns: list[str] | None = None,
    filters: list[tuple[str, str, Any]] | None = None,
    client: "Client | None" = None,
) -> pl.DataFrame:
    """Read data from a Supabase table into a Polars DataFrame.

    Args:
        table: Source table name.
        columns: Optional list of columns to select.
        filters: Optional list of (column, operator, value) tuples.
        client: Supabase client. Created if not provided.

    Returns:
        Polars DataFrame with query results.
    """
    if client is None:
        client = get_supabase_client()
    if client is None:
        logger.error("Cannot read from Supabase: no client available")
        return pl.DataFrame()

    query = client.table(table).select(",".join(columns) if columns else "*")

    if filters:
        for col, op, val in filters:
            if op == "eq":
                query = query.eq(col, val)
            elif op == "gte":
                query = query.gte(col, val)
            elif op == "lte":
                query = query.lte(col, val)

    result = query.execute()
    if not result.data:
        return pl.DataFrame()

    return pl.DataFrame(result.data)
