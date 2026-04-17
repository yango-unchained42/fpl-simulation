"""Silver layer utilities for FPL data processing.

Standard Pattern for Supabase operations:
-----------------------------------------
1. Bulk fetch from Supabase (fetch_all_paginated)
2. Build lookup dict in memory
3. Join/transform data
4. Batch update to Supabase

Modules:
--------
- uuid_resolver: Resolve raw IDs to unified UUIDs via mapping tables
- table_ops: Standard table operations (load, save, update)

Usage:
------
    from src.silver.uuid_resolver import resolve_fpl_player_stats

    result = resolve_fpl_player_stats(client, "silver_fpl_player_stats")

See Also:
--------
- src.utils.supabase_utils: Bulk fetch utilities (fetch_all_paginated)
"""

from src.silver.table_ops import load_table, save_table, update_table_from_lookup
from src.silver.uuid_resolver import resolve_all_uuids

__all__ = [
    "resolve_all_uuids",
    "load_table",
    "save_table",
    "update_table_from_lookup",
]
