# Ticket: BACKLOG-001 - Refactor Daily Pipeline to Use DuckDB Bulk Operations

## Status: Pending

## Summary

Refactor `scripts/daily_silver_update.py` to use DuckDB for bulk read/write operations instead of paginated Supabase REST API calls. This will significantly improve performance from ~10 minutes to ~30 seconds per run.

## Problem

Current implementation uses paginated API calls:
- ~100+ API calls per table
- Each call fetches 1000 rows
- Many individual upsert operations

This is slow and inefficient, especially for GitHub Actions where time matters.

## Solution

Use DuckDB to connect directly to Supabase PostgreSQL and perform bulk operations:

1. Load data from Supabase using DuckDB (single bulk query)
2. Transform data locally (fast, Python/pandas/polars)
3. Write back to Supabase using bulk INSERT

Example:
```python
import duckdb

# Connect directly to Supabase
conn = duckdb.connect(os.getenv('SUPABASE_DB_URL'))

# Bulk read
df = conn.execute("SELECT * FROM bronze_fpl_gw").df()

# Transform locally
df['unified_player_id'] = df.apply(...)

# Bulk write
conn.execute("INSERT INTO silver_fpl_player_stats SELECT * FROM df")
```

## Requirements

- Add `SUPABASE_DB_URL` as GitHub secret (connection string with service role)
- Handle large datasets in chunks (10k rows per insert)
- Validate schema compatibility

## Benefits

- 20-30x faster pipeline execution
- More reliable (no pagination edge cases)
- Works well in GitHub Actions

## Risks

- Requires database credentials in environment
- Schema changes could break queries
- Memory usage for large datasets

## Dependencies

- BLOCKED by: All Sprint 11 silver layer tasks complete
- After this: Can set up automated GitHub Actions daily runs
