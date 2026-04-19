# Ticket: BACKLOG-004 - Incremental Loading for Daily Pipeline

## Description
Implement incremental/delta loading for daily Bronze→Silver pipeline to avoid reprocessing full datasets on each run. Currently processes ~57K shots every time, should only process new data (100-500 rows/day).

## Technical Approach

### Option 1: ID-based tracking
Track last processed ID in metadata table per table/season:
```sql
-- metadata table already exists, add columns:
-- last_id, last_gameweek, last_updated
```

### Option 2: Timestamp-based
Use created_at/updated_at timestamps to fetch only new records.

### Option 3: Gameweek-based (FPL)
Only load current season data, filter by gameweek.

## Implementation Priority
1. **silver_understat_shots** - Highest impact (57K → ~500/day)
2. **silver_fpl_player_stats** - Filter to current season only
3. **silver_understat_player_stats** - By gameweek
4. All other tables

## Acceptance Criteria
- [ ] Daily pipeline processes only new data (<1000 rows/day instead of ~60K)
- [ ] Metadata table tracks last processed state per table
- [ ] Backfill capability for missed data
- [ ] No duplicate records in Silver tables

## Impact
- Reduces Supabase API calls significantly
- Faster daily pipeline runtime
- Lower costs (less data transfer/processing)

## Agent
build

## Status
Pending

## Progress Log
