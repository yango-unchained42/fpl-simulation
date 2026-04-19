# Ticket: BACKLOG-005 - Refactor Bronze Layer to Supabase Storage

## Description

Refactor the bronze layer to eliminate local file caching and use Supabase Storage for historical data. This will:
1. Remove local parquet file caching from daily pipeline
2. Store historical bronze data in Supabase Storage as parquet files
3. Load historical data from Storage on demand instead of DB
4. Fix cache staleness issues (GW32 was missed because local cache wasn't refreshed)

## Current Problems

- Local parquet files can become stale (cache TTL issues)
- CI/CD can't access local files
- Historical data sits in DB taking up space
- Every run loads full 100k+ rows from DB

## Target Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ BRONZE LAYER                                                │
├─────────────────────────────────────────────────────────────┤
│ Current Season (2025-26)                                    │
│   - FPL: Direct API → Supabase DB (real-time)              │
│   - Understat: Direct API → Supabase DB (daily)            │
│                                                             │
│ Historical (2021-22 → 2024-25)                             │
│   - Stored as Parquet files in Supabase Storage            │
│   - Downloaded on-demand for silver processing             │
│   - Never re-uploaded once archived                        │
└─────────────────────────────────────────────────────────────┘
```

## Implementation Steps

### Phase 1: Fix FPL Bronze (remove local caching)
- [ ] Modify `daily_bronze_update.py` to write FPL data directly to DB
- [ ] Remove local file writing in `ingest_fpl.py`
- [ ] Ensure every run fetches fresh data from API

### Phase 2: Historical Data to Storage
- [ ] Export historical bronze tables to Parquet
- [ ] Upload to Supabase Storage bucket `bronze/`
- [ ] Create loader to fetch from Storage when processing historical

### Phase 3: CI/CD Integration
- [ ] Update GitHub Actions to download from Storage
- [ ] Remove local file dependencies

## Migration Path

1. **Start with fresh**: Don't upload old data to Storage immediately
2. **Current season**: Still goes to DB
3. **At season end**: Archive current season to Storage, clear from DB
4. **Load**: Check Storage first, fallback to DB for current

## Acceptance Criteria
- [ ] No local parquet files created during daily run
- [ ] Historical data loads from Supabase Storage
- [ ] CI/CD pipeline works without local files
- [ ] No cache staleness issues
- [ ] Season transition is seamless (archive old, start new)

## Related
- Related to BACKLOG-004 (Incremental Loading) - both aim to reduce data reload
- Fixes: GW32 missing issue (April 2026)
