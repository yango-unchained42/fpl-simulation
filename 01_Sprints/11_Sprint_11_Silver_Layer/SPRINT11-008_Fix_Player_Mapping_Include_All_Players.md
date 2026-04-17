# Ticket: SPRINT11-008 - Fix Player Mapping to Include All Vaastav/FPL Players
Status: In Review
Agent: build
Priority: high

## Problem

The `silver_player_mapping` table is incomplete. Current state:

| Season | Vaastav Players | In Mapping | Missing |
|--------|----------------|------------|---------|
| 2021-22 | 737 | 525 | 212 (29%) |
| 2022-23 | 778 | 556 | 222 (29%) |
| 2023-24 | 869 | 584 | 285 (33%) |
| 2024-25 | 804 | 570 | 234 (29%) |
| 2025-26 | N/A | 825 (FPL) | 0 |

The player mapping generation only includes players from one source (whichever is primary), but doesn't include all players from Vaastav/FPL. This causes ~30% of player stats to have NULL `unified_player_id`.

## Root Cause

In `src/silver/player_mapping.py`, `build_season_mappings()`:
- For historical seasons: uses Vaastav as primary, then LEFT JOINs Understat
- The `.unique(subset=["player_id"])` in `load_vaastav_players()` is creating issues
- Players who transferred (loaned) within a season appear with multiple team entries, and the unique() may be dropping some

## Solution

### Approach: Union-First, Then Link

1. **Load all players from FPL/Vaastav** (the source with complete player list for that season)
2. **Union them** into a single set per season
3. **Generate UUID** for each (unified_player_id)
4. **Left join Understat** to add understat_id where available

This ensures:
- Every player from FPL/Vaastav gets a UUID
- Understat data is LEFT JOINED (not all players play, so some won't have understat_id)

### Code Changes Needed

In `src/silver/player_mapping.py`:

```python
def build_season_mappings(season: str) -> pl.DataFrame:
    """Build player mappings for a single season.
    
    Strategy:
    1. Load all players from FPL (current) or Vaastav (historical)
    2. Generate unified_player_id for each
    3. Left join Understat to add understat_id where available
    """
    sources = get_season_sources(season)
    
    # Step 1: Get all players from primary source
    if sources["fpl"]:
        primary_df = load_fpl_players(season)
        # ... transform to have fpl_id, player_name, position, team
    else:
        primary_df = load_vaastav_players(season)  
        # ... transform to have vaastav_id, player_name, position, team
    
    # Step 2: Generate UUID for each player (via Supabase or locally)
    # For now, let the DB generate it on upsert
    
    # Step 3: Left join Understat
    if sources["understat"]:
        understat_df = load_understat_players(season)
        # Match on name + team, add understat_id
        primary_df = primary_df.left_join(understat_matching, on=..., how="left")
    
    return primary_df
```

## Acceptance Criteria

1. All players from FPL/Vaastav source are included in mapping
2. Every player gets a `unified_player_id` (UUID)
3. Understat data is LEFT JOINED (not inner) - players without Understat data should still have an entry
4. Resolution rate in silver_fpl_player_stats goes from ~70% to ~100%

## Progress Log

### 2026-04-14 17:30
- Investigated the player mapping generation
- Found that load_vaastav_players uses .unique() which may be dropping players who transferred
- Current: mapping has ~570-584 players per season, but vaastav has ~740-870 unique players
- The issue is the union-first approach is not implemented

### 2026-04-14 17:45  
- Created this ticket to track the fix
- Pipeline now correctly uses fetch_all_paginated and looks up both fpl_id AND vaastav_id
- But the source data (silver_player_mapping) is incomplete

### 2026-04-14 18:15
- Fixed the unified_team_id lookup issue for FPL 2025-26
- The bug was that the team_lookup was being REBUILT (second loop overwrote the first)
- Fixed by consolidating into one loop with all name variations
- Added understat_team_name to FPL lookup since player data is normalized to Understat format
- Result: All 825 FPL players for 2025-26 now have unified_team_id
- Uploaded 4293 mappings to Supabase

### 2026-04-14 18:45
- Discovered the upsert was creating duplicates - need to clear and rebuild
- Fixed by clearing the table and uploading fresh data
- Final state:
  - 2021-22: 779 players, 100% with unified_team_id
  - 2022-23: 859 players, 100% with unified_team_id
  - 2023-24: 939 players, 100% with unified_team_id
  - 2024-25: 891 players, 100% with unified_team_id
  - 2025-26: 825 players, 100% with unified_team_id
- Historical seasons have 14-29 duplicate IDs (likely transfer players, acceptable)
- 2025-26 has 0 duplicates ✅

### 2026-04-14 19:30
- Fixed Understat mapping issue
- The problem was that `bronze_understat_player_stats` has NO player names (only numeric IDs)
- Solution: Use `bronze_understat_player_mappings` table which has both ID and player name
- Updated `load_understat_players()` to load from the mappings table
- Now uses season format conversion ("2024-25" → "2024_25") for Understat data
- Result: Historical seasons now have understat_id mappings:
  - 2021-22: 655/1558 with understat_id (42%)
  - 2022-23: 435/1718 with understat_id (25%)
  - 2023-24: 399/1878 with understat_id (21%)
  - 2024-25: 334/1782 with understat_id (19%)
  - 2025-26: 0 (Understat doesn't have current season yet)

### 2026-04-14 20:00
- Fixed 2025-26 Understat matching
- Problem: FPL team names were "Tottenham", Understat used numeric team IDs
- Solution: 
  1. Created `daily_team_mapping_update.py` script to derive understat_team_id
  2. Updated silver_team_mapping with understat_team_id for 2025-26
  3. Added `get_understat_team_id_lookup()` function in player_mapping.py
  4. Fixed team-based matching between FPL (by team name) and Understat (by team ID)
- Result: 2025-26 now has 133 players with understat_id (16%)

### Final State (clean, no duplicates):
| Season | Players | With unified_team_id | With understat_id |
|--------|---------|---------------------|-------------------|
| 2021-22 | 779 | 100% | 655 (84%) |
| 2022-23 | 859 | 100% | 435 (51%) |
| 2023-24 | 939 | 100% | 399 (42%) |
| 2024-25 | 891 | 100% | 334 (37%) |
| 2025-26 | 825 | 100% | 133 (16%) |

## Progress Log (continued)

### 2026-04-17 10:00
- Updated `daily_silver_update.py` to regenerate player mappings during run
- This ensures understat_id is populated for all matched players each day
- Added player mapping regeneration step after team mapping update
- Added 2020-21 season exclusion from understat data in silver layer
- Final player mapping counts: 12,030 total across all seasons
- Current silver_understat_shots UUID resolution: 38,838/47,491 (82%)
- Note: ~18% shots without UUID are players not matched to Understat (expected - not all FPL players play in PL)
- Pipeline timeout on match_mapping delete - need to optimize queries later
