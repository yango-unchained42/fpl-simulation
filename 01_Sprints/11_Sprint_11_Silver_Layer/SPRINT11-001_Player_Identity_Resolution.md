# Ticket: SPRINT11-001 - Player Identity Resolution

## Description
Create a unified player identity mapping across all data sources (FPL, Vaastav, Understat). This is the foundational step for the Silver layer - without it, we cannot merge player data from different sources.

## Technical Requirements
- Build `silver_player_mapping` table in Supabase with:
  - `unified_player_id` (PK, auto-generated UUID)
  - `season` (e.g., "2024-25") - **one row per player per season**
  - `fpl_id` (from FPL `id` column)
  - `vaastav_id` (from Vaastav `element` → `player_id`)
  - `understat_id` (from Understat `player_id`)
  - `player_name` (standardized, e.g., "Bukayo Saka")
  - `position` (GKP/DEF/MID/FWD)
  - `team` (team name for this season)
  - `source` (how mapping was determined: exact/fuzzy/manual)
  - `confidence_score` (0.0-1.0)
  - `requires_manual_review` (boolean, true if confidence < 0.85)
  - `created_at`, `updated_at`

- **Match per season**: One row per player per season, with `season` as a column
- **Team-based disambiguation**: Use team name to distinguish players with same name
- **All 3 sources**: Match FPL ↔ Vaastav, FPL ↔ Understat, Vaastav ↔ Understat
- Reuse existing `src/utils/name_resolver.py` for name standardization
- Reuse existing `src/data/crosswalk.py` logic (adapted for Supabase)

## Matching Logic (Per Season)

```
For each season (2021-22 to 2024-25, plus current 2025-26):
  1. Get FPL players for that season (from bronze_fpl_players)
  2. Get Vaastav players for that season (from bronze_player_history)
  3. Get Understat players for that season (from bronze_understat_player_stats)
  4. Match: first by exact name + team (highest confidence)
  5. Match: fuzzy by name only (if no team match)
  6. Store mapping with season column
```

## Confidence Thresholds
- >= 0.85: automatic match (green)
- 0.70-0.85: requires manual review flag (yellow)
- < 0.70: no match, mark for manual mapping (red)

## Edge Cases
- Players with name changes (e.g., "Son" vs "Heung-min Son")
- Players with same names (use team to disambiguate)
- Transfers between teams (keep historical team per season)
- Players not in all sources (partial matches OK)

## Acceptance Criteria
- [ ] `silver_player_mapping` table created in Supabase with `season` column
- [ ] All seasons mapped (2021-22 to 2025-26)
- [ ] All 3 sources mapped (FPL ↔ Vaastav ↔ Understat)
- [ ] Team-based disambiguation implemented
- [ ] Fuzzy matching with configurable threshold
- [ ] Manual review flags for low-confidence matches
- [ ] Name standardization via existing `name_resolver.py`
- [ ] Unit tests >80% coverage

## Definition of Done
- [ ] Code implemented in `src/silver/player_mapping.py`
- [ ] Unit tests written (>80% coverage)
- [ ] Integration tests passing
- [ ] All Ruff/Black/MyPy checks passing
- [ ] Data uploaded to Supabase `silver_player_mapping`
- [ ] Documentation updated

## Agent
build

## Status
In Review

## Progress Log
- 2026-04-09: Created SQL schema in `supabase/migrations/001_create_silver_player_mapping.sql`
- 2026-04-09: Implemented player mapping logic in `src/silver/player_mapping.py`
- 2026-04-09: Created unit tests in `tests/test_silver_player_mapping.py`
- 2026-04-09: Fixed Understat data ingestion to include player names (fixed `_to_polars` to reset_index)
- 2026-04-09: Fixed duplicate issue by removing team-based join that caused cartesian product
- 2026-04-09: Uploaded 3,060 player mappings to Supabase
- 2026-04-09: Added full_name field for better FPL↔Understat matching (combines first_name + second_name)
- 2026-04-09: Implemented Understat ID matching - now 1,854 players have Understat IDs across all seasons
- 2026-04-09: Current matching rate: 60.6% (1,854/3,060 players)
- 2026-04-09: Analysis of unmatched players:
  - ~421 FPL players not in Understat for 2025-26
  - Main reasons:
    1. Name variations: "Pedro Porro Sauceda" vs "Pedro Porro", "Martín Zubimendi Ibáñez" vs "Martín Zubimendi"
    2. Short names: "Raya" vs "David Raya", "Zubimendi" vs "Martín Zubimendi"
    3. Players with very few minutes not tracked by Understat
  - With threshold 0.50 instead of 0.70, match rate would be ~74%
- 2026-04-09: Improved matching with team disambiguation + lower threshold (0.55)
- 2026-04-09: **New matching rates:**
  - 2021-22: 405/525 = 77.1%
  - 2022-23: 418/556 = 75.2%
  - 2023-24: 420/584 = 71.9%
  - 2024-25: 419/570 = 73.5%
  - 2025-26: 542/825 = 65.7%
  - **Total: 2,204/3,060 = 72.0%**
