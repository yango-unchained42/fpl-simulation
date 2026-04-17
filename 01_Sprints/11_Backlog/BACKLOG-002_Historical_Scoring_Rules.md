# Ticket: BACKLOG-002 - Historical FPL Scoring Rules Table

## Description
Create a table to track FPL scoring rule changes over seasons. This is critical for accurate model training - if we train on historical data using current rules, predictions will be wrong.

## Problem
- FPL scoring rules have changed over years (e.g., bonus points system, goal points for mids/forwards)
- Current `fpl_scoring.py` uses 2025-26 rules
- Historical data (Vaastav) may have been scored under different rules
- Model training will produce incorrect results if rules mismatch

## Known Changes (Research Needed)

### 2024-25 vs 2025-26
- Bonus points system changed

### Historical (need verification)
- When did MID goals change from 5 to 5? (was 5, changed to 6, back to 5?)
- When did FWD goals change from 4 to? (was 4, changed to 5, back to 4?)
- Clean sheet points for DEF?

## Solution

### Option 1: Create Historical Rules Table
Create table in Supabase:
```sql
CREATE TABLE historical_fpl_rules (
    season TEXT PRIMARY KEY,
    goal_gkp INT,
    goal_def INT,
    goal_mid INT,
    goal_fwd INT,
    assist INT,
    clean_sheet_gkp INT,
    clean_sheet_def INT,
    clean_sheet_mid INT,
    clean_sheet_fwd INT,
    goals_conceded_per INT,
    bonus_system TEXT,  -- 'automatic', 'bps', 'new_system'
    save_per_3 INT,
    yellow_card INT,
    red_card INT,
    penalty_save INT,
    penalty_miss INT,
    minutes_60_plus INT,
    minutes_1_to_59 INT
);
```

### Option 2: Recalculate Historical Points
Instead of storing rules, recalculate historical points using the correct rules:
1. Load raw events (goals, assists, saves, etc.) from Vaastav
2. Apply the scoring rules for that season
3. Store the recalculated points

## Technical Requirements
- [ ] Research all FPL rule changes by season
- [ ] Create historical rules table or recalculation logic
- [ ] Update dataset builder to use correct rules per season
- [ ] Validate recalculated points match historical data

## Priority
High - Without this, historical model training is incorrect

## Dependencies
- Data in Supabase (Vaastav data)

## Agent
TBD

## Status
📋 Backlog