# Ticket: SPRINT11-005 - Unified Fixtures Table

## Description
Create a unified fixtures table combining data from FPL, Vaastav, and Understat with standardized team IDs.

## Technical Requirements
- Build `silver_fixtures` table in Supabase with:
  - `fixture_id` (unified, PK)
  - `season`
  - `gameweek`
  - `match_date`
  - `kickoff_time`
  - `home_team_id` (FK to silver_team_mapping)
  - `away_team_id` (FK)
  - `home_goals`
  - `away_goals`
  - `home_expected_goals` (xG)
  - `away_expected_goals` (xG)
  - `home_ppda`
  - `away_ppda`
  - `home_deep_completions`
  - `away_deep_completions`
  - `home_fpl_difficulty`
  - `away_fpl_difficulty`
  - `status` (scheduled/live/completed)
  - `source` (fpl/vaastav/understat)
  - `created_at`

- Sources:
  - FPL: Current season fixtures, difficulty ratings
  - Vaastav: 2021-25 historical fixtures with scores
  - Understat: Detailed match stats (xG, ppda)

## Acceptance Criteria
- [ ] `silver_fixtures` table created
- [ ] Historical fixtures (2021-25) merged
- [ ] Current season FPL fixtures included
- [ ] Team IDs standardized across all records

## Definition of Done
- [ ] Code in `src/silver/fixtures.py`
- [ ] Unit tests >80%
- [ ] Integration tests passing
- [ ] All checks passing
- [ ] Data uploaded to Supabase

## Agent
build

## Status
Pending

## Progress Log
