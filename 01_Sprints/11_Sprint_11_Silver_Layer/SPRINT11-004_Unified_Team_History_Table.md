# Ticket: SPRINT11-004 - Unified Team History Table

## Description
Create a unified team history table that merges team performance data from all sources across all seasons.

## Technical Requirements
- Build `silver_team_history` table in Supabase with:
  - `unified_team_id` (FK)
  - `season`
  - `gameweek`
  - `match_date`
  - `opponent_team_id` (FK)
  - `was_home` (boolean)
  - `goals_scored`
  - `goals_conceded`
  - `expected_goals` (xG for/against)
  - `expected_goals_conceded` (xGC)
  - `shots`
  - `shots_on_target`
  - `ppda` (Passes Per Defensive Action - defensive metric)
  - `deep_completions` (passes into penalty area)
  - `points` (FPL match points for team)
  - `result` (W/D/L)
  - `source` (fpl/vaastav/understat)

- Aggregate team stats (per season):
  - `total_goals_for`
  - `total_goals_against`
  - `total_xg_for`
  - `total_xg_against`
  - `clean_sheets`
  - `wins`/`draws`/`losses`

## Acceptance Criteria
- [ ] `silver_team_history` table created
- [ ] All historical team data merged (2021-25)
- [ ] Team aggregate stats calculated per season

## Definition of Done
- [ ] Code implemented in `src/silver/team_history.py`
- [ ] Unit tests written (>80% coverage)
- [ ] Integration tests passing
- [ ] All checks passing
- [ ] Data uploaded to Supabase

## Agent
build

## Status
In Progress

## Progress Log
- 2026-04-10: Ticket picked up - fixing Understat data issues first
