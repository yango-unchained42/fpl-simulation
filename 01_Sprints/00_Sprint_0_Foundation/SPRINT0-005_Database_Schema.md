# Ticket: SPRINT0-005 - Database Schema Design

## Description
Design and implement the Supabase (Postgres) database schema for storing FPL data, player statistics, predictions, and model outputs.

## Technical Requirements
Create database schema with the following tables:
- players: Player information (player_id, name "First Last" format, team_id, position, price, selected_by_percent, status)
- teams: Team information (team_id, name, strength_attack, strength_defense, strength_midfield)
- fixtures: Match data (fixture_id, home_team_id, away_team_id, gameweek, date, is_home_advantage, is_double_gw, is_blank_gw)
- player_stats: Player performance statistics per gameweek (player_id, fixture_id, gameweek, minutes, goals, assists, clean_sheets, yellow_cards, red_cards, saves, points, bps, xg, xa, xgb)
- team_h2h: Team head-to-head metrics (home_team_id, away_team_id, avg_goals_scored, avg_goals_conceded, clean_sheet_rate, last_5_meetings JSONB, appearances)
- player_vs_team: Player vs team defense metrics (player_id, opponent_team_id, avg_points, avg_xg, goals, appearances)
- predictions: Model predictions (id, gameweek, player_id, start_probability, expected_points, expected_goals, expected_assists, clean_sheet_probability, predicted_points)
- match_simulations: Aggregated match simulation results (fixture_id, home_win_pct, draw_pct, away_win_pct, home_cs_pct, away_cs_pct, score_distribution JSONB, expected_home_goals, expected_away_goals, p10_home_goals, p90_home_goals)

## Acceptance Criteria
- [ ] All tables created with proper schema in Supabase
- [ ] Foreign keys and indexes defined
- [ ] Supabase connection string configured (environment variable)
- [ ] Schema documentation generated
- [ ] SQL migration scripts created

## Definition of Done
- [ ] Code implemented and follows project conventions
- [ ] Unit tests written and passing (>80% coverage for this component)
- [ ] Type hints added (100% for public APIs)
- [ ] Code reviewed by reviewer
- [ ] Documentation updated
- [ ] Integrated into main pipeline


## Agent
devops

## Status
Done

## Progress Log

### 2026-04-01 — DevOps Review
Reviewed SQL migrations against scope requirements:
- 001_create_tables.sql: All 8 tables created with proper schema ✓
  - teams (team_id PK, name, strength_attack/defense/midfield) ✓
  - players (player_id PK, name, team_id FK, position, price, selected_by_percent, status) ✓
  - fixtures (fixture_id PK, home/away_team_id FK, gameweek, date, flags) ✓
  - player_stats (composite PK, all stat columns, xg/xa/xgb) ✓
  - team_h2h (composite PK, avg metrics, last_5_meetings JSONB) ✓
  - player_vs_team (composite PK, avg_points, avg_xg, goals, appearances) ✓
  - predictions (SERIAL PK, all prediction columns, created_at) ✓
  - match_simulations (fixture_id PK/FK, win/draw/cs pcts, score_distribution JSONB, percentiles) ✓
- All 10 FOREIGN KEY constraints present ✓
- Tables ordered correctly (teams first) ✓
- 002_create_indexes.sql: 6 performance indexes on gameweek, player_id, fixture_id ✓
- SUPABASE_SETUP.md created with provisioning guide ✓

### 2026-04-01 21:00:00 Quality review passed. All checks green. Ticket closed.

## Comments
[Agents can add questions, blockers, or notes here]
