# Ticket: SPRINT11-002 - Team Identity Resolution

## Description
Create a unified team identity mapping across all data sources (FPL, Vaastav, Understat). This enables joining player data with team data and calculating team-based features.

## Technical Requirements
- Build `silver_team_mapping` table in Supabase with:
  - `unified_team_id` (PK, auto-generated UUID)
  - `fpl_team_id` (from FPL `id` column)
  - `fpl_team_name` (from FPL `name` column)
  - `vaastav_team_name` (from Vaastav `team` column)
  - `understat_team_id` (from Understat `team_id`)
  - `understat_team_name` (from Understat `team`)
  - `team_code` (3-letter code, e.g., "CHE")
  - `source` (how mapping was determined: exact/fuzzy/manual)
  - `confidence_score` (0.0-1.0)
  - `created_at`, `updated_at`

- Handle team name variations:
  - "Man Utd" vs "Manchester United"
  - "Nott'm Forest" vs "Nottingham Forest"
  - Team name abbreviations

- Add historical team tracking:
  - Teams change names/promoted/relegated
  - Track team seasons (which seasons each team was in PL)

## Acceptance Criteria
- [ ] `silver_team_mapping` table created in Supabase
- [ ] All current season teams mapped across all sources
- [ ] Team name normalization handles common variations
- [ ] Historical team tracking (promoted/relegated teams)

## Definition of Done
- [ ] Code implemented in `src/silver/team_mapping.py`
- [ ] Unit tests written (>80% coverage for this component)
- [ ] Integration tests passing
- [ ] All Rust/Black/MyPy checks passing
- [ ] Data uploaded to Supabase `silver_team_mapping`
- [ ] Documentation updated

## Agent
build

## Status
In Review

## Progress Log
- 2026-04-09: Created SQL migration `supabase/migrations/002_create_silver_team_mapping.sql`
- 2026-04-09: Implemented team mapping logic in `src/silver/team_mapping.py`
- 2026-04-09: Created team name normalization mapping (Man City → Manchester City, etc.)
- 2026-04-09: Uploaded 100 team mappings to Supabase (20 teams × 5 seasons)
- 2026-04-09: All 2025-26 teams correctly mapped between FPL and Understat
