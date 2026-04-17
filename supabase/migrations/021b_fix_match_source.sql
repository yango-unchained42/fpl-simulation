-- Migration: 021b_fix_match_source
-- Fix the CHECK constraint on match_mapping.match_source to include 'both'

-- Drop existing table
DROP TABLE IF EXISTS match_mapping CASCADE;

-- Recreate with fixed constraint
CREATE TABLE IF NOT EXISTS match_mapping (
    match_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season TEXT NOT NULL,
    match_date DATE NOT NULL,
    fpl_fixture_id INTEGER,
    vaastav_fixture_id INTEGER,
    understat_game_id INTEGER,
    home_team_id INTEGER,
    away_team_id INTEGER,
    home_score INTEGER,
    away_score INTEGER,
    match_source TEXT CHECK (match_source IN ('fpl', 'vaastav', 'understat', 'both')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(season, fpl_fixture_id),
    UNIQUE(season, vaastav_fixture_id),
    UNIQUE(season, understat_game_id)
);

CREATE INDEX IF NOT EXISTS idx_match_mapping_season ON match_mapping(season);
CREATE INDEX IF NOT EXISTS idx_match_mapping_date ON match_mapping(match_date);
CREATE INDEX IF NOT EXISTS idx_match_mapping_fpl_fixture ON match_mapping(season, fpl_fixture_id);
CREATE INDEX IF NOT EXISTS idx_match_mapping_understat ON match_mapping(season, understat_game_id);
CREATE INDEX IF NOT EXISTS idx_match_mapping_teams ON match_mapping(season, home_team_id, away_team_id);

COMMENT ON TABLE match_mapping IS 'Unified match IDs linking FPL fixtures and Understat games by (date, teams)';
COMMENT ON COLUMN match_mapping.match_id IS 'Unique UUID for this match - use as join key instead of fixture/game_id';
COMMENT ON COLUMN match_mapping.match_source IS 'Which source provided the match data: fpl, vaastav, understat, or both';