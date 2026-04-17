-- Migration: 021_create_match_mapping
-- Creates match_mapping table to link FPL fixtures ↔ Understat games
-- Produces unique match_id for every game across ALL seasons

CREATE TABLE IF NOT EXISTS match_mapping (
    -- Unique ID for this match (across all sources)
    match_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Season reference
    season TEXT NOT NULL,
    
    -- Date (common between sources)
    match_date DATE NOT NULL,
    
    -- FPL/vaastav fixture IDs
    fpl_fixture_id INTEGER,
    vaastav_fixture_id INTEGER,
    
    -- Understat game ID
    understat_game_id INTEGER,
    
    -- Teams (using FPL team IDs for consistency)
    home_team_id INTEGER,
    away_team_id INTEGER,
    
    -- Team names (for matching across sources)
    home_team_name TEXT,
    away_team_name TEXT,
    
    -- Scores (for reference)
    home_score INTEGER,
    away_score INTEGER,
    
    -- Data quality
    match_source TEXT CHECK (match_source IN ('fpl', 'vaastav', 'understat', 'both')),
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    -- Removed UNIQUE constraints to allow many-to-many relationships
    -- A fixture can be linked to multiple matches if played on same date
);

-- Indexes for efficient lookups
CREATE INDEX IF NOT EXISTS idx_match_mapping_season ON match_mapping(season);
CREATE INDEX IF NOT EXISTS idx_match_mapping_date ON match_mapping(match_date);
CREATE INDEX IF NOT EXISTS idx_match_mapping_fpl_fixture ON match_mapping(season, fpl_fixture_id);
CREATE INDEX IF NOT EXISTS idx_match_mapping_understat ON match_mapping(season, understat_game_id);
CREATE INDEX IF NOT EXISTS idx_match_mapping_teams ON match_mapping(season, home_team_id, away_team_id);

-- Comments
COMMENT ON TABLE match_mapping IS 'Unified match IDs linking FPL fixtures and Understat games by (date, teams)';
COMMENT ON COLUMN match_mapping.match_id IS 'Unique UUID for this match - use as join key instead of fixture/game_id';
COMMENT ON COLUMN match_mapping.match_source IS 'Which source provided the match data: fpl, vaastav, or understat';

-- Let's also add columns to the existing silver tables to store the unified IDs
-- These will be populated by the daily job

-- Add to silver_fpl_player_stats
ALTER TABLE silver_fpl_player_stats 
    ADD COLUMN IF NOT EXISTS unified_player_id UUID,
    ADD COLUMN IF NOT EXISTS match_id UUID;

-- Add to silver_understat_player_stats  
ALTER TABLE silver_understat_player_stats
    ADD COLUMN IF NOT EXISTS unified_player_id UUID,
    ADD COLUMN IF NOT EXISTS unified_team_id UUID,
    ADD COLUMN IF NOT EXISTS match_id UUID;

COMMENT ON COLUMN silver_fpl_player_stats.unified_player_id IS 'Linked to silver_player_mapping.unified_player_id';
COMMENT ON COLUMN silver_fpl_player_stats.match_id IS 'Linked to match_mapping.match_id';
COMMENT ON COLUMN silver_understat_player_stats.unified_player_id IS 'Linked to silver_player_mapping.unified_player_id';
COMMENT ON COLUMN silver_understat_player_stats.unified_team_id IS 'Linked to silver_team_mapping.unified_team_id';
COMMENT ON COLUMN silver_understat_player_stats.match_id IS 'Linked to match_mapping.match_id';