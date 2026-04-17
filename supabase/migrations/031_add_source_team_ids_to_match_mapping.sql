-- Migration: 031_add_source_team_ids_to_match_mapping
-- Add source-specific team ID columns to silver_match_mapping for FK references

ALTER TABLE silver_match_mapping
    ADD COLUMN IF NOT EXISTS fpl_home_team_id INTEGER,
    ADD COLUMN IF NOT EXISTS fpl_away_team_id INTEGER,
    ADD COLUMN IF NOT EXISTS vaastav_home_team_id INTEGER,
    ADD COLUMN IF NOT EXISTS vaastav_away_team_id INTEGER,
    ADD COLUMN IF NOT EXISTS understat_home_team_id INTEGER,
    ADD COLUMN IF NOT EXISTS understat_away_team_id INTEGER;

-- Add foreign key references via comments (actual FK constraints would be added separately)
COMMENT ON COLUMN silver_match_mapping.fpl_home_team_id IS 'FPL team ID for home team - FK reference to bronze_fpl_teams.id';
COMMENT ON COLUMN silver_match_mapping.fpl_away_team_id IS 'FPL team ID for away team - FK reference to bronze_fpl_teams.id';
COMMENT ON COLUMN silver_match_mapping.vaastav_home_team_id IS 'Vaastav team ID for home team - FK reference to bronze_vaastav_teams.id';
COMMENT ON COLUMN silver_match_mapping.vaastav_away_team_id IS 'Vaastav team ID for away team - FK reference to bronze_vaastav_teams.id';
COMMENT ON COLUMN silver_match_mapping.understat_home_team_id IS 'Understat team ID for home team - FK reference to bronze_understat_teams.id';
COMMENT ON COLUMN silver_match_mapping.understat_away_team_id IS 'Understat team ID for away team - FK reference to bronze_understat_teams.id';

-- Create indexes for the new columns
CREATE INDEX IF NOT EXISTS idx_silver_match_mapping_fpl_home_team_id ON silver_match_mapping(fpl_home_team_id) WHERE fpl_home_team_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_silver_match_mapping_fpl_away_team_id ON silver_match_mapping(fpl_away_team_id) WHERE fpl_away_team_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_silver_match_mapping_vaastav_home_team_id ON silver_match_mapping(vaastav_home_team_id) WHERE vaastav_home_team_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_silver_match_mapping_vaastav_away_team_id ON silver_match_mapping(vaastav_away_team_id) WHERE vaastav_away_team_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_silver_match_mapping_understat_home_team_id ON silver_match_mapping(understat_home_team_id) WHERE understat_home_team_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_silver_match_mapping_understat_away_team_id ON silver_match_mapping(understat_away_team_id) WHERE understat_away_team_id IS NOT NULL;