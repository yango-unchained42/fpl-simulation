-- Migration 024: Drop obsolete team ID columns from silver_match_mapping
-- These are now replaced by unified_team_id columns

ALTER TABLE silver_match_mapping
DROP COLUMN IF EXISTS home_team_id,
DROP COLUMN IF EXISTS away_team_id;

COMMENT ON TABLE silver_match_mapping IS 'Unified fixture mapping across FPL, Vaastav, and Understat sources. Uses unified_team_id from silver_team_mapping.';
