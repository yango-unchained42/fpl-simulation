-- Migration 026: Drop redundant team name column from silver_player_mapping
-- Team is now referenced via unified_team_id UUID

ALTER TABLE silver_player_mapping
DROP COLUMN IF EXISTS team;

-- Also drop the index on (season, team) if it exists
DROP INDEX IF EXISTS idx_silver_player_mapping_season_team;