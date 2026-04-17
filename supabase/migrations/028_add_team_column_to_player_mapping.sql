-- Migration 028: Add team column to silver_player_mapping
-- Needed for tracking transferred players (player can have multiple teams per season)

ALTER TABLE silver_player_mapping
ADD COLUMN IF NOT EXISTS team TEXT;

-- Add index for (season, team) lookups
CREATE INDEX IF NOT EXISTS idx_silver_player_mapping_season_team
ON silver_player_mapping(season, team);

-- Add index for (season, vaastav_id, team) lookups - for transferred players
CREATE INDEX IF NOT EXISTS idx_silver_player_mapping_season_vaastav_team
ON silver_player_mapping(season, vaastav_id, team);

-- Add index for (season, fpl_id, team) lookups - for current season transfers
CREATE INDEX IF NOT EXISTS idx_silver_player_mapping_season_fpl_team
ON silver_player_mapping(season, fpl_id, team);
