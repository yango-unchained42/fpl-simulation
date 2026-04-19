-- Migration 025: Add unified_team_id to silver_player_mapping
-- Resolves team names to UUIDs using silver_team_mapping

ALTER TABLE silver_player_mapping
ADD COLUMN IF NOT EXISTS unified_team_id UUID REFERENCES silver_team_mapping(unified_team_id);

-- Add index for faster lookups
CREATE INDEX IF NOT EXISTS idx_silver_player_mapping_unified_team_id
ON silver_player_mapping(unified_team_id);

CREATE INDEX IF NOT EXISTS idx_silver_player_mapping_season_team
ON silver_player_mapping(season, team);
