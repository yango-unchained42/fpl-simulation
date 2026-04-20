-- Add UNIQUE constraints to silver_player_mapping to prevent duplicate UUIDs
-- Each (season, source_id) combination should map to exactly one UUID

-- First clean any remaining duplicates (should already be clean from code fix)
-- This is a safety net

-- Add UNIQUE constraints
ALTER TABLE silver_player_mapping 
  DROP CONSTRAINT IF EXISTS uq_player_season_understat;

ALTER TABLE silver_player_mapping 
  ADD CONSTRAINT uq_player_season_understat 
  UNIQUE (season, understat_id);

ALTER TABLE silver_player_mapping 
  DROP CONSTRAINT IF EXISTS uq_player_season_fpl;

ALTER TABLE silver_player_mapping 
  ADD CONSTRAINT uq_player_season_fpl 
  UNIQUE (season, fpl_id);

ALTER TABLE silver_player_mapping 
  DROP CONSTRAINT IF EXISTS uq_player_season_vaastav;

ALTER TABLE silver_player_mapping 
  ADD CONSTRAINT uq_player_season_vaastav 
  UNIQUE (season, vaastav_id);
