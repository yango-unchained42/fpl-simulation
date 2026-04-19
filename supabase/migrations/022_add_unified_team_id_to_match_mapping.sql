-- Migration: 022_add_unified_team_id_to_match_mapping
-- Add unified_team_id columns to match_mapping for proper team resolution

ALTER TABLE match_mapping
    ADD COLUMN IF NOT EXISTS home_unified_team_id UUID,
    ADD COLUMN IF NOT EXISTS away_unified_team_id UUID;

-- Update existing mappings using silver_team_mapping
-- This only works for seasons that have team mapping data

COMMENT ON COLUMN match_mapping.home_unified_team_id IS 'Linked to silver_team_mapping.unified_team_id for home team';
COMMENT ON COLUMN match_mapping.away_unified_team_id IS 'Linked to silver_team_mapping.unified_team_id for away team';
