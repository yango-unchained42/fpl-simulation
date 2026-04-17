-- Migration 022: Add unified team ID columns to match_mapping
ALTER TABLE match_mapping 
ADD COLUMN IF NOT EXISTS home_unified_team_id UUID,
ADD COLUMN IF NOT EXISTS away_unified_team_id UUID;