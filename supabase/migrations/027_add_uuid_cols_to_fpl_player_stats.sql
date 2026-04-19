-- Migration 027: Add UUID columns to silver_fpl_player_stats
-- Replace raw IDs with unified UUIDs for proper referential integrity

-- Add opponent unified team ID
ALTER TABLE silver_fpl_player_stats
ADD COLUMN IF NOT EXISTS opponent_unified_team_id UUID REFERENCES silver_team_mapping(unified_team_id);

-- Add index for lookups
CREATE INDEX IF NOT EXISTS idx_silver_fpl_player_stats_opponent_team
ON silver_fpl_player_stats(opponent_unified_team_id);

CREATE INDEX IF NOT EXISTS idx_silver_fpl_player_stats_match
ON silver_fpl_player_stats(match_id);
