-- Add UUID columns to silver_understat_shots for FK references
ALTER TABLE silver_understat_shots
ADD COLUMN IF NOT EXISTS unified_player_id UUID REFERENCES silver_player_mapping(unified_player_id),
ADD COLUMN IF NOT EXISTS unified_team_id UUID REFERENCES silver_team_mapping(unified_team_id);

-- Add UUID columns to silver_understat_match_stats for FK references
ALTER TABLE silver_understat_match_stats
ADD COLUMN IF NOT EXISTS home_unified_team_id UUID REFERENCES silver_team_mapping(unified_team_id),
ADD COLUMN IF NOT EXISTS away_unified_team_id UUID REFERENCES silver_team_mapping(unified_team_id);

-- Create indexes for FK lookups
CREATE INDEX IF NOT EXISTS idx_silver_understat_shots_player ON silver_understat_shots(unified_player_id) WHERE unified_player_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_silver_understat_shots_team ON silver_understat_shots(unified_team_id) WHERE unified_team_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_silver_understat_match_stats_home_team ON silver_understat_match_stats(home_unified_team_id) WHERE home_unified_team_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_silver_understat_match_stats_away_team ON silver_understat_match_stats(away_unified_team_id) WHERE away_unified_team_id IS NOT NULL;
