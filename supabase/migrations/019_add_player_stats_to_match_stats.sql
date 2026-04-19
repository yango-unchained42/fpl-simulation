-- Add aggregated player stats columns to silver_understat_match_stats
-- These are aggregated from silver_understat_player_stats

ALTER TABLE silver_understat_match_stats
ADD COLUMN IF NOT EXISTS home_shots INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS home_xa FLOAT DEFAULT 0,
ADD COLUMN IF NOT EXISTS home_key_passes INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS home_yellow_cards INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS home_red_cards INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS away_shots INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS away_xa FLOAT DEFAULT 0,
ADD COLUMN IF NOT EXISTS away_key_passes INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS away_yellow_cards INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS away_red_cards INTEGER DEFAULT 0;
