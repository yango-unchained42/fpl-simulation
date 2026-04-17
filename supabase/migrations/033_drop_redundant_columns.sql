-- Migration: 033_drop_redundant_columns_from_silver_fpl_player_stats
-- Reason: 
-- 1. Opponent team can be derived from match data (match_id + was_home)
-- 2. player_id is replaced by unified_player_id (the canonical identifier)
-- 3. fixture and opponent_team are redundant - using match_id instead

ALTER TABLE silver_fpl_player_stats DROP COLUMN IF EXISTS opponent_unified_team_id;
ALTER TABLE silver_fpl_player_stats DROP COLUMN IF EXISTS fixture;
ALTER TABLE silver_fpl_player_stats DROP COLUMN IF EXISTS opponent_team;
ALTER TABLE silver_fpl_player_stats DROP COLUMN IF EXISTS player_id;