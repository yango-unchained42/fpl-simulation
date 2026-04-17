-- Fix Understat player_stats primary key
-- Run in Supabase SQL Editor

-- Drop existing primary key
ALTER TABLE bronze_understat_player_stats DROP CONSTRAINT bronze_understat_player_stats_pkey;

-- Add new primary key without gameweek (since source data doesn't have it)
ALTER TABLE bronze_understat_player_stats ADD PRIMARY KEY (player_id, team_id, game_id, season);

-- Now make gameweek nullable
ALTER TABLE bronze_understat_player_stats ALTER COLUMN gameweek DROP NOT NULL;
