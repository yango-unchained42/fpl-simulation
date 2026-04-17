-- Fix Understat schema - make gameweek nullable
-- Run in Supabase SQL Editor

ALTER TABLE bronze_understat_player_stats ALTER COLUMN gameweek DROP NOT NULL;
