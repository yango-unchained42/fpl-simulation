-- Fix Understat schema issues
-- Run in Supabase SQL Editor

-- Make gameweek nullable in player_stats (source data doesn't have it)
ALTER TABLE bronze_understat_player_stats ALTER COLUMN gameweek DROP NOT NULL;

-- Drop the datetime columns that cause serialization issues 
-- (we'll filter them out in the script)
-- Or we can convert them to text in Python before upload
