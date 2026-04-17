-- Add missing columns to bronze_player_history (Vaastav)
-- Run this in Supabase SQL Editor

-- FPL stats
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS bonus int;
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS bps int;
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS saves int;
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS starts int;
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS own_goals int;
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS penalties_missed int;
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS penalties_saved int;
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS yellow_cards int;
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS red_cards int;

-- Advanced stats
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS creativity double precision;
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS influence double precision;
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS threat double precision;
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS ict_index double precision;
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS expected_goal_involvements double precision;
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS expected_goals_conceded double precision;

-- Transfers & value
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS transfers_in int;
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS transfers_out int;
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS value int;
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS selected int;

-- Match info
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS fixture int;
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS kickoff_time timestamp;
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS team_a_score int;
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS team_h_score int;

-- xP (expected points - Vaastav specific)
ALTER TABLE bronze_player_history ADD COLUMN IF NOT EXISTS xP double precision;
