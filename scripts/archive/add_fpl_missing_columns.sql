-- Add missing columns to bronze_fpl_players
-- Run this in Supabase SQL Editor

ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS birth_date text;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS can_select boolean;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS can_transact boolean;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS clean_sheets_per_90 double precision;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS corners_and_indirect_freekicks_order integer;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS corners_and_indirect_freekicks_text text;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS creativity_rank integer;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS creativity_rank_type text;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS defensive_contribution_per_90 double precision;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS direct_freekicks_order integer;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS direct_freekicks_text text;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS expected_assists_per_90 double precision;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS expected_goal_involvements_per_90 double precision;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS expected_goals_conceded_per_90 double precision;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS expected_goals_per_90 double precision;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS form_rank integer;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS form_rank_type text;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS goals_conceded_per_90 double precision;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS has_temporary_code boolean;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS ict_index_rank integer;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS ict_index_rank_type text;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS influence_rank integer;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS influence_rank_type text;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS known_name text;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS news_added text;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS now_cost_rank integer;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS now_cost_rank_type text;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS opta_code text;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS penalties_order integer;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS penalties_text text;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS points_per_game_rank integer;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS points_per_game_rank_type text;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS price_change_percent double precision;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS region text;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS saves_per_90 double precision;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS scout_news_link text;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS scout_risks text;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS selected_rank integer;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS selected_rank_type text;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS starts_per_90 double precision;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS team_join_date text;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS threat_rank integer;
ALTER TABLE bronze_fpl_players ADD COLUMN IF NOT EXISTS threat_rank_type text;

-- Add missing columns to bronze_fpl_teams
ALTER TABLE bronze_fpl_teams ADD COLUMN IF NOT EXISTS draw integer;
ALTER TABLE bronze_fpl_teams ADD COLUMN IF NOT EXISTS form double precision;
ALTER TABLE bronze_fpl_teams ADD COLUMN IF NOT EXISTS loss integer;
ALTER TABLE bronze_fpl_teams ADD COLUMN IF NOT EXISTS played integer;
ALTER TABLE bronze_fpl_teams ADD COLUMN IF NOT EXISTS points integer;
ALTER TABLE bronze_fpl_teams ADD COLUMN IF NOT EXISTS position integer;
ALTER TABLE bronze_fpl_teams ADD COLUMN IF NOT EXISTS team_division text;
ALTER TABLE bronze_fpl_teams ADD COLUMN IF NOT EXISTS unavailable boolean;
ALTER TABLE bronze_fpl_teams ADD COLUMN IF NOT EXISTS win integer;

-- Add missing columns to bronze_fpl_fixtures
ALTER TABLE bronze_fpl_fixtures ADD COLUMN IF NOT EXISTS code integer;
ALTER TABLE bronze_fpl_fixtures ADD COLUMN IF NOT EXISTS finished_provisional boolean;
ALTER TABLE bronze_fpl_fixtures ADD COLUMN IF NOT EXISTS minutes integer;
ALTER TABLE bronze_fpl_fixtures ADD COLUMN IF NOT EXISTS provisional_start_time boolean;
ALTER TABLE bronze_fpl_fixtures ADD COLUMN IF NOT EXISTS stats jsonb;
