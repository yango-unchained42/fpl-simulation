-- FPL Simulation Database Schema - Medallion Architecture
-- Run this in your Supabase SQL Editor.
--
-- BRONZE LAYER: Raw data as ingested from external sources
-- SILVER LAYER: Cleaned, standardized, enriched data
-- GOLD LAYER: Aggregated features and model predictions
--
-- IMPORTANT: Drop existing tables first if they exist:
-- DROP TABLE IF EXISTS gold_predictions, gold_user_teams, gold_player_features, silver_player_crosswalk, silver_player_history, silver_teams, silver_players, bronze_team_mappings, bronze_understat_player_stats, bronze_understat_shots, bronze_player_history, bronze_fpl_fixtures, bronze_fpl_teams, bronze_fpl_players CASCADE;

-- ============================================================================
-- BRONZE LAYER (Raw Data) - Source of truth, immutable once loaded
-- ============================================================================

-- 1. Bronze: FPL Players (Current Season Raw)
CREATE TABLE IF NOT EXISTS bronze_fpl_players (
    id INTEGER PRIMARY KEY,
    web_name TEXT,
    first_name TEXT,
    second_name TEXT,
    team INTEGER,
    element_type INTEGER,
    now_cost INTEGER,
    total_points INTEGER,
    selected_by_percent TEXT,
    status TEXT,
    code INTEGER,
    photo TEXT,
    points_per_game TEXT,
    form TEXT,
    value_season TEXT,
    value_form TEXT,
    transfers_in INTEGER,
    transfers_out INTEGER,
    transfers_in_event INTEGER,
    transfers_out_event INTEGER,
    news TEXT,
    chance_of_playing_next_round INTEGER,
    chance_of_playing_this_round INTEGER,
    cost_change_event INTEGER,
    cost_change_event_fall INTEGER,
    cost_change_start INTEGER,
    cost_change_start_fall INTEGER,
    dreamteam_count INTEGER,
    ep_next TEXT,
    ep_this TEXT,
    event_points INTEGER,
    in_dreamteam BOOLEAN,
    removed BOOLEAN,
    special BOOLEAN,
    squad_number INTEGER,
    team_code INTEGER,
    minutes INTEGER,
    goals_scored INTEGER,
    assists INTEGER,
    clean_sheets INTEGER,
    goals_conceded INTEGER,
    own_goals INTEGER,
    penalties_saved INTEGER,
    penalties_missed INTEGER,
    yellow_cards INTEGER,
    red_cards INTEGER,
    saves INTEGER,
    bonus INTEGER,
    bps INTEGER,
    influence TEXT,
    creativity TEXT,
    threat TEXT,
    ict_index TEXT,
    clearances_blocks_interceptions INTEGER,
    recoveries INTEGER,
    tackles INTEGER,
    defensive_contribution INTEGER,
    starts INTEGER,
    expected_goals TEXT,
    expected_assists TEXT,
    expected_goal_involvements TEXT,
    expected_goals_conceded TEXT,
    season TEXT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 2. Bronze: FPL Teams (Current Season Raw)
CREATE TABLE IF NOT EXISTS bronze_fpl_teams (
    id INTEGER PRIMARY KEY,
    name TEXT,
    short_name TEXT,
    code INTEGER,
    strength INTEGER,
    strength_overall_home INTEGER,
    strength_overall_away INTEGER,
    strength_attack_home INTEGER,
    strength_attack_away INTEGER,
    strength_defence_home INTEGER,
    strength_defence_away INTEGER,
    pulse_id INTEGER,
    season TEXT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 3. Bronze: FPL Fixtures (All Fixtures Raw)
CREATE TABLE IF NOT EXISTS bronze_fpl_fixtures (
    id INTEGER PRIMARY KEY,
    event INTEGER,
    team_h INTEGER,
    team_a INTEGER,
    finished BOOLEAN,
    started BOOLEAN,
    team_h_score INTEGER,
    team_a_score INTEGER,
    kickoff_time TEXT,
    team_h_difficulty INTEGER,
    team_a_difficulty INTEGER,
    pulse_id INTEGER,
    season TEXT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 4. Bronze: Player History (Historical GW Data from Vaastav)
CREATE TABLE IF NOT EXISTS bronze_player_history (
    player_id INTEGER,
    gameweek INTEGER,
    team TEXT,  -- Vaastav uses team names (e.g., "Arsenal", "Man City")
    minutes INTEGER,
    goals_scored INTEGER,
    assists INTEGER,
    clean_sheets INTEGER,
    goals_conceded INTEGER,
    expected_goals FLOAT,
    expected_assists FLOAT,
    total_points INTEGER,
    was_home BOOLEAN,
    opponent_team TEXT,  -- Vaastav uses team names
    season TEXT,
    PRIMARY KEY (player_id, gameweek, season),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 5. Bronze: Team Mappings (Crosswalk between sources)
CREATE TABLE IF NOT EXISTS bronze_team_mappings (
    season TEXT,
    source TEXT,
    source_team_id TEXT,  -- Can be INTEGER (FPL/Understat) or TEXT (Vaastav names)
    source_team_name TEXT,
    fpl_team_id INTEGER,
    fpl_team_name TEXT,
    PRIMARY KEY (season, source, source_team_id),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 6. Bronze: Understat Shots (Raw Shot Data)
CREATE TABLE IF NOT EXISTS bronze_understat_shots (
    id SERIAL PRIMARY KEY,
    game_id INTEGER,
    player_id INTEGER,
    team_id INTEGER,
    assist_player_id INTEGER,
    assist_player TEXT,
    xg FLOAT,
    location_x FLOAT,
    location_y FLOAT,
    minute INTEGER,
    body_part TEXT,
    situation TEXT,
    result TEXT,
    date TEXT,
    season TEXT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 7. Bronze: Understat Player Stats (Aggregated Match Stats)
CREATE TABLE IF NOT EXISTS bronze_understat_player_stats (
    player_id INTEGER,
    gameweek INTEGER,
    game_id INTEGER,
    team_id INTEGER,
    position TEXT,
    position_id INTEGER,
    minutes INTEGER,
    goals INTEGER,
    assists INTEGER,
    shots INTEGER,
    xg FLOAT,
    xa FLOAT,
    xg_chain FLOAT,
    xg_buildup FLOAT,
    key_passes INTEGER,
    own_goals INTEGER,
    yellow_cards INTEGER,
    red_cards INTEGER,
    season TEXT,
    PRIMARY KEY (player_id, gameweek, season),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 8. Bronze: Understat Match Stats (Team-level match data)
CREATE TABLE IF NOT EXISTS bronze_understat_match_stats (
    game_id INTEGER PRIMARY KEY,
    date TEXT,
    season TEXT,
    home_team_id INTEGER,
    away_team_id INTEGER,
    home_team TEXT,
    away_team TEXT,
    home_goals INTEGER,
    away_goals INTEGER,
    home_xg FLOAT,
    away_xg FLOAT,
    home_np_xg FLOAT,
    away_np_xg FLOAT,
    home_np_xg_difference FLOAT,
    away_np_xg_difference FLOAT,
    home_ppda FLOAT,
    away_ppda FLOAT,
    home_deep_completions INTEGER,
    away_deep_completions INTEGER,
    home_expected_points FLOAT,
    away_expected_points FLOAT,
    home_points INTEGER,
    away_points INTEGER,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ============================================================================
-- SILVER LAYER (Cleaned, Standardized, Enriched)
-- ============================================================================

-- 8. Silver: Cleaned Players (Standardized Names, Positions)
CREATE TABLE IF NOT EXISTS silver_players (
    fpl_player_id INTEGER PRIMARY KEY,
    web_name TEXT,
    full_name TEXT,
    position TEXT,
    fpl_team_id INTEGER,
    current_price FLOAT,
    season TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 9. Silver: Cleaned Teams (Canonical FPL Team IDs)
CREATE TABLE IF NOT EXISTS silver_teams (
    fpl_team_id INTEGER PRIMARY KEY,
    fpl_team_name TEXT,
    short_name TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 10. Silver: Player History with FPL Team IDs (Tracks Transfers)
CREATE TABLE IF NOT EXISTS silver_player_history (
    fpl_player_id INTEGER,
    fpl_team_id INTEGER,
    gameweek INTEGER,
    minutes INTEGER,
    goals_scored INTEGER,
    assists INTEGER,
    clean_sheets INTEGER,
    goals_conceded INTEGER,
    expected_goals FLOAT,
    expected_assists FLOAT,
    total_points INTEGER,
    was_home BOOLEAN,
    opponent_fpl_team_id INTEGER,
    season TEXT,
    PRIMARY KEY (fpl_player_id, gameweek, season),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 11. Silver: Player Crosswalk (Understat ↔ FPL ID Mappings)
CREATE TABLE IF NOT EXISTS silver_player_crosswalk (
    understat_player_id INTEGER,
    fpl_player_id INTEGER,
    understat_name TEXT,
    fpl_name TEXT,
    confidence FLOAT,
    season TEXT,
    PRIMARY KEY (understat_player_id, season),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ============================================================================
-- GOLD LAYER (Features, Predictions, Aggregated Data)
-- ============================================================================

-- 12. Gold: Player Features (All Computed Features for ML)
-- This is the main table for ML training and Streamlit app
-- Denormalized with player names for fast queries (no joins needed)
CREATE TABLE IF NOT EXISTS gold_player_features (
    fpl_player_id INTEGER,
    fpl_team_id INTEGER,
    gameweek INTEGER,
    season TEXT,
    -- Identity (denormalized for fast queries)
    player_name TEXT,
    player_short_name TEXT,
    position TEXT,
    team_name TEXT,
    team_short_name TEXT,
    opponent_fpl_team_id INTEGER,
    opponent_team_name TEXT,
    is_home BOOLEAN,
    -- Market data (for display)
    price FLOAT,
    total_points_season INTEGER,
    points_per_game FLOAT,
    form FLOAT,
    -- Rolling features (3/5/10 GW)
    rolling_points_3 FLOAT, rolling_points_5 FLOAT, rolling_points_10 FLOAT,
    rolling_minutes_3 FLOAT, rolling_minutes_5 FLOAT, rolling_minutes_10 FLOAT,
    rolling_goals_3 FLOAT, rolling_goals_5 FLOAT, rolling_goals_10 FLOAT,
    rolling_assists_3 FLOAT, rolling_assists_5 FLOAT, rolling_assists_10 FLOAT,
    rolling_xg_3 FLOAT, rolling_xg_5 FLOAT, rolling_xg_10 FLOAT,
    rolling_xa_3 FLOAT, rolling_xa_5 FLOAT, rolling_xa_10 FLOAT,
    rolling_xgi_3 FLOAT, rolling_xgi_5 FLOAT, rolling_xgi_10 FLOAT,
    rolling_xg_conceded_3 FLOAT, rolling_xg_conceded_5 FLOAT, rolling_xg_conceded_10 FLOAT,
    -- Rolling ICT metrics
    rolling_ict_3 FLOAT, rolling_ict_5 FLOAT, rolling_ict_10 FLOAT,
    rolling_influence_3 FLOAT, rolling_influence_5 FLOAT, rolling_influence_10 FLOAT,
    rolling_creativity_3 FLOAT, rolling_creativity_5 FLOAT, rolling_creativity_10 FLOAT,
    rolling_threat_3 FLOAT, rolling_threat_5 FLOAT, rolling_threat_10 FLOAT,
    -- Rolling Understat advanced metrics
    rolling_shots_3 FLOAT, rolling_shots_5 FLOAT, rolling_shots_10 FLOAT,
    rolling_key_passes_3 FLOAT, rolling_key_passes_5 FLOAT, rolling_key_passes_10 FLOAT,
    rolling_xg_chain_3 FLOAT, rolling_xg_chain_5 FLOAT, rolling_xg_chain_10 FLOAT,
    rolling_xg_buildup_3 FLOAT, rolling_xg_buildup_5 FLOAT, rolling_xg_buildup_10 FLOAT,
    -- Rolling defensive metrics
    rolling_clean_sheets_3 FLOAT, rolling_clean_sheets_5 FLOAT, rolling_clean_sheets_10 FLOAT,
    rolling_goals_conceded_3 FLOAT, rolling_goals_conceded_5 FLOAT, rolling_goals_conceded_10 FLOAT,
    rolling_saves_3 FLOAT, rolling_saves_5 FLOAT, rolling_saves_10 FLOAT,
    rolling_tackles_3 FLOAT, rolling_tackles_5 FLOAT, rolling_tackles_10 FLOAT,
    rolling_interceptions_3 FLOAT, rolling_interceptions_5 FLOAT, rolling_interceptions_10 FLOAT,
    rolling_recoveries_3 FLOAT, rolling_recoveries_5 FLOAT, rolling_recoveries_10 FLOAT,
    -- Rolling discipline
    rolling_yellow_cards_3 FLOAT, rolling_yellow_cards_5 FLOAT, rolling_yellow_cards_10 FLOAT,
    rolling_bonus_3 FLOAT, rolling_bonus_5 FLOAT, rolling_bonus_10 FLOAT,
    -- Rolling market metrics
    rolling_value_3 FLOAT, rolling_value_5 FLOAT, rolling_value_10 FLOAT,
    rolling_selected_3 FLOAT, rolling_selected_5 FLOAT, rolling_selected_10 FLOAT,
    rolling_form_3 FLOAT, rolling_form_5 FLOAT, rolling_form_10 FLOAT,
    rolling_transfers_in_3 FLOAT, rolling_transfers_in_5 FLOAT, rolling_transfers_in_10 FLOAT,
    rolling_transfers_out_3 FLOAT, rolling_transfers_out_5 FLOAT, rolling_transfers_out_10 FLOAT,
    -- H2H features
    h2h_avg_points_vs_opponent FLOAT,
    h2h_avg_xg_vs_opponent FLOAT,
    h2h_avg_xa_vs_opponent FLOAT,
    h2h_avg_minutes_vs_opponent FLOAT,
    h2h_avg_goals_vs_opponent FLOAT,
    h2h_avg_assists_vs_opponent FLOAT,
    h2h_games_played INTEGER,
    -- Team H2H features
    team_home_xg_avg FLOAT,
    team_away_xg_avg FLOAT,
    team_home_goals_avg FLOAT,
    team_away_goals_avg FLOAT,
    team_home_shots_avg FLOAT,
    team_away_shots_avg FLOAT,
    team_home_ppda_avg FLOAT,
    team_away_ppda_avg FLOAT,
    team_home_deep_completions_avg FLOAT,
    team_away_deep_completions_avg FLOAT,
    -- Home/Away advantage factors
    home_advantage_factor FLOAT,
    away_advantage_factor FLOAT,
    player_home_boost FLOAT,
    player_away_boost FLOAT,
    -- Form features
    form_score FLOAT,
    form_trend_3gw FLOAT,
    form_trend_5gw FLOAT,
    -- Context features
    fixture_difficulty INTEGER,
    opponent_strength_attack FLOAT,
    opponent_strength_defence FLOAT,
    opponent_overall_strength FLOAT,
    days_rest INTEGER,
    is_bgw BOOLEAN,
    is_dgw BOOLEAN,
    games_in_window INTEGER,
    -- ML features
    xi_probability FLOAT,
    expected_points FLOAT,
    PRIMARY KEY (fpl_player_id, gameweek, season),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 13. Gold: Predictions (Model Outputs)
CREATE TABLE IF NOT EXISTS gold_predictions (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    fpl_player_id INTEGER,
    gameweek INTEGER,
    expected_points FLOAT,
    xi_probability FLOAT,
    goal_probability FLOAT,
    assist_probability FLOAT,
    clean_sheet_probability FLOAT,
    minutes_probability FLOAT,
    combined_score FLOAT,
    is_captain_pick BOOLEAN,
    model_version TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 14. Gold: User Teams (Saved Squads from Optimizer)
CREATE TABLE IF NOT EXISTS gold_user_teams (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id TEXT,
    gameweek INTEGER,
    squad_ids INTEGER[],
    captain_id INTEGER,
    vice_captain_id INTEGER,
    expected_points FLOAT,
    total_cost FLOAT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Bronze indexes
CREATE INDEX IF NOT EXISTS idx_bronze_players_team ON bronze_fpl_players(team);
CREATE INDEX IF NOT EXISTS idx_bronze_players_season ON bronze_fpl_players(season);
CREATE INDEX IF NOT EXISTS idx_bronze_history_player ON bronze_player_history(player_id);
CREATE INDEX IF NOT EXISTS idx_bronze_history_gw ON bronze_player_history(gameweek);
CREATE INDEX IF NOT EXISTS idx_bronze_history_season ON bronze_player_history(season);
CREATE INDEX IF NOT EXISTS idx_bronze_fixtures_gw ON bronze_fpl_fixtures(event);
CREATE INDEX IF NOT EXISTS idx_bronze_fixtures_season ON bronze_fpl_fixtures(season);
CREATE INDEX IF NOT EXISTS idx_bronze_team_mappings_season ON bronze_team_mappings(season);
CREATE INDEX IF NOT EXISTS idx_bronze_understat_stats_player ON bronze_understat_player_stats(player_id);

-- Silver indexes
CREATE INDEX IF NOT EXISTS idx_silver_players_team ON silver_players(fpl_team_id);
CREATE INDEX IF NOT EXISTS idx_silver_players_season ON silver_players(season);
CREATE INDEX IF NOT EXISTS idx_silver_history_player ON silver_player_history(fpl_player_id);
CREATE INDEX IF NOT EXISTS idx_silver_history_team ON silver_player_history(fpl_team_id);
CREATE INDEX IF NOT EXISTS idx_silver_history_gw ON silver_player_history(gameweek);
CREATE INDEX IF NOT EXISTS idx_silver_crosswalk_fpl ON silver_player_crosswalk(fpl_player_id);

-- Gold indexes
CREATE INDEX IF NOT EXISTS idx_gold_features_player ON gold_player_features(fpl_player_id);
CREATE INDEX IF NOT EXISTS idx_gold_features_gw ON gold_player_features(gameweek);
CREATE INDEX IF NOT EXISTS idx_gold_features_season ON gold_player_features(season);
CREATE INDEX IF NOT EXISTS idx_gold_predictions_player ON gold_predictions(fpl_player_id);
CREATE INDEX IF NOT EXISTS idx_gold_predictions_gw ON gold_predictions(gameweek);
CREATE INDEX IF NOT EXISTS idx_gold_user_teams_gw ON gold_user_teams(gameweek);
