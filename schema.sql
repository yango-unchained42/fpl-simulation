-- FPL Simulation Database Schema (Final)
-- Run this in your Supabase SQL Editor.
-- IMPORTANT: Drop existing tables first if they exist:
-- DROP TABLE IF EXISTS predictions, user_teams, player_features, understat_player_stats, understat_shots, player_history, fpl_fixtures, fpl_teams, fpl_players CASCADE;

-- 1. FPL Players (Current Season Info)
CREATE TABLE IF NOT EXISTS fpl_players (
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
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 2. FPL Teams
CREATE TABLE IF NOT EXISTS fpl_teams (
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
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 3. FPL Fixtures
CREATE TABLE IF NOT EXISTS fpl_fixtures (
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
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 4. Player History (Historical GW Data from Vaastav)
-- Includes 'team' column to track player transfers over time
CREATE TABLE IF NOT EXISTS player_history (
    player_id INTEGER,
    gameweek INTEGER,
    team INTEGER,
    minutes INTEGER,
    goals_scored INTEGER,
    assists INTEGER,
    clean_sheets INTEGER,
    goals_conceded INTEGER,
    expected_goals FLOAT,
    expected_assists FLOAT,
    total_points INTEGER,
    was_home BOOLEAN,
    opponent_team INTEGER,
    season TEXT,
    PRIMARY KEY (player_id, gameweek, season),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 5. Player Features (Calculated metrics for ML & App)
CREATE TABLE IF NOT EXISTS player_features (
    player_id INTEGER,
    gameweek INTEGER,
    season TEXT,
    rolling_points_3 FLOAT,
    rolling_points_5 FLOAT,
    rolling_points_10 FLOAT,
    rolling_xg_3 FLOAT,
    rolling_xa_3 FLOAT,
    h2h_avg_points_vs_opponent FLOAT,
    h2h_avg_xg_vs_opponent FLOAT,
    form_score FLOAT,
    fixture_difficulty INTEGER,
    xi_probability FLOAT,
    PRIMARY KEY (player_id, gameweek, season),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 6. Understat Shots (Granular Shot Data)
CREATE TABLE IF NOT EXISTS understat_shots (
    player_id INTEGER,
    gameweek INTEGER,
    xg FLOAT,
    shot_id INTEGER,
    situation TEXT,
    season TEXT,
    PRIMARY KEY (shot_id),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 7. Understat Player Stats (Aggregated)
CREATE TABLE IF NOT EXISTS understat_player_stats (
    player_id INTEGER,
    gameweek INTEGER,
    avg_shot_xg FLOAT,
    shot_frequency FLOAT,
    conversion_rate FLOAT,
    box_entry_rate FLOAT,
    penalty_involvement INTEGER,
    set_piece_taking INTEGER,
    season TEXT,
    PRIMARY KEY (player_id, gameweek, season),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 8. User Teams (Saved Squads from Optimizer)
CREATE TABLE IF NOT EXISTS user_teams (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id TEXT,
    gameweek INTEGER,
    squad_ids INTEGER[],
    captain_id INTEGER,
    expected_points FLOAT,
    total_cost FLOAT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 9. Predictions (Model Outputs)
CREATE TABLE IF NOT EXISTS predictions (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    player_id INTEGER,
    gameweek INTEGER,
    expected_points FLOAT,
    xi_probability FLOAT,
    combined_score FLOAT,
    is_captain_pick BOOLEAN,
    model_version TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_player_history_gw ON player_history(gameweek);
CREATE INDEX IF NOT EXISTS idx_player_history_player ON player_history(player_id);
CREATE INDEX IF NOT EXISTS idx_player_features_gw ON player_features(gameweek);
CREATE INDEX IF NOT EXISTS idx_predictions_gw ON predictions(gameweek);
CREATE INDEX IF NOT EXISTS idx_predictions_player ON predictions(player_id);
CREATE INDEX IF NOT EXISTS idx_user_teams_gw ON user_teams(gameweek);
