-- 001_create_tables.sql
-- Supabase / Postgres schema for FPL Prediction Pipeline
-- Run this migration to initialize the database schema.

-- Teams (create first — referenced by other tables)
CREATE TABLE IF NOT EXISTS teams (
    team_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    strength_attack DECIMAL(5,2),
    strength_defense DECIMAL(5,2),
    strength_midfield DECIMAL(5,2)
);

-- Players
CREATE TABLE IF NOT EXISTS players (
    player_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    team_id INTEGER REFERENCES teams(team_id),
    position TEXT,
    price DECIMAL(5,2),
    selected_by_percent DECIMAL(5,2),
    status TEXT DEFAULT 'a'
);

-- Fixtures
CREATE TABLE IF NOT EXISTS fixtures (
    fixture_id INTEGER PRIMARY KEY,
    home_team_id INTEGER REFERENCES teams(team_id),
    away_team_id INTEGER REFERENCES teams(team_id),
    gameweek INTEGER,
    date TIMESTAMP,
    is_home_advantage BOOLEAN,
    is_double_gw BOOLEAN DEFAULT FALSE,
    is_blank_gw BOOLEAN DEFAULT FALSE
);

-- Player Stats (per gameweek)
CREATE TABLE IF NOT EXISTS player_stats (
    player_id INTEGER REFERENCES players(player_id),
    fixture_id INTEGER REFERENCES fixtures(fixture_id),
    gameweek INTEGER,
    minutes INTEGER,
    goals INTEGER,
    assists INTEGER,
    clean_sheets INTEGER,
    yellow_cards INTEGER,
    red_cards INTEGER,
    saves INTEGER,
    points INTEGER,
    bps INTEGER,
    xg DECIMAL(10,4),
    xa DECIMAL(10,4),
    xgb DECIMAL(10,4),
    PRIMARY KEY (player_id, fixture_id)
);

-- Team H2H Metrics (last 3 seasons)
CREATE TABLE IF NOT EXISTS team_h2h (
    home_team_id INTEGER REFERENCES teams(team_id),
    away_team_id INTEGER REFERENCES teams(team_id),
    avg_goals_scored DECIMAL(5,2),
    avg_goals_conceded DECIMAL(5,2),
    clean_sheet_rate DECIMAL(5,2),
    last_5_meetings JSONB,
    appearances INTEGER,
    PRIMARY KEY (home_team_id, away_team_id)
);

-- Player vs Team Defense
CREATE TABLE IF NOT EXISTS player_vs_team (
    player_id INTEGER REFERENCES players(player_id),
    opponent_team_id INTEGER REFERENCES teams(team_id),
    avg_points DECIMAL(5,2),
    avg_xg DECIMAL(10,4),
    goals INTEGER,
    appearances INTEGER,
    PRIMARY KEY (player_id, opponent_team_id)
);

-- Predictions
CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    gameweek INTEGER NOT NULL,
    player_id INTEGER REFERENCES players(player_id),
    start_probability DECIMAL(5,2),
    expected_points DECIMAL(5,2),
    expected_goals DECIMAL(5,2),
    expected_assists DECIMAL(5,2),
    clean_sheet_probability DECIMAL(5,2),
    predicted_points INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Match Simulations (aggregated)
CREATE TABLE IF NOT EXISTS match_simulations (
    fixture_id INTEGER PRIMARY KEY REFERENCES fixtures(fixture_id),
    home_win_pct DECIMAL(5,2),
    draw_pct DECIMAL(5,2),
    away_win_pct DECIMAL(5,2),
    home_cs_pct DECIMAL(5,2),
    away_cs_pct DECIMAL(5,2),
    score_distribution JSONB,
    expected_home_goals DECIMAL(5,2),
    expected_away_goals DECIMAL(5,2),
    p10_home_goals INTEGER,
    p90_home_goals INTEGER
);
