-- Silver Understat Player Stats
CREATE TABLE IF NOT EXISTS silver_understat_player_stats (
    player_id INTEGER NOT NULL,
    gameweek INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    team_id INTEGER,
    position TEXT,
    position_id INTEGER,
    minutes INTEGER,
    goals INTEGER,
    assists INTEGER,
    shots INTEGER,
    xg REAL,
    xa REAL,
    xg_chain REAL,
    xg_buildup REAL,
    key_passes INTEGER,
    own_goals INTEGER,
    yellow_cards INTEGER,
    red_cards INTEGER,
    season TEXT NOT NULL,
    league_id INTEGER,
    season_id INTEGER,
    
    -- Data quality flags
    data_quality_score REAL DEFAULT 1.0,
    is_incomplete BOOLEAN DEFAULT FALSE,
    missing_fields TEXT[],
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    PRIMARY KEY (player_id, gameweek, season)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_silver_understat_player_stats_season ON silver_understat_player_stats(season);
CREATE INDEX IF NOT EXISTS idx_silver_understat_player_stats_gameweek ON silver_understat_player_stats(gameweek);
CREATE INDEX IF NOT EXISTS idx_silver_understat_player_stats_player_id ON silver_understat_player_stats(player_id);
CREATE INDEX IF NOT EXISTS idx_silver_understat_player_stats_team_id ON silver_understat_player_stats(team_id);


-- Silver Understat Match Stats
CREATE TABLE IF NOT EXISTS silver_understat_match_stats (
    game_id INTEGER NOT NULL,
    date DATE NOT NULL,
    season TEXT NOT NULL,
    home_team_id INTEGER,
    away_team_id INTEGER,
    home_team TEXT,
    away_team TEXT,
    home_goals INTEGER,
    away_goals INTEGER,
    home_xg REAL,
    away_xg REAL,
    home_np_xg REAL,
    away_np_xg REAL,
    home_np_xg_difference REAL,
    away_np_xg_difference REAL,
    home_ppda REAL,
    away_ppda REAL,
    home_deep_completions INTEGER,
    away_deep_completions INTEGER,
    home_expected_points REAL,
    away_expected_points REAL,
    home_points INTEGER,
    away_points INTEGER,
    away_team_code TEXT,
    home_team_code TEXT,
    league_id INTEGER,
    season_id INTEGER,
    
    -- Data quality flags
    data_quality_score REAL DEFAULT 1.0,
    is_incomplete BOOLEAN DEFAULT FALSE,
    missing_fields TEXT[],
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    PRIMARY KEY (game_id, season)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_silver_understat_match_stats_season ON silver_understat_match_stats(season);
CREATE INDEX IF NOT EXISTS idx_silver_understat_match_stats_date ON silver_understat_match_stats(date);
CREATE INDEX IF NOT EXISTS idx_silver_understat_match_stats_home_team ON silver_understat_match_stats(home_team_id);
CREATE INDEX IF NOT EXISTS idx_silver_understat_match_stats_away_team ON silver_understat_match_stats(away_team_id);


-- Silver Understat Shots (summary table, not raw shots)
CREATE TABLE IF NOT EXISTS silver_understat_shots_summary (
    player_id INTEGER NOT NULL,
    season TEXT NOT NULL,
    gameweek INTEGER NOT NULL,
    team_id INTEGER,
    total_shots INTEGER,
    shots_on_target INTEGER,
    goals INTEGER,
    xg REAL,
    avg_location_x REAL,
    avg_location_y REAL,
    
    -- Breakdown by situation
    shots_open_play INTEGER,
    shots_from_freekick INTEGER,
    shots_from_corners INTEGER,
    shots_from_penalty INTEGER,
    
    -- Breakdown by body part
    shots_right_foot INTEGER,
    shots_left_foot INTEGER,
    shots_head INTEGER,
    shots_other INTEGER,
    
    -- Data quality flags
    data_quality_score REAL DEFAULT 1.0,
    is_incomplete BOOLEAN DEFAULT FALSE,
    missing_fields TEXT[],
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    PRIMARY KEY (player_id, season, gameweek)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_silver_understat_shots_summary_season ON silver_understat_shots_summary(season);
CREATE INDEX IF NOT EXISTS idx_silver_understat_shots_summary_player_id ON silver_understat_shots_summary(player_id);