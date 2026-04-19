-- Silver: Vaastav GW Data (Consolidated from bronze_vaastav_player_history_gw)
-- Consolidates all seasons of player GW data with data quality flags

CREATE TABLE IF NOT EXISTS silver_vaastav_gw (
    unified_player_id UUID,
    player_id INTEGER NOT NULL,
    gameweek INTEGER NOT NULL,
    team TEXT,
    minutes INTEGER,
    goals_scored INTEGER,
    assists INTEGER,
    clean_sheets INTEGER,
    goals_conceded INTEGER,
    expected_goals FLOAT,
    expected_assists FLOAT,
    total_points INTEGER,
    was_home BOOLEAN,
    opponent_team TEXT,
    season TEXT NOT NULL,
    name TEXT,
    position TEXT,
    bonus INTEGER,
    bps INTEGER,
    saves INTEGER,
    starts INTEGER,
    own_goals INTEGER,
    penalties_missed INTEGER,
    penalties_saved INTEGER,
    yellow_cards INTEGER,
    red_cards INTEGER,
    creativity INTEGER,
    influence INTEGER,
    threat INTEGER,
    ict_index INTEGER,
    expected_goal_involvements FLOAT,
    expected_goals_conceded FLOAT,
    transfers_in INTEGER,
    transfers_out INTEGER,
    value INTEGER,
    selected INTEGER,
    fixture INTEGER,
    kickoff_time TEXT,
    team_a_score INTEGER,
    team_h_score INTEGER,
    xp FLOAT,
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
CREATE INDEX IF NOT EXISTS idx_silver_vaastav_gw_season ON silver_vaastav_gw(season);
CREATE INDEX IF NOT EXISTS idx_silver_vaastav_gw_player_id ON silver_vaastav_gw(player_id);
CREATE INDEX IF NOT EXISTS idx_silver_vaastav_gw_gameweek ON silver_vaastav_gw(gameweek);
CREATE INDEX IF NOT EXISTS idx_silver_vaastav_gw_team ON silver_vaastav_gw(team) WHERE team IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_silver_vaastav_gw_opponent ON silver_vaastav_gw(opponent_team) WHERE opponent_team IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_silver_vaastav_gw_unified ON silver_vaastav_gw(unified_player_id) WHERE unified_player_id IS NOT NULL;
