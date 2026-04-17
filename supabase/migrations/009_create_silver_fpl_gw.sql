-- Silver: FPL GW Data (Consolidated from bronze_fpl_gw)
-- Consolidates current season GW data with data quality flags

CREATE TABLE IF NOT EXISTS silver_fpl_gw (
    element INTEGER NOT NULL,
    fixture INTEGER,
    opponent_team TEXT,
    total_points INTEGER,
    was_home BOOLEAN,
    kickoff_time TEXT,
    team_h_score INTEGER,
    team_a_score INTEGER,
    round INTEGER NOT NULL,
    modified BOOLEAN,
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
    influence REAL,
    creativity REAL,
    threat REAL,
    ict_index REAL,
    clearances_blocks_interceptions INTEGER,
    recoveries INTEGER,
    tackles INTEGER,
    defensive_contribution REAL,
    starts INTEGER,
    expected_goals REAL,
    expected_assists REAL,
    expected_goal_involvements REAL,
    expected_goals_conceded REAL,
    value INTEGER,
    transfers_balance INTEGER,
    selected INTEGER,
    transfers_in INTEGER,
    transfers_out INTEGER,
    player_id INTEGER,
    season TEXT NOT NULL,
    -- Data quality flags
    data_quality_score REAL DEFAULT 1.0,
    is_incomplete BOOLEAN DEFAULT FALSE,
    missing_fields TEXT[],
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    PRIMARY KEY (element, round, season)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_silver_fpl_gw_season ON silver_fpl_gw(season);
CREATE INDEX IF NOT EXISTS idx_silver_fpl_gw_round ON silver_fpl_gw(round);
CREATE INDEX IF NOT EXISTS idx_silver_fpl_gw_player_id ON silver_fpl_gw(player_id) WHERE player_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_silver_fpl_gw_opponent ON silver_fpl_gw(opponent_team) WHERE opponent_team IS NOT NULL;