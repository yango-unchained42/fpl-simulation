-- Unified Silver GW Table
-- Combines FPL (2025-26) and Vaastav (2021-22 to 2023-24) GW data
-- Column names follow Vaastav convention (player_id, gameweek)
-- Extra stats from FPL (clearances, tackles, etc.) included

CREATE TABLE IF NOT EXISTS silver_gw (
    -- Key identifiers
    player_id INTEGER NOT NULL,
    gameweek INTEGER NOT NULL,
    season TEXT NOT NULL,
    source TEXT NOT NULL,  -- 'fpl' or 'vaastav'
    
    -- Core stats (common to both)
    total_points INTEGER,
    minutes INTEGER,
    goals_scored INTEGER,
    assists INTEGER,
    clean_sheets INTEGER,
    goals_conceded INTEGER,
    expected_goals REAL,
    expected_assists REAL,
    expected_goal_involvements REAL,
    expected_goals_conceded REAL,
    
    -- Bonus points
    bonus INTEGER,
    bps INTEGER,
    
    -- ICT indices
    influence REAL,
    creativity REAL,
    threat REAL,
    ict_index REAL,
    
    -- Other stats
    own_goals INTEGER,
    penalties_saved INTEGER,
    penalties_missed INTEGER,
    yellow_cards INTEGER,
    red_cards INTEGER,
    saves INTEGER,
    starts INTEGER,
    
    -- Match context
    was_home BOOLEAN,
    opponent_team TEXT,
    fixture INTEGER,
    kickoff_time TEXT,
    team_a_score INTEGER,
    team_h_score INTEGER,
    
    -- Ownership/transfer data
    value INTEGER,
    selected INTEGER,
    transfers_in INTEGER,
    transfers_out INTEGER,
    
    -- FPL-only extra stats (NULL for vaastav)
    clearances_blocks_interceptions INTEGER,
    recoveries INTEGER,
    tackles INTEGER,
    defensive_contribution REAL,
    transfers_balance INTEGER,
    
    -- Data quality flags
    data_quality_score REAL DEFAULT 1.0,
    is_incomplete BOOLEAN DEFAULT FALSE,
    missing_fields TEXT[],
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    PRIMARY KEY (player_id, gameweek, season, source)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_silver_gw_season ON silver_gw(season);
CREATE INDEX IF NOT EXISTS idx_silver_gw_gameweek ON silver_gw(gameweek);
CREATE INDEX IF NOT EXISTS idx_silver_gw_player_id ON silver_gw(player_id);
CREATE INDEX IF NOT EXISTS idx_silver_gw_source ON silver_gw(source);
CREATE INDEX IF NOT EXISTS idx_silver_gw_opponent ON silver_gw(opponent_team) WHERE opponent_team IS NOT NULL;