-- Silver FPL Fantasy Stats
-- Per-GW fantasy ownership data (value, selected, transfers)
-- Plus player state: now_cost, chance_of_playing, status, news, etc.

CREATE TABLE IF NOT EXISTS silver_fpl_fantasy_stats (
    -- Key identifiers
    player_id INTEGER NOT NULL,
    season TEXT NOT NULL,
    gameweek INTEGER NOT NULL,
    
    -- Fantasy ownership data (from GW)
    value INTEGER,
    selected INTEGER,
    transfers_in INTEGER,
    transfers_out INTEGER,
    
    -- Player state (from player state / snapshot)
    now_cost INTEGER,
    chance_of_playing_next_round INTEGER,
    chance_of_playing_this_round INTEGER,
    news TEXT,
    status TEXT,
    form TEXT,
    selected_by_percent TEXT,
    in_dreamteam BOOLEAN,
    removed BOOLEAN,
    special BOOLEAN,
    corners_and_indirect_freekicks_order INTEGER,
    direct_freekicks_order INTEGER,
    penalties_order INTEGER,
    
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
CREATE INDEX IF NOT EXISTS idx_silver_fpl_fantasy_stats_season ON silver_fpl_fantasy_stats(season);
CREATE INDEX IF NOT EXISTS idx_silver_fpl_fantasy_stats_gameweek ON silver_fpl_fantasy_stats(gameweek);
CREATE INDEX IF NOT EXISTS idx_silver_fpl_fantasy_stats_player_id ON silver_fpl_fantasy_stats(player_id);


-- Silver FPL Player Stats
-- Per-GW match performance data only (no fantasy/ownership columns)

CREATE TABLE IF NOT EXISTS silver_fpl_player_stats (
    -- Key identifiers
    player_id INTEGER NOT NULL,
    season TEXT NOT NULL,
    gameweek INTEGER NOT NULL,
    source TEXT NOT NULL,  -- 'fpl' or 'vaastav'
    
    -- Match stats
    total_points INTEGER,
    minutes INTEGER,
    goals_scored INTEGER,
    assists INTEGER,
    clean_sheets INTEGER,
    goals_conceded INTEGER,
    
    -- Expected stats
    expected_goals REAL,
    expected_assists REAL,
    expected_goal_involvements REAL,
    expected_goals_conceded REAL,
    
    -- Bonus/bps
    bonus INTEGER,
    bps INTEGER,
    
    -- ICT
    influence REAL,
    creativity REAL,
    threat REAL,
    ict_index REAL,
    
    -- Other match stats
    own_goals INTEGER,
    penalties_saved INTEGER,
    penalties_missed INTEGER,
    yellow_cards INTEGER,
    red_cards INTEGER,
    saves INTEGER,
    starts INTEGER,
    
    -- FPL-only (NULL for vaastav)
    clearances_blocks_interceptions INTEGER,
    recoveries INTEGER,
    tackles INTEGER,
    defensive_contribution REAL,
    
    -- Match context
    was_home BOOLEAN,
    opponent_team TEXT,
    fixture INTEGER,
    kickoff_time TEXT,
    team_a_score INTEGER,
    team_h_score INTEGER,
    
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
CREATE INDEX IF NOT EXISTS idx_silver_fpl_player_stats_season ON silver_fpl_player_stats(season);
CREATE INDEX IF NOT EXISTS idx_silver_fpl_player_stats_gameweek ON silver_fpl_player_stats(gameweek);
CREATE INDEX IF NOT EXISTS idx_silver_fpl_player_stats_player_id ON silver_fpl_player_stats(player_id);
CREATE INDEX IF NOT EXISTS idx_silver_fpl_player_stats_source ON silver_fpl_player_stats(source);