-- Migration: 020_create_unified_player_stats
-- Creates silver_unified_player_stats table combining FPL and Understat data

-- Silver Unified Player Stats
-- Combines: silver_fpl_player_stats + silver_understat_player_stats
-- Primary Key: (player_id, season, gameweek)

CREATE TABLE IF NOT EXISTS silver_unified_player_stats (
    -- Key identifiers (primary key components)
    player_id INTEGER NOT NULL,
    season TEXT NOT NULL,
    gameweek INTEGER NOT NULL,
    
    -- Team/Position context
    team_id INTEGER,
    position TEXT,
    position_id INTEGER,
    game_id INTEGER,
    
    -- Core match stats (from FPL - official stats)
    total_points INTEGER,
    goals_scored INTEGER,
    assists INTEGER,
    clean_sheets INTEGER,
    goals_conceded INTEGER,
    starts INTEGER,
    
    -- Minutes (from Understat - more accurate for subs)
    minutes INTEGER,
    
    -- Expected stats (from Understat - superior methodology)
    xg REAL,
    xa REAL,
    xg_chain REAL,
    xg_buildup REAL,
    
    -- FPL expected stats (fallback/backup)
    expected_goals REAL,
    expected_assists REAL,
    expected_goal_involvements REAL,
    expected_goals_conceded REAL,
    
    -- Shot/Creative stats (from Understat)
    shots INTEGER,
    key_passes INTEGER,
    
    -- Discipline (from FPL - official data)
    yellow_cards INTEGER,
    red_cards INTEGER,
    own_goals INTEGER,
    penalties_saved INTEGER,
    penalties_missed INTEGER,
    
    -- Bonus/BPS (FPL-only)
    bonus INTEGER,
    bps INTEGER,
    
    -- ICT Index (FPL-only)
    influence REAL,
    creativity REAL,
    threat REAL,
    ict_index REAL,
    
    -- Defensive stats (FPL-only)
    tackles INTEGER,
    clearances_blocks_interceptions INTEGER,
    recoveries INTEGER,
    defensive_contribution REAL,
    saves INTEGER,
    
    -- Match context (IDs only - NO names!)
    was_home BOOLEAN,
    opponent_team_id INTEGER,
    fixture_id INTEGER,
    kickoff_time TEXT,
    home_score INTEGER,
    away_score INTEGER,
    
    -- Data quality flags
    data_quality_score REAL DEFAULT 1.0,
    is_incomplete BOOLEAN DEFAULT FALSE,
    missing_fields TEXT[],
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    PRIMARY KEY (player_id, season, gameweek)
);

-- Indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_unified_player_stats_player_id ON silver_unified_player_stats(player_id);
CREATE INDEX IF NOT EXISTS idx_unified_player_stats_season ON silver_unified_player_stats(season);
CREATE INDEX IF NOT EXISTS idx_unified_player_stats_gameweek ON silver_unified_player_stats(gameweek);
CREATE INDEX IF NOT EXISTS idx_unified_player_stats_team_id ON silver_unified_player_stats(team_id);
CREATE INDEX IF NOT EXISTS idx_unified_player_stats_position ON silver_unified_player_stats(position);
CREATE INDEX IF NOT EXISTS idx_unified_player_stats_opponent_team ON silver_unified_player_stats(opponent_team_id);
CREATE INDEX IF NOT EXISTS idx_unified_player_stats_season_gw ON silver_unified_player_stats(season, gameweek);

-- Comments for documentation
COMMENT ON TABLE silver_unified_player_stats IS 'Unified player stats combining FPL and Understat data. Primary source: FPL for official stats, Understat for xG/xA/advanced metrics.';
COMMENT ON COLUMN silver_unified_player_stats.xg IS 'Expected goals from Understat (primary)';
COMMENT ON COLUMN silver_unified_player_stats.xa IS 'Expected assists from Understat (primary)';
COMMENT ON COLUMN silver_unified_player_stats.minutes IS 'Minutes played from Understat (more accurate for sub time)';
COMMENT ON COLUMN silver_unified_player_stats.opponent_team_id IS 'Opponent team ID (replaces opponent_team TEXT)';