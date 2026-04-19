-- Create silver_team_mapping table
CREATE TABLE IF NOT EXISTS silver_team_mapping (
    unified_team_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season VARCHAR(10) NOT NULL,
    fpl_team_id INTEGER,
    fpl_team_name VARCHAR(100),
    vaastav_team_name VARCHAR(100),
    understat_team_id INTEGER,
    understat_team_name VARCHAR(100),
    team_code VARCHAR(3),
    source VARCHAR(20) DEFAULT 'exact',
    confidence_score FLOAT DEFAULT 1.0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(season, fpl_team_id)
);

-- Index for efficient lookups
CREATE INDEX IF NOT EXISTS idx_silver_team_season ON silver_team_mapping(season);

-- Comment
COMMENT ON TABLE silver_team_mapping IS 'Unified team identity mapping across FPL, Vaastav, and Understat sources';
