-- Unified Silver Fixtures Table
-- Combines FPL and Vaastav fixtures

CREATE TABLE IF NOT EXISTS silver_fixtures (
    -- Key identifiers
    id INTEGER NOT NULL,
    season TEXT NOT NULL,
    event INTEGER,  -- gameweek
    source TEXT NOT NULL,  -- 'fpl' or 'vaastav'
    
    -- Team identifiers
    team_h INTEGER,
    team_a INTEGER,
    
    -- Match status
    finished BOOLEAN,
    started BOOLEAN,
    
    -- Score
    team_h_score INTEGER,
    team_a_score INTEGER,
    
    -- Timing
    kickoff_time TIMESTAMP WITH TIME ZONE,
    
    -- FPL-specific (NULL for vaastav)
    team_h_difficulty INTEGER,
    team_a_difficulty INTEGER,
    pulse_id INTEGER,
    code INTEGER,
    
    -- Data quality flags
    data_quality_score REAL DEFAULT 1.0,
    is_incomplete BOOLEAN DEFAULT FALSE,
    missing_fields TEXT[],
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    PRIMARY KEY (id, season, source)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_silver_fixtures_season ON silver_fixtures(season);
CREATE INDEX IF NOT EXISTS idx_silver_fixtures_event ON silver_fixtures(event);
CREATE INDEX IF NOT EXISTS idx_silver_fixtures_source ON silver_fixtures(source);
CREATE INDEX IF NOT EXISTS idx_silver_fixtures_team_h ON silver_fixtures(team_h);
CREATE INDEX IF NOT EXISTS idx_silver_fixtures_team_a ON silver_fixtures(team_a);