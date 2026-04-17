-- Create Vaastav fixtures table for Bronze layer
-- Matching FPL fixtures schema where applicable
-- Note: Vaastav uses team names (TEXT) instead of team IDs

CREATE TABLE IF NOT EXISTS bronze_vaastav_fixtures (
    id INTEGER NOT NULL,                    -- Maps to FPL fixture id
    event INTEGER,                          -- Maps to FPL event (gameweek)
    kickoff_time TIMESTAMP,
    team_h TEXT NOT NULL,                   -- Home team name
    team_a TEXT NOT NULL,                   -- Away team name
    team_h_score INTEGER,                  -- Home goals
    team_a_score INTEGER,                   -- Away goals
    finished BOOLEAN DEFAULT TRUE,          -- Vaastav only has past matches
    started BOOLEAN DEFAULT TRUE,            -- Vaastav only has started matches
    season TEXT NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- Primary key
    PRIMARY KEY (id, season)
);

-- Add indexes for common queries
CREATE INDEX IF NOT EXISTS idx_vaastav_fixtures_season ON bronze_vaastav_fixtures(season);
CREATE INDEX IF NOT EXISTS idx_vaastav_fixtures_event ON bronze_vaastav_fixtures(season, event);
CREATE INDEX IF NOT EXISTS idx_vaastav_fixtures_team_h ON bronze_vaastav_fixtures(team_h);
CREATE INDEX IF NOT EXISTS idx_vaastav_fixtures_team_a ON bronze_vaastav_fixtures(team_a);
