-- Silver Layer: Player Mapping Table
-- Maps player IDs across FPL, Vaastav, and Understat per season

CREATE TABLE IF NOT EXISTS silver_player_mapping (
    unified_player_id UUID DEFAULT gen_random_uuid() NOT NULL,
    season TEXT NOT NULL,
    fpl_id INTEGER,
    vaastav_id INTEGER,
    understat_id INTEGER,
    player_name TEXT NOT NULL,
    position TEXT,
    team TEXT,
    source TEXT DEFAULT 'fuzzy',
    confidence_score REAL DEFAULT 0.0,
    requires_manual_review BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    PRIMARY KEY (unified_player_id, season),

    -- Constraints
    CHECK (confidence_score >= 0.0 AND confidence_score <= 1.0),
    CHECK (source IN ('exact', 'fuzzy', 'manual'))
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_silver_player_mapping_season ON silver_player_mapping(season);
CREATE INDEX IF NOT EXISTS idx_silver_player_mapping_fpl_id ON silver_player_mapping(fpl_id) WHERE fpl_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_silver_player_mapping_vaastav_id ON silver_player_mapping(vaastav_id) WHERE vaastav_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_silver_player_mapping_understat_id ON silver_player_mapping(understat_id) WHERE understat_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_silver_player_mapping_player_name ON silver_player_mapping(player_name);
CREATE INDEX IF NOT EXISTS idx_silver_player_mapping_team ON silver_player_mapping(team) WHERE team IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_silver_player_mapping_manual_review ON silver_player_mapping(requires_manual_review) WHERE requires_manual_review = TRUE;
