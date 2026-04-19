-- Drop the full silver_vaastav_players table (too large, not needed)
-- Create a minimal silver_vaastav_players_metadata table for reference only

DROP TABLE IF EXISTS silver_vaastav_players;

CREATE TABLE IF NOT EXISTS silver_vaastav_players_metadata (
    id INTEGER NOT NULL,
    web_name TEXT,
    first_name TEXT,
    second_name TEXT,
    team INTEGER,
    element_type INTEGER,
    season TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    PRIMARY KEY (id, season)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_silver_vaastav_players_metadata_season ON silver_vaastav_players_metadata(season);
CREATE INDEX IF NOT EXISTS idx_silver_vaastav_players_metadata_team ON silver_vaastav_players_metadata(team) WHERE team IS NOT NULL;

-- Create silver_player_state table for tracking dynamic fields per GW
CREATE TABLE IF NOT EXISTS silver_player_state (
    id INTEGER NOT NULL,
    season TEXT NOT NULL,
    gameweek INTEGER NOT NULL,
    web_name TEXT,
    team INTEGER,
    element_type INTEGER,
    -- Dynamic fields that change each GW
    now_cost INTEGER,
    chance_of_playing_next_round INTEGER,
    chance_of_playing_this_round INTEGER,
    news TEXT,
    status TEXT,
    in_dreamteam BOOLEAN,
    removed BOOLEAN,
    special BOOLEAN,
    corners_and_indirect_freekicks_order INTEGER,
    direct_freekicks_order INTEGER,
    penalties_order INTEGER,
    form TEXT,
    selected_by_percent TEXT,
    transfers_in INTEGER,
    transfers_out INTEGER,
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    PRIMARY KEY (id, season, gameweek)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_silver_player_state_season_gw ON silver_player_state(season, gameweek);
CREATE INDEX IF NOT EXISTS idx_silver_player_state_team ON silver_player_state(team) WHERE team IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_silver_player_state_status ON silver_player_state(status) WHERE status IS NOT NULL;
