-- Add missing columns to Understat tables
-- Current season: 2025-26

-- bronze_understat_player_stats
ALTER TABLE bronze_understat_player_stats ADD COLUMN IF NOT EXISTS league_id text;
ALTER TABLE bronze_understat_player_stats ADD COLUMN IF NOT EXISTS season_id integer;

-- bronze_understat_shots
ALTER TABLE bronze_understat_shots ADD COLUMN IF NOT EXISTS league_id text;
ALTER TABLE bronze_understat_shots ADD COLUMN IF NOT EXISTS season_id integer;
ALTER TABLE bronze_understat_shots ADD COLUMN IF NOT EXISTS shot_id integer;

-- bronze_understat_match_stats  
ALTER TABLE bronze_understat_match_stats ADD COLUMN IF NOT EXISTS season_id integer;

-- Create bronze_understat_player_season_stats if it doesn't exist
CREATE TABLE IF NOT EXISTS bronze_understat_player_season_stats (
    player_id integer NOT NULL,
    player_name text,
    team_id integer,
    team_name text,
    games_played integer,
    minutes integer,
    goals integer,
    assists integer,
    shots integer,
    np_xg double precision,
    xg double precision,
    xa double precision,
    key_passes integer,
    np_goals integer,
    xg_buildup double precision,
    xg_chain double precision,
    yellow_cards integer,
    red_cards integer,
    position text,
    season text NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (player_id, team_id, season)
);

CREATE INDEX IF NOT EXISTS idx_player_season_stats_season ON bronze_understat_player_season_stats(season);
CREATE INDEX IF NOT EXISTS idx_player_season_stats_team ON bronze_understat_player_season_stats(team_id);
