-- Add remaining missing columns to Understat tables
-- Run in Supabase SQL Editor

-- bronze_understat_player_stats
ALTER TABLE bronze_understat_player_stats ADD COLUMN IF NOT EXISTS league_id text;
ALTER TABLE bronze_understat_player_stats ADD COLUMN IF NOT EXISTS season_id integer;

-- bronze_understat_shots
ALTER TABLE bronze_understat_shots ADD COLUMN IF NOT EXISTS league_id text;
ALTER TABLE bronze_understat_shots ADD COLUMN IF NOT EXISTS season_id integer;
ALTER TABLE bronze_understat_shots ADD COLUMN IF NOT EXISTS shot_id integer;

-- bronze_understat_match_stats
ALTER TABLE bronze_understat_match_stats ADD COLUMN IF NOT EXISTS season_id integer;

-- bronze_understat_player_season_stats (create if needed)
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
