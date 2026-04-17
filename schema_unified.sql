-- =============================================================================
-- FPL Simulation — Unified Database Schema
-- =============================================================================
--
-- Medallion Architecture:
--   BRONZE  = raw data from external sources (current season only)
--   SILVER  = cleaned, standardized, UUID-keyed data (all seasons)
--
-- Run this in your Supabase SQL Editor to create or recreate all tables.
-- For incremental changes, use supabase/migrations/ instead.
-- =============================================================================


-- =============================================================================
-- BRONZE LAYER (Raw Data — current season, overwritten daily)
-- =============================================================================

-- FPL Players (from bootstrap-static API)
CREATE TABLE IF NOT EXISTS bronze_fpl_players (
    id              INTEGER PRIMARY KEY,
    web_name        TEXT,
    first_name      TEXT,
    second_name     TEXT,
    known_name      TEXT,
    team            INTEGER,
    element_type    INTEGER,
    now_cost        INTEGER,
    total_points    INTEGER,
    selected_by_percent TEXT,
    status          TEXT,
    code            INTEGER,
    photo           TEXT,
    points_per_game TEXT,
    form            TEXT,
    value_season    TEXT,
    value_form      TEXT,
    transfers_in    INTEGER,
    transfers_out   INTEGER,
    transfers_in_event  INTEGER,
    transfers_out_event INTEGER,
    news            TEXT,
    chance_of_playing_next_round INTEGER,
    chance_of_playing_this_round INTEGER,
    cost_change_event       INTEGER,
    cost_change_event_fall  INTEGER,
    cost_change_start       INTEGER,
    cost_change_start_fall  INTEGER,
    dreamteam_count INTEGER,
    ep_next         TEXT,
    ep_this         TEXT,
    event_points    INTEGER,
    in_dreamteam    BOOLEAN,
    removed         BOOLEAN,
    special         BOOLEAN,
    squad_number    INTEGER,
    team_code       INTEGER,
    minutes         INTEGER,
    goals_scored    INTEGER,
    assists         INTEGER,
    clean_sheets    INTEGER,
    goals_conceded  INTEGER,
    own_goals       INTEGER,
    penalties_saved INTEGER,
    penalties_missed INTEGER,
    yellow_cards    INTEGER,
    red_cards       INTEGER,
    saves           INTEGER,
    bonus           INTEGER,
    bps             INTEGER,
    influence       TEXT,
    creativity      TEXT,
    threat          TEXT,
    ict_index       TEXT,
    clearances_blocks_interceptions INTEGER,
    recoveries      INTEGER,
    tackles         INTEGER,
    defensive_contribution INTEGER,
    starts          INTEGER,
    expected_goals              TEXT,
    expected_assists            TEXT,
    expected_goal_involvements  TEXT,
    expected_goals_conceded     TEXT,
    season          TEXT NOT NULL,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- FPL Teams (from bootstrap-static API)
CREATE TABLE IF NOT EXISTS bronze_fpl_teams (
    id                      INTEGER PRIMARY KEY,
    name                    TEXT,
    short_name              TEXT,
    code                    INTEGER,
    strength                INTEGER,
    strength_overall_home   INTEGER,
    strength_overall_away   INTEGER,
    strength_attack_home    INTEGER,
    strength_attack_away    INTEGER,
    strength_defence_home   INTEGER,
    strength_defence_away   INTEGER,
    pulse_id                INTEGER,
    season                  TEXT NOT NULL,
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);

-- FPL Fixtures (from fixtures API)
CREATE TABLE IF NOT EXISTS bronze_fpl_fixtures (
    id              INTEGER PRIMARY KEY,
    event           INTEGER,
    team_h          INTEGER,
    team_a          INTEGER,
    team_h_score    INTEGER,
    team_a_score    INTEGER,
    finished        BOOLEAN,
    started         BOOLEAN,
    kickoff_time    TEXT,
    minutes         INTEGER,
    provisional     BOOLEAN,
    season          TEXT NOT NULL,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- FPL Gameweek history (from element-summary API)
CREATE TABLE IF NOT EXISTS bronze_fpl_gw (
    element             INTEGER,
    fixture             INTEGER,
    opponent_team       INTEGER,
    total_points        INTEGER,
    was_home            BOOLEAN,
    kickoff_time        TEXT,
    team_h_score        INTEGER,
    team_a_score        INTEGER,
    round               INTEGER,
    minutes             INTEGER,
    goals_scored        INTEGER,
    assists             INTEGER,
    clean_sheets        INTEGER,
    goals_conceded      INTEGER,
    own_goals           INTEGER,
    penalties_saved     INTEGER,
    penalties_missed    INTEGER,
    yellow_cards        INTEGER,
    red_cards           INTEGER,
    saves               INTEGER,
    bonus               INTEGER,
    bps                 INTEGER,
    influence           TEXT,
    creativity          TEXT,
    threat              TEXT,
    ict_index           TEXT,
    starts              INTEGER,
    expected_goals              TEXT,
    expected_assists            TEXT,
    expected_goal_involvements  TEXT,
    expected_goals_conceded     TEXT,
    season              TEXT NOT NULL,
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (element, fixture)
);

-- Understat player match stats (xG, xA, shots per player-game)
CREATE TABLE IF NOT EXISTS bronze_understat_player_stats (
    player_id       INTEGER NOT NULL,
    game_id         INTEGER NOT NULL,
    position        TEXT,
    position_id     TEXT,
    team_id         TEXT,
    team_name       TEXT,
    h_team          TEXT,
    a_team          TEXT,
    h_goals         INTEGER,
    a_goals         INTEGER,
    date            TEXT,
    player          TEXT,
    minutes         INTEGER,
    goals           INTEGER,
    xg              REAL,
    assists         INTEGER,
    xa              REAL,
    shots           INTEGER,
    key_passes      INTEGER,
    yellow_cards    INTEGER,
    red_cards       INTEGER,
    roster_id       INTEGER,
    season          TEXT NOT NULL,
    league_id       TEXT,
    season_id       TEXT,
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (player_id, game_id)
);

-- Understat shots (individual shot events)
CREATE TABLE IF NOT EXISTS bronze_understat_shots (
    id              TEXT PRIMARY KEY,
    minute          INTEGER,
    result          TEXT,
    x               REAL,
    y               REAL,
    xg              REAL,
    player          TEXT,
    player_id       INTEGER,
    situation       TEXT,
    season          TEXT,
    shot_type       TEXT,
    game_id         INTEGER,
    home_team       TEXT,
    away_team       TEXT,
    h_goals         INTEGER,
    a_goals         INTEGER,
    date            TEXT,
    league_id       TEXT,
    season_id       TEXT,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Understat match stats (team-level xG per match)
CREATE TABLE IF NOT EXISTS bronze_understat_match_stats (
    game_id         INTEGER PRIMARY KEY,
    home_team       TEXT,
    away_team       TEXT,
    home_id         INTEGER,
    away_id         INTEGER,
    h_goals         INTEGER,
    a_goals         INTEGER,
    date            TEXT,
    season          TEXT NOT NULL,
    league_id       TEXT,
    season_id       TEXT,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Vaastav gameweek data (historical FPL data 2021-25)
CREATE TABLE IF NOT EXISTS bronze_vaastav_player_history_gw (
    element         INTEGER,
    fixture         INTEGER,
    opponent_team   INTEGER,
    total_points    INTEGER,
    was_home        BOOLEAN,
    kickoff_time    TEXT,
    team_h_score    INTEGER,
    team_a_score    INTEGER,
    round           INTEGER,
    minutes         INTEGER,
    goals_scored    INTEGER,
    assists         INTEGER,
    clean_sheets    INTEGER,
    goals_conceded  INTEGER,
    own_goals       INTEGER,
    penalties_saved INTEGER,
    penalties_missed INTEGER,
    yellow_cards    INTEGER,
    red_cards       INTEGER,
    saves           INTEGER,
    bonus           INTEGER,
    bps             INTEGER,
    influence       TEXT,
    creativity      TEXT,
    threat          TEXT,
    ict_index       TEXT,
    name            TEXT,
    position        TEXT,
    team            TEXT,
    season          TEXT NOT NULL,
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (element, fixture)
);

-- Understat player name → team mapping (derived from player_season_stats)
CREATE TABLE IF NOT EXISTS bronze_understat_player_mappings (
    understat_player_id     INTEGER NOT NULL,
    understat_player_name   TEXT,
    understat_team_id       TEXT,
    understat_team_name     TEXT,
    season                  TEXT NOT NULL,
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (understat_player_id, season)
);

-- Metadata table (tracks last update per source/season)
CREATE TABLE IF NOT EXISTS metadata (
    table_name      TEXT NOT NULL,
    season          TEXT NOT NULL,
    source_hash     TEXT,
    row_count       INTEGER,
    last_updated    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (table_name, season)
);


-- =============================================================================
-- SILVER LAYER (Cleaned, UUID-keyed, multi-season)
-- =============================================================================

-- Team mapping: unified team UUIDs across FPL/Vaastav/Understat
CREATE TABLE IF NOT EXISTS silver_team_mapping (
    unified_team_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season              TEXT NOT NULL,
    fpl_team_id         INTEGER,
    fpl_team_name       TEXT,
    vaastav_team_name   TEXT,
    understat_team_id   TEXT,
    understat_team_name TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (season, fpl_team_id)
);

-- Player mapping: unified player UUIDs across FPL/Vaastav/Understat
CREATE TABLE IF NOT EXISTS silver_player_mapping (
    unified_player_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season              TEXT NOT NULL,
    fpl_id              INTEGER,
    vaastav_id          INTEGER,
    understat_id        INTEGER,
    player_name         TEXT,
    team                TEXT,
    position            TEXT,
    match_confidence    REAL,
    match_type          TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (season, fpl_id)
);

-- Match mapping: unified match/fixture UUIDs
CREATE TABLE IF NOT EXISTS silver_match_mapping (
    match_id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season                  TEXT NOT NULL,
    gameweek                INTEGER,
    home_unified_team_id    UUID REFERENCES silver_team_mapping(unified_team_id),
    away_unified_team_id    UUID REFERENCES silver_team_mapping(unified_team_id),
    fpl_fixture_id          INTEGER,
    understat_game_id       INTEGER,
    kickoff_time            TEXT,
    home_score              INTEGER,
    away_score              INTEGER,
    finished                BOOLEAN DEFAULT FALSE,
    source                  TEXT,  -- 'fpl', 'understat', or 'both'
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);

-- Silver FPL player stats (per-player, per-match performance)
CREATE TABLE IF NOT EXISTS silver_fpl_player_stats (
    id                          BIGSERIAL PRIMARY KEY,
    unified_player_id           UUID REFERENCES silver_player_mapping(unified_player_id),
    match_id                    UUID REFERENCES silver_match_mapping(match_id),
    season                      TEXT NOT NULL,
    gameweek                    INTEGER,
    total_points                INTEGER,
    minutes                     INTEGER,
    goals_scored                INTEGER,
    assists                     INTEGER,
    clean_sheets                INTEGER,
    goals_conceded              INTEGER,
    own_goals                   INTEGER,
    penalties_saved             INTEGER,
    penalties_missed            INTEGER,
    yellow_cards                INTEGER,
    red_cards                   INTEGER,
    saves                       INTEGER,
    bonus                       INTEGER,
    bps                         INTEGER,
    influence                   REAL,
    creativity                  REAL,
    threat                      REAL,
    ict_index                   REAL,
    starts                      INTEGER,
    expected_goals              REAL,
    expected_assists            REAL,
    expected_goal_involvements  REAL,
    expected_goals_conceded     REAL,
    was_home                    BOOLEAN,
    opponent_team_id            INTEGER,
    fixture_id                  INTEGER,
    kickoff_time                TEXT,
    data_quality_score          REAL,
    is_incomplete               BOOLEAN DEFAULT FALSE,
    created_at                  TIMESTAMPTZ DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ DEFAULT NOW()
);

-- Silver FPL fantasy stats (ownership, transfers, pricing — per player per GW)
CREATE TABLE IF NOT EXISTS silver_fpl_fantasy_stats (
    id                              BIGSERIAL PRIMARY KEY,
    unified_player_id               UUID REFERENCES silver_player_mapping(unified_player_id),
    match_id                        UUID REFERENCES silver_match_mapping(match_id),
    season                          TEXT NOT NULL,
    gameweek                        INTEGER,
    now_cost                        INTEGER,
    selected_by_percent             REAL,
    transfers_in                    INTEGER,
    transfers_out                   INTEGER,
    form                            REAL,
    status                          TEXT,
    news                            TEXT,
    chance_of_playing_next_round    INTEGER,
    chance_of_playing_this_round    INTEGER,
    in_dreamteam                    BOOLEAN,
    penalties_order                 INTEGER,
    corners_and_indirect_freekicks_order INTEGER,
    direct_freekicks_order          INTEGER,
    data_quality_score              REAL,
    is_incomplete                   BOOLEAN DEFAULT FALSE,
    created_at                      TIMESTAMPTZ DEFAULT NOW(),
    updated_at                      TIMESTAMPTZ DEFAULT NOW()
);

-- Silver Understat player stats (xG/xA per player per match)
CREATE TABLE IF NOT EXISTS silver_understat_player_stats (
    id                  BIGSERIAL PRIMARY KEY,
    unified_player_id   UUID REFERENCES silver_player_mapping(unified_player_id),
    match_id            UUID REFERENCES silver_match_mapping(match_id),
    season              TEXT NOT NULL,
    gameweek            INTEGER,
    minutes             INTEGER,
    goals               INTEGER,
    xg                  REAL,
    assists             INTEGER,
    xa                  REAL,
    shots               INTEGER,
    key_passes          INTEGER,
    xg_chain            REAL,
    xg_buildup          REAL,
    position            TEXT,
    data_quality_score  REAL,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Silver Understat match stats (team-level xG per match)
CREATE TABLE IF NOT EXISTS silver_understat_match_stats (
    id                  BIGSERIAL PRIMARY KEY,
    match_id            UUID REFERENCES silver_match_mapping(match_id),
    season              TEXT NOT NULL,
    gameweek            INTEGER,
    home_team_id        UUID REFERENCES silver_team_mapping(unified_team_id),
    away_team_id        UUID REFERENCES silver_team_mapping(unified_team_id),
    home_goals          INTEGER,
    away_goals          INTEGER,
    home_xg             REAL,
    away_xg             REAL,
    date                TEXT,
    game_id             INTEGER,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Silver fixtures (cleaned fixture schedule with unified team IDs)
CREATE TABLE IF NOT EXISTS silver_fixtures (
    id                  BIGSERIAL PRIMARY KEY,
    match_id            UUID REFERENCES silver_match_mapping(match_id),
    season              TEXT NOT NULL,
    gameweek            INTEGER,
    home_team_id        UUID REFERENCES silver_team_mapping(unified_team_id),
    away_team_id        UUID REFERENCES silver_team_mapping(unified_team_id),
    kickoff_time        TEXT,
    home_score          INTEGER,
    away_score          INTEGER,
    finished            BOOLEAN DEFAULT FALSE,
    fpl_fixture_id      INTEGER,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Silver unified player stats (merged FPL + Understat — the main feature source)
CREATE TABLE IF NOT EXISTS silver_unified_player_stats (
    id                          BIGSERIAL PRIMARY KEY,
    unified_player_id           UUID REFERENCES silver_player_mapping(unified_player_id),
    match_id                    UUID REFERENCES silver_match_mapping(match_id),
    season                      TEXT NOT NULL,
    gameweek                    INTEGER,
    team_id                     INTEGER,
    position                    TEXT,
    -- FPL stats
    total_points                INTEGER,
    minutes                     INTEGER,
    goals_scored                INTEGER,
    assists                     INTEGER,
    clean_sheets                INTEGER,
    goals_conceded              INTEGER,
    starts                      INTEGER,
    expected_goals              REAL,
    expected_assists            REAL,
    expected_goal_involvements  REAL,
    expected_goals_conceded     REAL,
    -- Understat stats (preferred over FPL when available)
    xg                          REAL,
    xa                          REAL,
    xg_chain                    REAL,
    xg_buildup                  REAL,
    shots                       INTEGER,
    key_passes                  INTEGER,
    -- Discipline
    yellow_cards                INTEGER,
    red_cards                   INTEGER,
    own_goals                   INTEGER,
    penalties_saved             INTEGER,
    penalties_missed            INTEGER,
    -- Bonus/BPS
    bonus                       INTEGER,
    bps                         INTEGER,
    -- ICT
    influence                   REAL,
    creativity                  REAL,
    threat                      REAL,
    ict_index                   REAL,
    -- Defensive
    tackles                     INTEGER,
    clearances_blocks_interceptions INTEGER,
    recoveries                  INTEGER,
    defensive_contribution      INTEGER,
    saves                       INTEGER,
    -- Match context
    was_home                    BOOLEAN,
    opponent_team_id            INTEGER,
    fixture_id                  INTEGER,
    kickoff_time                TEXT,
    home_score                  INTEGER,
    away_score                  INTEGER,
    -- Data quality
    data_quality_score          REAL,
    is_incomplete               BOOLEAN DEFAULT FALSE,
    created_at                  TIMESTAMPTZ DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ DEFAULT NOW()
);


-- =============================================================================
-- INDEXES (performance-critical queries)
-- =============================================================================

-- Silver team mapping
CREATE INDEX IF NOT EXISTS idx_team_map_season ON silver_team_mapping(season);
CREATE INDEX IF NOT EXISTS idx_team_map_fpl ON silver_team_mapping(fpl_team_id);

-- Silver player mapping
CREATE INDEX IF NOT EXISTS idx_player_map_season ON silver_player_mapping(season);
CREATE INDEX IF NOT EXISTS idx_player_map_fpl ON silver_player_mapping(fpl_id);
CREATE INDEX IF NOT EXISTS idx_player_map_understat ON silver_player_mapping(understat_id);

-- Silver match mapping
CREATE INDEX IF NOT EXISTS idx_match_map_season ON silver_match_mapping(season);
CREATE INDEX IF NOT EXISTS idx_match_map_gw ON silver_match_mapping(season, gameweek);
CREATE INDEX IF NOT EXISTS idx_match_map_fpl ON silver_match_mapping(fpl_fixture_id);

-- Silver player stats
CREATE INDEX IF NOT EXISTS idx_fpl_stats_player ON silver_fpl_player_stats(unified_player_id);
CREATE INDEX IF NOT EXISTS idx_fpl_stats_season_gw ON silver_fpl_player_stats(season, gameweek);
CREATE INDEX IF NOT EXISTS idx_understat_stats_player ON silver_understat_player_stats(unified_player_id);
CREATE INDEX IF NOT EXISTS idx_unified_stats_player ON silver_unified_player_stats(unified_player_id);
CREATE INDEX IF NOT EXISTS idx_unified_stats_season_gw ON silver_unified_player_stats(season, gameweek);

-- Bronze indexes
CREATE INDEX IF NOT EXISTS idx_bronze_fpl_gw_season ON bronze_fpl_gw(season);
CREATE INDEX IF NOT EXISTS idx_bronze_fpl_gw_element ON bronze_fpl_gw(element);
CREATE INDEX IF NOT EXISTS idx_bronze_understat_ps_season ON bronze_understat_player_stats(season);
