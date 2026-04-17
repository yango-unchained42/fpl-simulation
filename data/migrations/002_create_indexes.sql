-- 002_create_indexes.sql
-- Indexes for query performance optimization.

CREATE INDEX IF NOT EXISTS idx_player_stats_gameweek ON player_stats(gameweek);
CREATE INDEX IF NOT EXISTS idx_predictions_gameweek ON predictions(gameweek);
CREATE INDEX IF NOT EXISTS idx_player_h2h ON player_vs_team(player_id);
CREATE INDEX IF NOT EXISTS idx_fixture_gameweek ON fixtures(gameweek);
CREATE INDEX IF NOT EXISTS idx_player_stats_player ON player_stats(player_id);
CREATE INDEX IF NOT EXISTS idx_predictions_player ON predictions(player_id);
