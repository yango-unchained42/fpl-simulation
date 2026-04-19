-- Migration: 034_add_fk_constraints_to_silver_fpl_player_stats
-- Already applied via CLI

-- FK constraints confirmed:
-- - fk_silver_fpl_player_stats_unified_player_id -> silver_player_mapping(unified_player_id)
-- - fk_silver_fpl_player_stats_match_id -> silver_match_mapping(match_id)
