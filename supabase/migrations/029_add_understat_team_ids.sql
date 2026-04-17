-- Migration 029: Add Understat team IDs to silver_team_mapping for 2025-26
-- These IDs were derived by matching player names between FPL and Understat

UPDATE silver_team_mapping
SET understat_team_id = 83
WHERE season = '2025-26' AND fpl_team_name = 'Arsenal';

UPDATE silver_team_mapping
SET understat_team_id = 71
WHERE season = '2025-26' AND fpl_team_name = 'Aston Villa';

UPDATE silver_team_mapping
SET understat_team_id = 92
WHERE season = '2025-26' AND fpl_team_name = 'Burnley';

UPDATE silver_team_mapping
SET understat_team_id = 73
WHERE season = '2025-26' AND fpl_team_name = 'Bournemouth';

UPDATE silver_team_mapping
SET understat_team_id = 244
WHERE season = '2025-26' AND fpl_team_name = 'Brentford';

UPDATE silver_team_mapping
SET understat_team_id = 220
WHERE season = '2025-26' AND fpl_team_name = 'Brighton';

UPDATE silver_team_mapping
SET understat_team_id = 80
WHERE season = '2025-26' AND fpl_team_name = 'Chelsea';

UPDATE silver_team_mapping
SET understat_team_id = 78
WHERE season = '2025-26' AND fpl_team_name = 'Crystal Palace';

UPDATE silver_team_mapping
SET understat_team_id = 72
WHERE season = '2025-26' AND fpl_team_name = 'Everton';

UPDATE silver_team_mapping
SET understat_team_id = 228
WHERE season = '2025-26' AND fpl_team_name = 'Fulham';

UPDATE silver_team_mapping
SET understat_team_id = 245
WHERE season = '2025-26' AND fpl_team_name = 'Leeds';

UPDATE silver_team_mapping
SET understat_team_id = 87
WHERE season = '2025-26' AND fpl_team_name = 'Liverpool';

UPDATE silver_team_mapping
SET understat_team_id = 88
WHERE season = '2025-26' AND (fpl_team_name = 'Man City' OR fpl_team_name = 'Manchester City');

UPDATE silver_team_mapping
SET understat_team_id = 89
WHERE season = '2025-26' AND (fpl_team_name = 'Man Utd' OR fpl_team_name = 'Manchester United');

UPDATE silver_team_mapping
SET understat_team_id = 86
WHERE season = '2025-26' AND (fpl_team_name = 'Newcastle' OR fpl_team_name = 'Newcastle United');

UPDATE silver_team_mapping
SET understat_team_id = 249
WHERE season = '2025-26' AND (fpl_team_name = 'Nott''m Forest' OR fpl_team_name = 'Nottingham Forest');

UPDATE silver_team_mapping
SET understat_team_id = 77
WHERE season = '2025-26' AND fpl_team_name = 'Sunderland';

UPDATE silver_team_mapping
SET understat_team_id = 82
WHERE season = '2025-26' AND (fpl_team_name = 'Spurs' OR fpl_team_name = 'Tottenham');

UPDATE silver_team_mapping
SET understat_team_id = 81
WHERE season = '2025-26' AND fpl_team_name = 'West Ham';

UPDATE silver_team_mapping
SET understat_team_id = 229
WHERE season = '2025-26' AND (fpl_team_name = 'Wolves' OR fpl_team_name = 'Wolverhampton Wanderers');

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_silver_team_mapping_understat_team_id
ON silver_team_mapping(understat_team_id) WHERE understat_team_id IS NOT NULL;
