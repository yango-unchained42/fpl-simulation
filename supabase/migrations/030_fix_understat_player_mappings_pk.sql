-- Migration 030: Fix bronze_understat_player_mappings primary key
-- The current PK only includes (player_id, season) but players can transfer
-- between teams within a season. Drop the PK and use a different approach.

-- First, clear the table
DELETE FROM bronze_understat_player_mappings WHERE true;

-- Drop the problematic primary key constraint
ALTER TABLE bronze_understat_player_mappings DROP CONSTRAINT bronze_understat_player_mappings_pkey;

-- Add a new unique constraint that includes team_id (for transfers within season)
ALTER TABLE bronze_understat_player_mappings ADD PRIMARY KEY (understat_player_id, understat_team_id, season);

-- Re-run the upload_data.py script to populate the table
