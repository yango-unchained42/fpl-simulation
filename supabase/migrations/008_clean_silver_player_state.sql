-- Clean up silver_player_state - remove redundant columns
-- Names/positions can be resolved via silver_player_mapping in Streamlit/Gold

ALTER TABLE silver_player_state DROP COLUMN IF EXISTS web_name;
ALTER TABLE silver_player_state DROP COLUMN IF EXISTS team;
ALTER TABLE silver_player_state DROP COLUMN IF EXISTS element_type;
