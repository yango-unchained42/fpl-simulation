-- Migration 023: Rename match_mapping to silver_match_mapping
-- For consistency with other silver layer tables

ALTER TABLE match_mapping RENAME TO silver_match_mapping;

-- Update any foreign key constraints if they exist
-- (None currently referencing match_mapping)

COMMENT ON TABLE silver_match_mapping IS 'Unified fixture mapping across FPL, Vaastav, and Understat sources. Uses unified_team_id from silver_team_mapping.';
