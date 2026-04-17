-- Migration: 032_add_fk_constraints_to_match_mapping
-- Add foreign key constraints to silver_match_mapping for proper referential integrity

-- Add FK for home team
ALTER TABLE silver_match_mapping 
    ADD CONSTRAINT fk_match_home_team 
    FOREIGN KEY (home_unified_team_id) 
    REFERENCES silver_team_mapping(unified_team_id);

-- Add FK for away team
ALTER TABLE silver_match_mapping 
    ADD CONSTRAINT fk_match_away_team 
    FOREIGN KEY (away_unified_team_id) 
    REFERENCES silver_team_mapping(unified_team_id);

COMMENT ON CONSTRAINT fk_match_home_team ON silver_match_mapping IS 'Foreign key to silver_team_mapping for home team';
COMMENT ON CONSTRAINT fk_match_away_team ON silver_match_mapping IS 'Foreign key to silver_team_mapping for away team';