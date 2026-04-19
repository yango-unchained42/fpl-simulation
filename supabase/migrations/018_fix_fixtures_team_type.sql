-- Fix silver_fixtures to allow TEXT for team IDs (Vaastav uses team names)

ALTER TABLE silver_fixtures ALTER COLUMN team_h TYPE TEXT;
ALTER TABLE silver_fixtures ALTER COLUMN team_a TYPE TEXT;
