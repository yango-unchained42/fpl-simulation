-- Add missing saves_per_90 column to bronze_vaastav_players

ALTER TABLE bronze_vaastav_players ADD COLUMN IF NOT EXISTS saves_per_90 TEXT;