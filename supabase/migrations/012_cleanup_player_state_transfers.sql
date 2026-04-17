-- Remove cumulative transfer columns from silver_player_state
-- Only keep GW-specific transfer data

ALTER TABLE silver_player_state DROP COLUMN IF EXISTS transfers_in;
ALTER TABLE silver_player_state DROP COLUMN IF EXISTS transfers_out;
ALTER TABLE silver_player_state DROP COLUMN IF EXISTS transfers_balance;

-- Rename event columns to simpler names
ALTER TABLE silver_player_state RENAME COLUMN transfers_in_event TO transfers_in;
ALTER TABLE silver_player_state RENAME COLUMN transfers_out_event TO transfers_out;