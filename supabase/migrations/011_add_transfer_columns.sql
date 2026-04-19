-- Add transfer event columns to silver_player_state

ALTER TABLE silver_player_state ADD COLUMN IF NOT EXISTS transfers_in_event INTEGER;
ALTER TABLE silver_player_state ADD COLUMN IF NOT EXISTS transfers_out_event INTEGER;
ALTER TABLE silver_player_state ADD COLUMN IF NOT EXISTS transfers_balance INTEGER;
