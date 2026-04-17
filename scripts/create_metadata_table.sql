-- Metadata table for tracking source data changes
-- Used by daily_bronze_update.py to detect data changes

CREATE TABLE IF NOT EXISTS metadata (
    table_name TEXT NOT NULL,
    season TEXT NOT NULL,
    source_hash TEXT,
    row_count INTEGER,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    PRIMARY KEY (table_name, season)
);

CREATE INDEX IF NOT EXISTS idx_metadata_last_updated ON metadata(last_updated DESC);
