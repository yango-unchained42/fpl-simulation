-- Migration: Fix silver_player_mapping duplicates
-- Run this in Supabase SQL Editor to:
-- 1. Remove duplicate entries (keep best match per season+fpl_id)
-- 2. Add unique constraint to prevent future duplicates

-- Step 1: Remove duplicates — keep the row with highest confidence_score per (season, fpl_id)
WITH ranked AS (
    SELECT
        unified_player_id,
        season,
        fpl_id,
        confidence_score,
        ROW_NUMBER() OVER (
            PARTITION BY season, fpl_id
            ORDER BY
                confidence_score DESC NULLS LAST,
                updated_at DESC NULLS LAST,
                unified_player_id
        ) AS rn
    FROM silver_player_mapping
    WHERE fpl_id IS NOT NULL
)
DELETE FROM silver_player_mapping
WHERE unified_player_id IN (
    SELECT unified_player_id FROM ranked WHERE rn > 1
);

-- Step 2: Also remove rows where fpl_id is null (orphaned entries)
DELETE FROM silver_player_mapping WHERE fpl_id IS NULL;

-- Step 3: Add unique constraint on (season, fpl_id)
-- This prevents future duplicates at the database level
ALTER TABLE silver_player_mapping
    ADD CONSTRAINT uq_player_season_fpl UNIQUE (season, fpl_id);

-- Step 4: Verify — should return 0
SELECT COUNT(*) AS remaining_duplicates
FROM (
    SELECT season, fpl_id, COUNT(*)
    FROM silver_player_mapping
    GROUP BY season, fpl_id
    HAVING COUNT(*) > 1
) dupes;
