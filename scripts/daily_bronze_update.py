"""Daily Bronze layer update for FPL Simulation.

This script runs daily to:
1. Check if FPL source data has been updated
2. If updated, fetch and upsert to Bronze tables
3. Check if Understat source data has been updated
4. If updated, fetch and upsert to Bronze tables

Only processes current season data (no historical).
Uses incremental updates - only upserts changed data.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import polars as pl
import requests
from dotenv import load_dotenv

from src.config import BATCH_SIZE, CURRENT_SEASON, FPL_API_BASE
from src.config import get_supabase as _get_supabase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration — can be overridden by --season arg
SEASON = CURRENT_SEASON
DATA_DIR = Path("data/raw/fpl")


def get_supabase():
    """Get Supabase client."""
    load_dotenv()
    return _get_supabase()


def compute_file_hash(file_path: Path) -> str:
    """Compute MD5 hash of a file."""
    if not file_path.exists():
        return ""
    with open(file_path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


def get_source_hash(table_name: str, season: str = SEASON) -> str:
    """Get the current hash of source data from local cache."""
    season_dir = DATA_DIR / season

    if table_name == "fpl_players":
        return compute_file_hash(season_dir / "bootstrap-static.json")
    elif table_name == "fpl_fixtures":
        return compute_file_hash(season_dir / "fixtures.json")
    elif table_name == "fpl_gw":
        return compute_file_hash(season_dir / "player_history.parquet")
    return ""


def has_source_changed(table_name: str, season: str = SEASON) -> bool:
    """Check if source data has changed since last upload."""
    supabase = get_supabase()

    # Get last update timestamp from metadata table
    try:
        result = (
            supabase.table("metadata")
            .select("*")
            .eq("table_name", table_name)
            .eq("season", season)
            .execute()
        )
        if result.data:
            last_hash = result.data[0].get("source_hash", "")
            current_hash = get_source_hash(table_name, season)
            return last_hash != current_hash
    except Exception:
        pass

    # If no metadata, assume changed
    return True


def update_metadata(table_name: str, season: str, row_count: int) -> None:
    """Update metadata table with current hash and row count."""
    supabase = get_supabase()
    current_hash = get_source_hash(table_name, season)

    try:
        supabase.table("metadata").upsert(
            {
                "table_name": table_name,
                "season": season,
                "source_hash": current_hash,
                "row_count": row_count,
                "last_updated": datetime.now().isoformat(),
            },
            on_conflict="table_name,season",
        ).execute()
    except Exception as e:
        logger.warning(f"Could not update metadata: {e}")


def fetch_fpl_players() -> pl.DataFrame:
    """Fetch FPL players from API or local cache."""
    season_dir = DATA_DIR / SEASON
    cache_file = season_dir / "bootstrap-static.json"

    # Try API first
    try:
        logger.info("  Fetching FPL players from API...")
        response = requests.get(f"{FPL_API_BASE}bootstrap-static/", timeout=30)
        response.raise_for_status()
        data = response.json()

        # Cache locally
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_file, "w") as f:
            json.dump(data, f)

        return pl.DataFrame(data.get("elements", []))
    except Exception as e:
        logger.warning(f"  API fetch failed: {e}, using cache")

    # Fallback to cache
    if cache_file.exists():
        with open(cache_file) as f:
            data = json.load(f)
        return pl.DataFrame(data.get("elements", []))

    return pl.DataFrame()


def fetch_fpl_fixtures() -> pl.DataFrame:
    """Fetch FPL fixtures from API or local cache."""
    season_dir = DATA_DIR / SEASON
    cache_file = season_dir / "fixtures.json"

    # Try API first
    try:
        logger.info("  Fetching FPL fixtures from API...")
        response = requests.get(f"{FPL_API_BASE}fixtures/", timeout=30)
        response.raise_for_status()
        data = response.json()

        # Cache locally
        with open(cache_file, "w") as f:
            json.dump(data, f)

        return pl.DataFrame(data)
    except Exception as e:
        logger.warning(f"  API fetch failed: {e}, using cache")

    # Fallback to cache
    if cache_file.exists():
        with open(cache_file) as f:
            data = json.load(f)
        return pl.DataFrame(data)

    return pl.DataFrame()


def fetch_fpl_gw() -> pl.DataFrame:
    """Fetch FPL gameweek data from API or local cache."""
    season_dir = DATA_DIR / SEASON
    cache_file = season_dir / "player_history.parquet"

    # Try API first
    try:
        logger.info("  Fetching FPL GW data from API...")
        response = requests.get(f"{FPL_API_BASE}element-summary/{1}/", timeout=30)
        # This would need to iterate all players - for now use cache approach
        # Actually FPL doesn't have a bulk GW endpoint, we use the cached approach
    except Exception:
        pass

    # Fallback to cache
    if cache_file.exists():
        df = pl.read_parquet(cache_file)
        # Deduplicate: keep row with most minutes per player-gameweek
        df = df.sort(["element", "round", "minutes"], descending=[False, False, True])
        df = df.unique(subset=["element", "round"], keep="first")
        return df

    return pl.DataFrame()


def fetch_fpl_teams() -> pl.DataFrame:
    """Fetch FPL teams from API or local cache."""
    season_dir = DATA_DIR / SEASON
    cache_file = season_dir / "bootstrap-static.json"

    if cache_file.exists():
        with open(cache_file) as f:
            data = json.load(f)
        return pl.DataFrame(data.get("teams", []))

    return pl.DataFrame()


def get_table_columns(supabase, table_name: str) -> set:
    """Get columns that exist in Supabase table."""
    try:
        result = supabase.table(table_name).select("*").limit(1).execute()
        if result.data:
            return set(result.data[0].keys())
    except Exception:
        pass
    return set()


def filter_to_schema(df: pl.DataFrame, valid_cols: set) -> pl.DataFrame:
    """Filter DataFrame to only include columns in valid_cols."""
    cols_to_keep = [c for c in df.columns if c in valid_cols]
    return df.select(cols_to_keep)


def upload_table(supabase, table_name: str, df: pl.DataFrame) -> int:
    """Upload DataFrame to Supabase, returning row count.

    Truncates table before upload to avoid duplicates from multiple runs.
    """
    import subprocess

    if df.is_empty():
        logger.info(f"  ⏭️  Skipping {table_name} (empty)")
        return 0

    # Truncate table to avoid duplicates (use CLI if available)
    token = os.getenv("SUPABASE_ACCESS_TOKEN")
    if token:
        try:
            result = subprocess.run(
                [
                    "supabase",
                    "db",
                    "query",
                    "--linked",
                    f"TRUNCATE {table_name} CASCADE;",
                ],
                capture_output=True,
                text=True,
                env={**os.environ, "SUPABASE_ACCESS_TOKEN": token},
            )
            if result.returncode == 0:
                logger.info(f"  🗑️  Truncated {table_name}")
        except Exception:
            pass  # Continue without truncate if CLI fails

    # Filter to only columns that exist in Supabase schema
    valid_cols = get_table_columns(supabase, table_name)

    # Convert datetime columns to ISO strings for JSON serialization
    datetime_cols = [c for c in df.columns if df[c].dtype == pl.Datetime]
    if datetime_cols:
        for col in datetime_cols:
            df = df.with_columns(
                pl.col(col).dt.strftime("%Y-%m-%dT%H:%M:%S").alias(col)
            )

    df = filter_to_schema(df, valid_cols)

    if df.is_empty():
        logger.info(f"  ⏭️  Skipping {table_name} (no valid columns)")
        return 0

    records = df.to_dicts()
    total = len(records)
    logger.info(f"  📤 Uploading {table_name}: {total} rows...")

    for i in range(0, total, BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        try:
            supabase.table(table_name).upsert(batch).execute()
        except Exception as e:
            logger.error(f"  ❌ Error at batch {i}: {e}")

    logger.info(f"  ✅ {table_name} complete!")
    return total


def update_fpl_bronze() -> bool:
    """Update FPL Bronze tables if source changed."""
    supabase = get_supabase()
    updated = False

    # 1. FPL Players
    if has_source_changed("fpl_players"):
        logger.info("🥉 FPL Players - source changed, updating...")
        df = fetch_fpl_players()
        if not df.is_empty():
            df = df.with_columns(pl.lit(SEASON).alias("season"))
            upload_table(supabase, "bronze_fpl_players", df)
            update_metadata("fpl_players", SEASON, len(df))
            updated = True
    else:
        logger.info("🥉 FPL Players - no changes, skipping")

    # 2. FPL Teams
    if has_source_changed("fpl_teams"):
        logger.info("🥉 FPL Teams - source changed, updating...")
        df = fetch_fpl_teams()
        if not df.is_empty():
            df = df.with_columns(pl.lit(SEASON).alias("season"))
            upload_table(supabase, "bronze_fpl_teams", df)
            update_metadata("fpl_teams", SEASON, len(df))
            updated = True
    else:
        logger.info("🥉 FPL Teams - no changes, skipping")

    # 3. FPL Fixtures
    if has_source_changed("fpl_fixtures"):
        logger.info("🥉 FPL Fixtures - source changed, updating...")
        df = fetch_fpl_fixtures()
        if not df.is_empty():
            df = df.with_columns(pl.lit(SEASON).alias("season"))
            upload_table(supabase, "bronze_fpl_fixtures", df)
            update_metadata("fpl_fixtures", SEASON, len(df))
            updated = True
    else:
        logger.info("🥉 FPL Fixtures - no changes, skipping")

    # 4. FPL GW (only if new gameweek data exists)
    if has_source_changed("fpl_gw"):
        logger.info("🥉 FPL GW - source changed, updating...")
        df = fetch_fpl_gw()
        if not df.is_empty():
            df = df.with_columns(pl.lit(SEASON).alias("season"))
            # Drop player_id if exists (duplicate of element)
            if "player_id" in df.columns:
                df = df.drop("player_id")
            upload_table(supabase, "bronze_fpl_gw", df)
            update_metadata("fpl_gw", SEASON, len(df))
            updated = True
    else:
        logger.info("🥉 FPL GW - no changes, skipping")

    return updated


def update_understat_bronze() -> bool:
    """Update Understat Bronze tables if source changed."""
    supabase = get_supabase()
    updated = False

    # Understat - fetch current season from API
    logger.info("🥉 Understat - fetching from API...")

    try:
        from src.data.ingest_understat import (
            ingest_understat_match_stats,
            ingest_understat_player_match_stats,
            ingest_understat_shots,
        )

        # Only fetch current season
        current_season_list = [SEASON.replace("-", "_")]

        # 1. Player match stats (xG, xA, shots per player-game)
        logger.info("  🥉 Fetching understat_player_stats...")
        df = ingest_understat_player_match_stats(
            seasons=current_season_list, use_cache=False
        )
        if not df.is_empty():
            # Add season column
            df = df.with_columns(pl.lit(SEASON).alias("season"))
            # Filter to PL
            if "league_id" in df.columns:
                df = df.filter(pl.col("league_id") == "1")
            # Drop null keys
            if "player_id" in df.columns:
                df = df.filter(pl.col("player_id").is_not_null())
            if "game_id" in df.columns:
                df = df.filter(pl.col("game_id").is_not_null())
            if "season_id" in df.columns:
                df = df.drop("season_id")

            upload_table(supabase, "bronze_understat_player_stats", df)
            update_metadata("understat_player_match_stats", SEASON, len(df))
            updated = True
            logger.info(f"    Uploaded {len(df)} player stats")
        else:
            logger.info("    No data returned")

        # 2. Shots
        logger.info("  🥉 Fetching understat_shots...")
        df = ingest_understat_shots(seasons=current_season_list, use_cache=False)
        if not df.is_empty():
            df = df.with_columns(pl.lit(SEASON).alias("season"))
            if "league_id" in df.columns:
                df = df.filter(pl.col("league_id") == "1")
            if "shot_id" in df.columns:
                df = df.filter(pl.col("shot_id").is_not_null())
                df = df.rename({"shot_id": "id"})
            if "season_id" in df.columns:
                df = df.drop("season_id")

            upload_table(supabase, "bronze_understat_shots", df)
            update_metadata("understat_shots", SEASON, len(df))
            updated = True
            logger.info(f"    Uploaded {len(df)} shots")
        else:
            logger.info("    No data returned")

        # 3. Match stats
        logger.info("  🥉 Fetching understat_match_stats...")
        df = ingest_understat_match_stats(seasons=current_season_list, use_cache=False)
        if not df.is_empty():
            df = df.with_columns(pl.lit(SEASON).alias("season"))
            if "league_id" in df.columns:
                df = df.filter(pl.col("league_id") == "1")
            if "game_id" in df.columns:
                df = df.filter(pl.col("game_id").is_not_null())
            if "season_id" in df.columns:
                df = df.drop("season_id")

            upload_table(supabase, "bronze_understat_match_stats", df)
            update_metadata("understat_match_stats", SEASON, len(df))
            updated = True
            logger.info(f"    Uploaded {len(df)} match stats")
        else:
            logger.info("    No data returned")

    except Exception as e:
        logger.error(f"  ❌ Error fetching Understat from API: {e}")

    return updated


def create_metadata_table(supabase) -> None:
    """Create metadata table if it doesn't exist."""
    try:
        supabase.table("metadata").select("*").limit(1).execute()
    except Exception:
        logger.info("Creating metadata table...")
        # This would need SQL - in practice you'd run this once in Supabase
        pass


def main() -> None:
    """Main daily Bronze update function."""
    import argparse

    global SEASON  # noqa: PLW0603

    parser = argparse.ArgumentParser(description="Daily Bronze layer update")
    parser.add_argument(
        "--season",
        type=str,
        default=CURRENT_SEASON,
        help=f"Season to update (default: {CURRENT_SEASON})",
    )
    args = parser.parse_args()
    SEASON = args.season

    logger.info("🚀 Starting daily Bronze layer update...")
    logger.info(f"   Season: {SEASON}")
    start_time = time.time()

    try:
        supabase = get_supabase()
        logger.info("🔗 Connected to Supabase")
    except Exception as e:
        logger.error(f"❌ Failed to connect: {e}")
        return

    # Ensure metadata table exists
    create_metadata_table(supabase)

    # Update FPL Bronze tables
    logger.info("\n" + "=" * 50)
    logger.info("FPL BRONZE LAYER")
    logger.info("=" * 50)
    fpl_updated = update_fpl_bronze()

    # Update Understat Bronze tables
    logger.info("\n" + "=" * 50)
    logger.info("UNDERSTAT BRONZE LAYER")
    logger.info("=" * 50)
    understat_updated = update_understat_bronze()

    elapsed = time.time() - start_time

    if fpl_updated or understat_updated:
        logger.info(f"\n✅ Bronze update complete in {elapsed:.2f}s - data was updated")
    else:
        logger.info(
            f"\n✅ Bronze update complete in {elapsed:.2f}s - no changes detected"
        )


if __name__ == "__main__":
    main()
