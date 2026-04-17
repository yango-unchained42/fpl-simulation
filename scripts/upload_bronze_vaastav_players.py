"""Upload bronze_vaastav_players to Supabase.

Loads players_raw.parquet from all seasons and uploads to Supabase.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

DATA_DIR = Path("data/raw/vaastav")
SEASONS = ["2021-22", "2022-23", "2023-24", "2024-25"]


def load_vaastav_players() -> pl.DataFrame:
    """Load all players_raw data across seasons."""
    frames = []
    for season in SEASONS:
        path = DATA_DIR / season / "players_raw.parquet"
        if path.exists():
            df = pl.read_parquet(path)
            df = df.with_columns(pl.lit(season).alias("season"))
            frames.append(df)
            logger.info(f"Loaded {len(df)} players from {season}")
        else:
            logger.warning(f"Missing {path}")

    if not frames:
        raise FileNotFoundError(f"No players_raw.parquet found in {DATA_DIR}")

    combined = pl.concat(frames, how="diagonal")
    logger.info(f"Total: {len(combined)} players across {len(SEASONS)} seasons")
    return combined


def main() -> None:
    """Main entry point."""
    from dotenv import load_dotenv

    load_dotenv()

    from supabase import create_client

    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_KEY", "")
    client = create_client(url, key)

    df = load_vaastav_players()

    # Convert to list of dicts
    records = df.to_dicts()
    logger.info(f"Uploading {len(records)} records...")

    # Upload in chunks to avoid payload limits
    chunk_size = 1000
    for i in range(0, len(records), chunk_size):
        chunk = records[i : i + chunk_size]
        client.table("bronze_vaastav_players").insert(chunk).execute()
        logger.info(f"Uploaded {i + len(chunk)}/{len(records)}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
