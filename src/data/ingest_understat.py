"""Understat data ingestion via soccerdata.

Scrapes match-level xG, xA, player shot data, and per-player match/season
statistics from Understat for historical analysis. Premier League only.
Implements caching and error handling.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import polars as pl

from src.config import (
    ALL_SEASONS,
    CACHE_TTL_SECONDS,
    CURRENT_SEASON_UNDERSTAT,
    RAW_DATA_DIR,
)

logger = logging.getLogger(__name__)

DATA_DIR = RAW_DATA_DIR / "understat"


def _season_dir(season: str) -> Path:
    """Get the season-specific data directory."""
    safe = season.replace("/", "_")
    dir_path = DATA_DIR / safe
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


def _cache_key(season: str, table: str) -> Path:
    """Generate cache file path for a season/table combination."""
    return _season_dir(season) / f"{table}.parquet"


def _to_soccerdata_season(season: str) -> str:
    """Convert season string to soccerdata format.

    Args:
        season: Season string (e.g., "2023/24", "2023-24", or "2023_24").

    Returns:
        Soccerdata season code (e.g., "2324").
    """
    # Normalize: replace underscores and slashes with hyphens
    clean = season.replace("/", "-").replace("_", "-")
    parts = clean.split("-")
    if len(parts) == 2:
        return f"{parts[0][-2:]}{parts[1][-2:]}"
    return season


def _to_polars(df: Any) -> pl.DataFrame:
    """Convert pandas or polars DataFrame to polars.

    Args:
        df: DataFrame to convert.

    Returns:
        Polars DataFrame.
    """
    if isinstance(df, pl.DataFrame):
        return df
    import pandas as pd

    if isinstance(df, pd.DataFrame):
        # Reset index to include index values as columns (e.g., player name)
        df = df.reset_index()
        return pl.from_pandas(df)
    return pl.DataFrame()


def _is_cache_valid(cache_path: Path, ttl: int = CACHE_TTL_SECONDS) -> bool:
    """Check if a cached file is still valid.

    Args:
        cache_path: Path to the cache file.
        ttl: Time-to-live in seconds.

    Returns:
        True if cache exists and is younger than ttl.
    """
    if not cache_path.exists():
        return False
    age = time.time() - cache_path.stat().st_mtime
    return age < ttl


def _save_cache(cache_path: Path, df: pl.DataFrame) -> None:
    """Save DataFrame to a Parquet cache file.

    Args:
        cache_path: Path to the cache file.
        df: DataFrame to cache.
    """
    df.write_parquet(cache_path)


def _load_cache(cache_path: Path) -> pl.DataFrame:
    """Load DataFrame from a Parquet cache file.

    Args:
        cache_path: Path to the cache file.

    Returns:
        Cached DataFrame.
    """
    return pl.read_parquet(cache_path)


def _fetch_season_table(
    season: str,
    table_name: str,
    reader_method: str,
) -> pl.DataFrame:
    """Generic fetch helper for any Understat table.

    Args:
        season: Season string (e.g., "2023/24").
        table_name: Human-readable table name for logging.
        reader_method: Name of the soccerdata method to call.

    Returns:
        Polars DataFrame, or empty DataFrame on failure.
    """
    logger.info("Fetching Understat %s for %s", table_name, season)
    try:
        from soccerdata import Understat

        sd_season = _to_soccerdata_season(season)
        us = Understat(leagues="ENG-Premier League", seasons=sd_season, no_cache=True)
        reader = getattr(us, reader_method)
        raw_df = reader()
        return _to_polars(raw_df)
    except ImportError:
        logger.warning("soccerdata not installed, skipping Understat %s", table_name)
        return pl.DataFrame()
    except Exception as e:  # noqa: BLE001
        logger.error("Failed to fetch Understat %s for %s: %s", table_name, season, e)
        return pl.DataFrame()


def _ingest_season_table(
    seasons: list[str] | None,
    table_name: str,
    reader_method: str,
    use_cache: bool,
) -> pl.DataFrame:
    """Generic ingest helper for any Understat table.

    Args:
        seasons: List of season strings.
        table_name: Cache table name.
        reader_method: Understat reader method name.
        use_cache: Whether to use cached data.

    Returns:
        Combined Polars DataFrame across all seasons.
    """
    if seasons is None:
        seasons = ALL_SEASONS

    frames: list[pl.DataFrame] = []
    for season in seasons:
        cache_path = _cache_key(season, table_name)

        if use_cache and _is_cache_valid(cache_path):
            logger.info("Using cached Understat %s for %s", table_name, season)
            df = _load_cache(cache_path)
        else:
            df = _fetch_season_table(season, table_name, reader_method)
            if not df.is_empty():
                _save_cache(cache_path, df)

        if not df.is_empty():
            df = df.with_columns(pl.lit(season).alias("season"))
            frames.append(df)

    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal")


# ── Public ingestion functions ──────────────────────────────────────────────


def ingest_understat_shots(
    seasons: list[str] | None = None,
    use_cache: bool = True,
) -> pl.DataFrame:
    """Ingest Understat individual shot events.

    Args:
        seasons: List of season strings. Defaults to last 3 seasons.
        use_cache: Whether to use cached data if available.

    Returns:
        Polars DataFrame with shot events (xG, location, outcome, etc.).
    """
    return _ingest_season_table(seasons, "shots", "read_shot_events", use_cache)


def ingest_understat_match_stats(
    seasons: list[str] | None = None,
    use_cache: bool = True,
) -> pl.DataFrame:
    """Ingest Understat team-level match statistics.

    Args:
        seasons: List of season strings. Defaults to last 3 seasons.
        use_cache: Whether to use cached data if available.

    Returns:
        Polars DataFrame with team match-level xG data.
    """
    return _ingest_season_table(
        seasons, "match_stats", "read_team_match_stats", use_cache
    )


def ingest_understat_player_match_stats(
    seasons: list[str] | None = None,
    use_cache: bool = True,
) -> pl.DataFrame:
    """Ingest Understat per-player match statistics.

    The most valuable table for FPL — per-player, per-match xG, xA,
    shots, key passes, minutes played, etc. (~11k rows per season).

    Args:
        seasons: List of season strings. Defaults to last 3 seasons.
        use_cache: Whether to use cached data if available.

    Returns:
        Polars DataFrame with player match stats.
    """
    return _ingest_season_table(
        seasons, "player_match_stats", "read_player_match_stats", use_cache
    )


def ingest_understat_player_season_stats(
    seasons: list[str] | None = None,
    use_cache: bool = True,
) -> pl.DataFrame:
    """Ingest Understat per-player season statistics.

    Season-aggregated xG, xA, shots, key passes per player
    (~500 rows per season).

    Args:
        seasons: List of season strings. Defaults to last 3 seasons.
        use_cache: Whether to use cached data if available.

    Returns:
        Polars DataFrame with player season stats.
    """
    return _ingest_season_table(
        seasons, "player_season_stats", "read_player_season_stats", use_cache
    )


def ingest_understat(
    seasons: list[str] | None = None,
    use_cache: bool = True,
) -> dict[str, pl.DataFrame]:
    """Ingest all Understat tables for specified seasons.

    Args:
        seasons: List of season strings. Defaults to last 3 seasons.
        use_cache: Whether to use cached data if available.

    Returns:
        Dict with keys 'shots', 'match_stats', 'player_match_stats',
        and 'player_season_stats' mapping to their respective DataFrames.
    """
    return {
        "shots": ingest_understat_shots(seasons, use_cache=use_cache),
        "match_stats": ingest_understat_match_stats(seasons, use_cache=use_cache),
        "player_match_stats": ingest_understat_player_match_stats(
            seasons, use_cache=use_cache
        ),
        "player_season_stats": ingest_understat_player_season_stats(
            seasons, use_cache=use_cache
        ),
    }


def clear_cache(season: str | None = None) -> None:
    """Clear cached Understat data.

    Args:
        season: Specific season to clear. If None, clears all seasons.
    """
    if season:
        season_path = _season_dir(season)
        if season_path.exists():
            for f in season_path.glob("*.parquet"):
                f.unlink()
            logger.info("Cleared Understat cache for season %s", season)
    else:
        if DATA_DIR.exists():
            for f in DATA_DIR.rglob("*.parquet"):
                f.unlink()
            logger.info("Cleared all Understat cache in %s", DATA_DIR)
