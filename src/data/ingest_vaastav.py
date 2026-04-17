"""Vaastav historical GW stats loader.

Loads historical per-gameweek FPL stats from the vaastav/Fantasy-Premier-League
GitHub repository (2016/17–present). Uses FPL player IDs natively.
Implements caching, retry logic, and error logging.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import polars as pl
import requests

logger = logging.getLogger(__name__)

VAASTAV_BASE_URL = (
    "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master"
)
DATA_DIR = Path("data/raw/vaastav")
CACHE_TTL_SECONDS = 86400  # 24 hours for historical data
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2


def _season_dir(season: str) -> Path:
    """Get the season-specific data directory."""
    dir_path = DATA_DIR / season
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


def _cache_key(season: str, endpoint: str) -> Path:
    """Generate cache file path for a season/endpoint combination."""
    return _season_dir(season) / f"{endpoint}.parquet"


def _get_with_retry(url: str, timeout: int = 60) -> requests.Response:
    """Fetch URL with exponential backoff retry logic.

    Args:
        url: The URL to fetch.
        timeout: Request timeout in seconds.

    Returns:
        HTTP response object.

    Raises:
        requests.RequestException: If all retries are exhausted.
    """
    last_exception: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            last_exception = e
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY_SECONDS * (2**attempt)
                logger.warning(
                    "Request to %s failed (attempt %d/%d): %s. Retrying in %ds",
                    url,
                    attempt + 1,
                    MAX_RETRIES,
                    e,
                    delay,
                )
                time.sleep(delay)
    raise requests.RequestException(
        f"Failed to fetch {url} after {MAX_RETRIES} attempts: {last_exception}"
    )


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


def fetch_gw_history(
    season: str,
    gameweek: int | None = None,
    use_cache: bool = True,
) -> pl.DataFrame:
    """Fetch historical gameweek data from vaastav repository.

    Args:
        season: Season string in format "YYYY-YY" (e.g., "2023-24").
        gameweek: Optional specific gameweek number. If None, loads all GWs.
        use_cache: Whether to use cached data if available.

    Returns:
        Polars DataFrame with historical player gameweek stats.

    Raises:
        requests.RequestException: If the API request fails after retries.
    """
    cache_path = _cache_key(season, "gws")

    if use_cache and _is_cache_valid(cache_path):
        logger.info("Using cached vaastav GW history for %s", season)
        df = _load_cache(cache_path)
    else:
        url = f"{VAASTAV_BASE_URL}/data/{season}/gws/merged_gw.csv"
        logger.info("Fetching vaastav GW history from %s", url)
        response = _get_with_retry(url)
        df = pl.read_csv(response.content)
        _save_cache(cache_path, df)

    if gameweek is not None:
        df = df.filter(pl.col("gw") == gameweek)
    return df


def fetch_season_history(season: str, use_cache: bool = True) -> pl.DataFrame:
    """Fetch season summary data from vaastav repository.

    Args:
        season: Season string in format "YYYY-YY".
        use_cache: Whether to use cached data if available.

    Returns:
        Polars DataFrame with season summary stats.

    Raises:
        requests.RequestException: If the API request fails after retries.
    """
    cache_path = _cache_key(season, "players_raw")

    if use_cache and _is_cache_valid(cache_path):
        logger.info("Using cached vaastav season history for %s", season)
        return _load_cache(cache_path)

    url = f"{VAASTAV_BASE_URL}/data/{season}/players_raw.csv"
    logger.info("Fetching vaastav season history from %s", url)
    response = _get_with_retry(url)
    df = pl.read_csv(response.content, null_values=["", "None"], ignore_errors=True)
    _save_cache(cache_path, df)
    return df


def load_historical_data(
    seasons: list[str] | None = None,
    use_cache: bool = True,
) -> pl.DataFrame:
    """Load historical data for multiple seasons.

    Args:
        seasons: List of season strings. Defaults to last 3 seasons.
        use_cache: Whether to use cached data if available.

    Returns:
        Combined Polars DataFrame across all requested seasons.
    """
    if seasons is None:
        seasons = ["2021-22", "2022-23", "2023-24"]

    frames: list[pl.DataFrame] = []
    errors: list[dict[str, Any]] = []

    for season in seasons:
        try:
            df = fetch_gw_history(season, use_cache=use_cache)
            frames.append(df)
        except Exception as e:  # noqa: BLE001
            error_info = {"season": season, "error": str(e), "timestamp": time.time()}
            errors.append(error_info)
            logger.warning("Failed to load season %s: %s", season, e)

    if errors:
        _log_errors_to_mlflow(errors)

    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal")


def _log_errors_to_mlflow(errors: list[dict[str, Any]]) -> None:
    """Log parsing errors to MLflow for local tracking.

    Args:
        errors: List of error dictionaries with season, error message, and timestamp.
    """
    try:
        from src.utils.mlflow_client import _get_mlflow

        mlflow = _get_mlflow()
        if mlflow is None:
            logger.debug("MLflow not available, skipping error logging")
            return

        mlflow.set_tracking_uri("mlruns")
        mlflow.set_experiment("fpl_data_ingestion")
        with mlflow.start_run(run_name="vaastav_ingestion_errors"):
            mlflow.log_param("total_errors", len(errors))
            for i, err in enumerate(errors):
                mlflow.log_param(f"error_season_{i}", err["season"])
                mlflow.log_param(f"error_msg_{i}", err["error"])
            logger.info("Logged %d ingestion errors to MLflow", len(errors))
    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to log errors to MLflow: %s", e)


def clear_cache(season: str | None = None) -> None:
    """Clear cached vaastav data.

    Args:
        season: Specific season to clear. If None, clears all seasons.
    """
    if season:
        season_path = _season_dir(season)
        if season_path.exists():
            for f in season_path.glob("*.parquet"):
                f.unlink()
            logger.info("Cleared vaastav cache for season %s", season)
    else:
        if DATA_DIR.exists():
            for f in DATA_DIR.rglob("*.parquet"):
                f.unlink()
            logger.info("Cleared all vaastav cache in %s", DATA_DIR)
