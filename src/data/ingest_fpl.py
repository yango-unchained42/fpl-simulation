"""FPL API data ingestion module.

Fetches current-season data from the Fantasy Premier League API
including player info, gameweek stats, team info, and fixtures.
Implements caching, retry logic, and Supabase integration.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

import polars as pl
import requests

from src.config import (
    CACHE_TTL_SECONDS,
    CURRENT_SEASON,
    FPL_API_BASE,
    MAX_RETRIES,
    RAW_DATA_DIR,
    RETRY_DELAY_SECONDS,
)

logger = logging.getLogger(__name__)

DATA_DIR = RAW_DATA_DIR / "fpl"


def _season_dir(season: str = CURRENT_SEASON) -> Path:
    """Get the season-specific data directory.

    Args:
        season: Season string (e.g., "2025-26").

    Returns:
        Path to the season directory.
    """
    dir_path = DATA_DIR / season
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


def _cache_key(endpoint: str, season: str = CURRENT_SEASON) -> Path:
    """Generate cache file path for an API endpoint.

    Args:
        endpoint: API endpoint name (e.g., "bootstrap-static").
        season: Season string.

    Returns:
        Path to the cache file.
    """
    return _season_dir(season) / f"{endpoint}.json"


def _get_with_retry(url: str, timeout: int = 30) -> requests.Response:
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


def _load_cache(cache_path: Path) -> dict[str, Any]:
    """Load data from a cache file.

    Args:
        cache_path: Path to the cache file.

    Returns:
        Parsed JSON data.
    """
    with open(cache_path, encoding="utf-8") as f:
        return json.load(f)  # type: ignore[no-any-return]


def _save_cache(cache_path: Path, data: dict[str, Any]) -> None:
    """Save data to a cache file.

    Args:
        cache_path: Path to the cache file.
        data: Data to cache as JSON.
    """
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(data, f)


def fetch_bootstrap_static(
    use_cache: bool = True,
    season: str = CURRENT_SEASON,
) -> dict[str, Any]:
    """Fetch the FPL bootstrap-static endpoint with caching.

    Args:
        use_cache: Whether to use cached data if available.
        season: Season string for directory organization.

    Returns:
        Dict containing players, teams, events, and game settings.

    Raises:
        requests.RequestException: If the API request fails after retries.
    """
    cache_path = _cache_key("bootstrap-static", season)

    if use_cache and _is_cache_valid(cache_path):
        logger.info("Using cached bootstrap-static data")
        return _load_cache(cache_path)

    url = f"{FPL_API_BASE}bootstrap-static/"
    logger.info("Fetching bootstrap-static from FPL API")
    response = _get_with_retry(url)
    data = response.json()

    if use_cache:
        _save_cache(cache_path, data)
        logger.info("Cached bootstrap-static data to %s", cache_path)

    return data  # type: ignore[no-any-return]


def fetch_fixtures(
    use_cache: bool = True,
    season: str = CURRENT_SEASON,
) -> pl.DataFrame:
    """Fetch all fixtures from the FPL API fixtures endpoint.

    Args:
        use_cache: Whether to use cached data if available.
        season: Season string for directory organization.

    Returns:
        Polars DataFrame with all fixtures for the season.
    """
    cache_path = _cache_key("fixtures", season)

    if use_cache and _is_cache_valid(cache_path, ttl=86400):
        logger.info("Using cached fixtures data")
        cached = _load_cache(cache_path)
        return pl.DataFrame(cached)

    url = f"{FPL_API_BASE}fixtures/"
    logger.info("Fetching fixtures from FPL API")
    response = _get_with_retry(url)
    fixtures = response.json()

    if use_cache:
        _save_cache(cache_path, fixtures)
        logger.info("Cached fixtures data to %s", cache_path)

    if not fixtures:
        return pl.DataFrame(
            schema={
                "id": pl.Int64,
                "event": pl.Int64,
                "team_h": pl.Int64,
                "team_a": pl.Int64,
                "started": pl.Boolean,
                "finished": pl.Boolean,
                "team_h_score": pl.Int64,
                "team_a_score": pl.Int64,
                "kickoff_time": pl.String,
            }
        )
    return pl.DataFrame(fixtures)


def fetch_player_history(
    player_ids: list[int] | None = None,
    use_cache: bool = True,
    season: str = CURRENT_SEASON,
) -> pl.DataFrame:
    """Fetch gameweek-by-gameweek history for players from FPL API.

    Fetches the `element-summary/{id}/` endpoint for each player to get
    detailed match data including minutes, goals, assists, xG, xA, etc.
    Caches as a single Parquet file per season.

    Args:
        player_ids: List of player IDs to fetch. If None, fetches all players
            from bootstrap-static.
        use_cache: Whether to use cached data if available.
        season: Season string for directory organization.

    Returns:
        Polars DataFrame with player gameweek history.
    """
    cache_path = _season_dir(season) / "player_history.parquet"

    if use_cache and cache_path.exists() and _is_cache_valid(cache_path, ttl=86400):
        logger.info("Using cached player history data")
        return pl.read_parquet(cache_path)

    if player_ids is None:
        static_data = fetch_bootstrap_static(use_cache=use_cache, season=season)
        elements = static_data.get("elements", [])
        player_ids = [e["id"] for e in elements]

    frames: list[pl.DataFrame] = []
    for pid in player_ids:
        url = f"{FPL_API_BASE}element-summary/{pid}/"
        try:
            response = _get_with_retry(url)
            data = response.json()
            history = data.get("history", [])
            if history:
                df = pl.DataFrame(history)
                df = df.with_columns(pl.lit(pid).alias("player_id"))
                frames.append(df)
        except requests.RequestException as e:
            logger.warning("Failed to fetch history for player %d: %s", pid, e)

        # Small delay to avoid rate limiting
        time.sleep(0.1)

    if not frames:
        return pl.DataFrame()

    result = pl.concat(frames, how="diagonal")

    if use_cache:
        result.write_parquet(cache_path)
        logger.info(
            "Cached player history to %s (%d rows)", cache_path, result.shape[0]
        )

    return result


def parse_players(data: dict[str, Any]) -> pl.DataFrame:
    """Parse player elements from bootstrap-static data.

    Args:
        data: Raw bootstrap-static JSON response.

    Returns:
        Polars DataFrame with player information.
    """
    elements = data.get("elements", [])
    if not elements:
        return pl.DataFrame(
            schema={
                "id": pl.Int64,
                "first_name": pl.String,
                "second_name": pl.String,
                "web_name": pl.String,
                "team": pl.Int64,
                "element_type": pl.Int64,
                "now_cost": pl.Int64,
                "total_points": pl.Int64,
                "selected_by_percent": pl.String,
                "status": pl.String,
            }
        )
    df = pl.DataFrame(elements)
    return df


def parse_teams(data: dict[str, Any]) -> pl.DataFrame:
    """Parse team elements from bootstrap-static data.

    Args:
        data: Raw bootstrap-static JSON response.

    Returns:
        Polars DataFrame with team information.
    """
    teams = data.get("teams", [])
    if not teams:
        return pl.DataFrame(
            schema={
                "id": pl.Int64,
                "name": pl.String,
                "short_name": pl.String,
                "strength": pl.Int64,
                "strength_overall_home": pl.Int64,
                "strength_overall_away": pl.Int64,
                "strength_attack_home": pl.Int64,
                "strength_attack_away": pl.Int64,
                "strength_defence_home": pl.Int64,
                "strength_defence_away": pl.Int64,
            }
        )
    df = pl.DataFrame(teams)
    return df


def ingest_fpl_data(
    use_cache: bool = True,
    season: str = CURRENT_SEASON,
) -> dict[str, pl.DataFrame]:
    """Ingest all data from the FPL API.

    Args:
        use_cache: Whether to use cached data if available.
        season: Season string for directory organization.

    Returns:
        Dict with keys 'players', 'teams', 'fixtures', 'player_history'
        mapping to their respective Polars DataFrames.
    """
    data = fetch_bootstrap_static(use_cache=use_cache, season=season)
    player_history = fetch_player_history(use_cache=use_cache, season=season)
    fixtures = fetch_fixtures(use_cache=use_cache, season=season)

    teams_df = parse_teams(data)

    # Auto-generate and append team mappings
    try:
        from src.data.team_mappings import (  # noqa: I001
            append_mappings,
            create_fpl_mappings,
            get_fpl_team_id,
        )

        # Check if we already have mappings for this season
        existing_id = get_fpl_team_id(season, "fpl", 1)
        if existing_id is None:
            new_mappings = create_fpl_mappings(season, teams_df)
            append_mappings(new_mappings)
            logger.info("Auto-generated FPL team mappings for %s", season)
        else:
            logger.debug("FPL team mappings already exist for %s", season)
    except Exception as e:
        logger.warning("Could not auto-generate team mappings: %s", e)

    return {
        "players": parse_players(data),
        "teams": teams_df,
        "fixtures": fixtures,
        "player_history": player_history,
    }


def clear_cache(season: str | None = None) -> None:
    """Clear cached FPL API data.

    Args:
        season: Specific season to clear. If None, clears all seasons.
    """
    if season:
        season_path = _season_dir(season)
        if season_path.exists():
            for f in season_path.rglob("*.json"):
                f.unlink()
            logger.info("Cleared FPL cache for season %s", season)
    else:
        if DATA_DIR.exists():
            for f in DATA_DIR.rglob("*.json"):
                f.unlink()
            logger.info("Cleared all FPL cache in %s", DATA_DIR)
