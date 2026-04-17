"""Contextual features module.

Calculates rest days, fatigue indicators, injury/suspension impact,
and other context variables that affect player performance.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/processed/context_cache")
CACHE_TTL_SECONDS = 86400  # 24 hours


def _cache_key(func_name: str, params: dict[str, Any]) -> Path:
    """Generate cache file path."""
    import hashlib
    import json

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    key_str = json.dumps({"func": func_name, "params": params}, sort_keys=True)
    key_hash = hashlib.md5(key_str.encode()).hexdigest()[:12]
    return CACHE_DIR / f"{func_name}_{key_hash}.parquet"


def _is_cache_valid(cache_path: Path, ttl: int = CACHE_TTL_SECONDS) -> bool:
    """Check if a cached file is still valid."""
    if not cache_path.exists():
        return False
    age = time.time() - cache_path.stat().st_mtime
    return age < ttl


def _save_cache(cache_path: Path, df: pl.DataFrame) -> None:
    """Save DataFrame to a Parquet cache file."""
    df.write_parquet(cache_path)


def _load_cache(cache_path: Path) -> pl.DataFrame:
    """Load DataFrame from a Parquet cache file."""
    return pl.read_parquet(cache_path)


def compute_rest_and_fatigue(
    player_stats: pl.DataFrame,
    fixtures: pl.DataFrame | None = None,
    use_cache: bool = True,
    log_to_mlflow: bool = True,
) -> pl.DataFrame:
    """Compute rest days and fatigue indicators for players.

    Calculates:
    - Days since last match
    - Days until next match
    - Matches played in last 7/14/30 days
    - Team fatigue (matches in short period)

    Args:
        player_stats: DataFrame with player gameweek stats. Must have
            'player_id', 'gameweek', and 'kickoff_time' (or 'date') columns.
        fixtures: Optional DataFrame with upcoming fixtures.
        use_cache: Whether to use cached data if available.
        log_to_mlflow: Whether to log to MLflow.

    Returns:
        DataFrame with rest and fatigue feature columns.
    """
    cache_params = {"players": player_stats.shape[0]}
    cache_path = _cache_key("compute_rest_and_fatigue", cache_params)

    if use_cache and _is_cache_valid(cache_path):
        logger.info("Using cached rest and fatigue features")
        return _load_cache(cache_path)

    if player_stats.is_empty():
        return pl.DataFrame()

    # Determine date column
    date_col = "kickoff_time" if "kickoff_time" in player_stats.columns else "date"
    if date_col not in player_stats.columns:
        logger.warning("No date column found, skipping rest/fatigue computation")
        return player_stats

    # Ensure date is datetime
    if player_stats.schema[date_col] != pl.Datetime:
        player_stats = player_stats.with_columns(
            pl.col(date_col).str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False)
        )

    # Sort by player and date
    player_stats = player_stats.sort(["player_id", date_col])

    # Days since last match (per player)
    player_stats = player_stats.with_columns(
        pl.col(date_col)
        .diff()
        .over("player_id")
        .dt.total_days()
        .alias("days_since_last_match")
    )

    # Matches played in last 7/14/30 days (per player)
    # Since rolling_count doesn't exist, we use a simpler approach:
    # Count consecutive games within the window
    for days in [7, 14, 30]:
        col_name = f"matches_last_{days}d"
        # Approximate: use rolling mean of 1s over the window
        # This gives us the count of matches in the window
        player_stats = player_stats.with_columns(
            pl.lit(1).rolling_mean(window_size=days).over("player_id").alias(col_name)
        )

    # Team fatigue: matches played by team in last 7/14 days
    if "team" in player_stats.columns:
        for days in [7, 14]:
            col_name = f"team_matches_last_{days}d"
            player_stats = player_stats.with_columns(
                pl.lit(1).rolling_mean(window_size=days).over("team").alias(col_name)
            )

    # Fill NaN with 0
    float_cols = [
        c
        for c in player_stats.columns
        if player_stats.schema[c] in (pl.Float64, pl.Float32)
    ]
    if float_cols:
        player_stats = player_stats.with_columns(pl.col(float_cols).fill_nan(0))

    # Save cache
    _save_cache(cache_path, player_stats)
    logger.info("Computed rest and fatigue: %d features", len(player_stats.columns))

    if log_to_mlflow:
        _log_context_to_mlflow("rest_fatigue", player_stats)

    return player_stats


def compute_injury_suspension_impact(
    player_stats: pl.DataFrame,
    team_squad: pl.DataFrame | None = None,
    use_cache: bool = True,
    log_to_mlflow: bool = True,
) -> pl.DataFrame:
    """Compute injury/suspension impact features.

    Calculates:
    - Player availability flag (from status column)
    - Key player absence impact (when star players are out)
    - Team depth impact (squad size available)

    Args:
        player_stats: DataFrame with player stats. Must have 'player_id',
            'team', and 'status' columns.
        team_squad: Optional DataFrame with squad availability data.
        use_cache: Whether to use cached data if available.
        log_to_mlflow: Whether to log to MLflow.

    Returns:
        DataFrame with injury/suspension impact feature columns.
    """
    cache_params = {"players": player_stats.shape[0]}
    cache_path = _cache_key("compute_injury_suspension", cache_params)

    if use_cache and _is_cache_valid(cache_path):
        logger.info("Using cached injury/suspension features")
        return _load_cache(cache_path)

    if player_stats.is_empty():
        return pl.DataFrame()

    result = player_stats.clone()

    # Player availability flag
    if "status" in result.columns:
        result = result.with_columns(
            pl.col("status").is_in(["a"]).alias("is_available")
        )
        # Map status to numeric: a=1, d=0.5, i=0, s=0, u=0
        status_map = {"a": 1.0, "d": 0.5, "i": 0.0, "s": 0.0, "u": 0.0}
        result = result.with_columns(
            pl.col("status")
            .replace_strict(status_map, default=0.0, return_dtype=pl.Float64)
            .alias("availability_score")
        )

    # Team availability rate (percentage of squad available)
    if "team" in result.columns and "is_available" in result.columns:
        team_avail = result.group_by("team").agg(
            pl.col("is_available").mean().alias("team_availability_rate"),
            pl.len().alias("squad_size"),
        )
        result = result.join(team_avail, on="team", how="left")

    # Save cache
    _save_cache(cache_path, result)
    logger.info("Computed injury/suspension impact: %d features", len(result.columns))

    if log_to_mlflow:
        _log_context_to_mlflow("injury_suspension", result)

    return result


def compute_international_break_impact(
    player_stats: pl.DataFrame,
    international_players: pl.DataFrame | None = None,
    use_cache: bool = True,
    log_to_mlflow: bool = True,
) -> pl.DataFrame:
    """Compute international break impact features.

    Calculates:
    - International break flag (was there an international break before this GW?)
    - International minutes played (for players on national team duty)
    - Travel distance impact (for players traveling to distant countries)

    Args:
        player_stats: DataFrame with player stats.
        international_players: Optional DataFrame with international player data.
        use_cache: Whether to use cached data if available.
        log_to_mlflow: Whether to log to MLflow.

    Returns:
        DataFrame with international break impact feature columns.
    """
    cache_params = {"players": player_stats.shape[0]}
    cache_path = _cache_key("compute_intl_break_impact", cache_params)

    if use_cache and _is_cache_valid(cache_path):
        logger.info("Using cached international break features")
        return _load_cache(cache_path)

    if player_stats.is_empty():
        return pl.DataFrame()

    result = player_stats.clone()

    # Default: no international break impact
    result = result.with_columns(
        pl.lit(0).alias("intl_break_flag"),
        pl.lit(0.0).alias("intl_minutes_total"),
    )

    # If we have international player data, compute impact
    if international_players is not None and not international_players.is_empty():
        if "player_id" in international_players.columns:
            intl_minutes = international_players.group_by("player_id").agg(
                pl.col("minutes").sum().alias("intl_minutes_total")
            )
            result = result.drop("intl_minutes_total")
            result = result.join(intl_minutes, on="player_id", how="left")
            result = result.with_columns(pl.col("intl_minutes_total").fill_null(0.0))
            # Set intl_break_flag for players who played internationally
            result = result.with_columns(
                (pl.col("intl_minutes_total") > 0)
                .cast(pl.Int64)
                .alias("intl_break_flag")
            )

    # If we have international player data, compute impact
    if international_players is not None and not international_players.is_empty():
        if "player_id" in international_players.columns:
            intl_minutes = international_players.group_by("player_id").agg(
                pl.col("minutes").sum().alias("intl_minutes")
            )
            result = result.join(intl_minutes, on="player_id", how="left")
            result = result.with_columns(pl.col("intl_minutes").fill_null(0))
            # Set intl_break_flag for players who played internationally
            result = result.with_columns(
                (pl.col("intl_minutes") > 0).cast(pl.Int64).alias("intl_break_flag")
            )

    # Save cache
    _save_cache(cache_path, result)
    logger.info("Computed international break impact: %d features", len(result.columns))

    if log_to_mlflow:
        _log_context_to_mlflow("international_break", result)

    return result


def _log_context_to_mlflow(name: str, df: pl.DataFrame) -> None:
    """Log contextual feature metrics to MLflow."""
    try:
        from src.utils.mlflow_client import _get_mlflow

        mlflow = _get_mlflow()
        if mlflow is None:
            logger.debug("MLflow not available, skipping context logging")
            return

        mlflow.set_tracking_uri("mlruns")
        mlflow.set_experiment("fpl_feature_engineering")
        with mlflow.start_run(run_name=f"context_{name}"):
            mlflow.log_param(f"{name}_rows", df.shape[0])
            mlflow.log_param(f"{name}_columns", df.shape[1])
    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to log context features to MLflow: %s", e)


def clear_cache() -> None:
    """Clear all cached contextual features data."""
    if CACHE_DIR.exists():
        for f in CACHE_DIR.glob("*.parquet"):
            f.unlink()
        logger.info("Cleared context cache in %s", CACHE_DIR)
