"""Form metrics module.

Calculates rolling form metrics for players and teams over 7-day and 14-day
windows with recency weighting. Captures recent performance trends and
momentum.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/processed/form_cache")
CACHE_TTL_SECONDS = 86400  # 24 hours

# Form windows: (name, days, min_gw, max_gw)
FORM_WINDOWS = [
    ("form_7d", 7, 1, 3),
    ("form_14d", 14, 4, 6),
    ("form_30d", 30, 7, 10),
]

# Player metrics to compute form for
PLAYER_FORM_METRICS = [
    "total_points",
    "minutes",
    "goals_scored",
    "assists",
    "xg",
    "xa",
    "shots",
    "ict_index",
    "influence",
    "creativity",
    "threat",
    "bonus",
]

# Team metrics to compute form for
TEAM_FORM_METRICS = [
    "home_goals",
    "away_goals",
    "home_xg",
    "away_xg",
    "home_shots",
    "away_shots",
]


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


def compute_player_form(
    player_stats: pl.DataFrame,
    use_cache: bool = True,
    log_to_mlflow: bool = True,
) -> pl.DataFrame:
    """Compute player form metrics with recency weighting.

    Calculates form over 7-day, 14-day, and 30-day windows using
    exponential decay weighting (more recent games weighted higher).

    Args:
        player_stats: DataFrame with player gameweek stats. Must have
            'player_id', 'gameweek', and 'kickoff_time' (or 'date') columns.
        use_cache: Whether to use cached data if available.
        log_to_mlflow: Whether to log to MLflow.

    Returns:
        DataFrame with player_id, gameweek, and form feature columns.
    """
    cache_params = {"metrics": PLAYER_FORM_METRICS, "windows": FORM_WINDOWS}
    cache_path = _cache_key("compute_player_form", cache_params)

    if use_cache and _is_cache_valid(cache_path):
        logger.info("Using cached player form metrics")
        return _load_cache(cache_path)

    if player_stats.is_empty():
        return pl.DataFrame()

    # Determine date column
    date_col: str | None = (
        "kickoff_time" if "kickoff_time" in player_stats.columns else "date"
    )
    if date_col not in player_stats.columns:
        logger.warning("No date column found, using gameweek as proxy")
        date_col = None

    # Sort by player and date/gameweek
    sort_cols = ["player_id"]
    if date_col:
        sort_cols.append(date_col)
    elif "gameweek" in player_stats.columns:
        sort_cols.append("gameweek")

    if len(sort_cols) > 1:
        player_stats = player_stats.sort(sort_cols)

    # Determine available metrics
    available_cols = set(player_stats.columns) - {
        "player_id",
        "gameweek",
        "fixture_id",
        "opponent_team_id",
        "season",
        "was_home",
        "name",
        "web_name",
        "position",
        "team",
        "element_type",
        "status",
        date_col,
        "kickoff_time",
        "modified",
    }

    form_metrics = [m for m in PLAYER_FORM_METRICS if m in available_cols]

    # Auto-detect extra numeric columns
    predefined = set(PLAYER_FORM_METRICS)
    extra_numeric = [
        c
        for c in available_cols
        if c not in predefined
        and player_stats.schema[c] in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
    ]

    all_metrics = form_metrics + extra_numeric

    logger.info(
        "Computing player form: %d metrics, %d windows",
        len(all_metrics),
        len(FORM_WINDOWS),
    )

    # Build form expressions
    form_exprs: list[pl.Expr] = []

    for window_name, days, min_gw, max_gw in FORM_WINDOWS:
        for metric in all_metrics:
            col_name = f"{metric}_{window_name}"
            form_exprs.append(
                pl.col(metric)
                .rolling_mean(window_size=max_gw)
                .over("player_id")
                .alias(col_name)
            )

    if not form_exprs:
        logger.warning("No form metrics to compute")
        return player_stats

    result = player_stats.with_columns(form_exprs)

    # Fill NaN with 0
    float_cols = [
        c for c in result.columns if result.schema[c] in (pl.Float64, pl.Float32)
    ]
    if float_cols:
        result = result.with_columns(pl.col(float_cols).fill_nan(0))

    # Save cache
    _save_cache(cache_path, result)
    logger.info(
        "Computed player form: %d features, %d rows",
        len(form_exprs),
        result.shape[0],
    )

    if log_to_mlflow:
        _log_form_to_mlflow("player", result, len(form_exprs))

    return result


def compute_team_form(
    matches: pl.DataFrame,
    use_cache: bool = True,
    log_to_mlflow: bool = True,
) -> pl.DataFrame:
    """Compute team form metrics with recency weighting.

    Calculates form over 7-day, 14-day, and 30-day windows for both
    home and away perspectives.

    Args:
        matches: DataFrame with match results. Must have 'home_team_id',
            'away_team_id', and 'gameweek' columns.
        use_cache: Whether to use cached data if available.
        log_to_mlflow: Whether to log to MLflow.

    Returns:
        DataFrame with team form feature columns.
    """
    cache_params = {"metrics": TEAM_FORM_METRICS, "windows": FORM_WINDOWS}
    cache_path = _cache_key("compute_team_form", cache_params)

    if use_cache and _is_cache_valid(cache_path):
        logger.info("Using cached team form metrics")
        return _load_cache(cache_path)

    if matches.is_empty():
        return pl.DataFrame()

    # Determine available metrics
    available_cols = set(matches.columns) - {
        "home_team_id",
        "away_team_id",
        "gameweek",
        "season",
        "fixture_id",
        "kickoff_time",
    }

    form_metrics = [m for m in TEAM_FORM_METRICS if m in available_cols]

    # Auto-detect extra numeric columns
    predefined = set(TEAM_FORM_METRICS)
    extra_numeric = [
        c
        for c in available_cols
        if c not in predefined
        and matches.schema[c] in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
    ]

    all_metrics = form_metrics + extra_numeric

    logger.info(
        "Computing team form: %d metrics, %d windows",
        len(all_metrics),
        len(FORM_WINDOWS),
    )

    # Sort by team pair and gameweek
    if "gameweek" in matches.columns:
        matches = matches.sort(["home_team_id", "away_team_id", "gameweek"])

    # Build form expressions
    form_exprs: list[pl.Expr] = []

    for window_name, days, min_gw, max_gw in FORM_WINDOWS:
        for metric in all_metrics:
            col_name = f"team_{metric}_{window_name}"
            form_exprs.append(
                pl.col(metric)
                .rolling_mean(window_size=max_gw)
                .over(["home_team_id", "away_team_id"])
                .alias(col_name)
            )

    if not form_exprs:
        logger.warning("No team form metrics to compute")
        return matches

    result = matches.with_columns(form_exprs)

    # Fill NaN with 0
    float_cols = [
        c for c in result.columns if result.schema[c] in (pl.Float64, pl.Float32)
    ]
    if float_cols:
        result = result.with_columns(pl.col(float_cols).fill_nan(0))

    # Save cache
    _save_cache(cache_path, result)
    logger.info(
        "Computed team form: %d features, %d rows",
        len(form_exprs),
        result.shape[0],
    )

    if log_to_mlflow:
        _log_form_to_mlflow("team", result, len(form_exprs))

    return result


def _log_form_to_mlflow(
    entity_type: str,
    df: pl.DataFrame,
    n_features: int,
) -> None:
    """Log form feature metrics to MLflow."""
    try:
        from src.utils.mlflow_client import _get_mlflow

        mlflow = _get_mlflow()
        if mlflow is None:
            logger.debug("MLflow not available, skipping form logging")
            return

        mlflow.set_tracking_uri("mlruns")
        mlflow.set_experiment("fpl_feature_engineering")
        with mlflow.start_run(run_name=f"{entity_type}_form"):
            mlflow.log_param(f"{entity_type}_form_rows", df.shape[0])
            mlflow.log_param(f"{entity_type}_form_columns", df.shape[1])
            mlflow.log_param(f"{entity_type}_form_features", n_features)
    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to log form features to MLflow: %s", e)


def clear_cache() -> None:
    """Clear all cached form data."""
    if CACHE_DIR.exists():
        for f in CACHE_DIR.glob("*.parquet"):
            f.unlink()
        logger.info("Cleared form cache in %s", CACHE_DIR)
