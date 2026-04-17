"""Home/Away H2H features module.

Computes venue-specific H2H features including home advantage factors,
away degradation factors, and venue-specific statistics for players and teams.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/processed/h2h_cache")
CACHE_TTL_SECONDS = 86400  # 24 hours

# Metrics to compute home advantage/away degradation for
ADVANTAGE_METRICS = [
    "total_points",
    "goals_scored",
    "assists",
    "xg",
    "xa",
    "shots",
    "ict_index",
    "influence",
    "creativity",
    "threat",
    "clean_sheets",
    "expected_goals",
    "expected_assists",
]

TEAM_ADVANTAGE_METRICS = [
    "home_goals",
    "away_goals",
    "home_xg",
    "away_xg",
    "home_shots",
    "away_shots",
    "home_ppda",
    "away_ppda",
    "home_expected_points",
    "away_expected_points",
]


def _cache_key(func_name: str, params: dict[str, Any]) -> Path:
    """Generate cache file path."""
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


def compute_home_away_h2h(
    player_stats: pl.DataFrame,
    matches: pl.DataFrame,
    seasons: list[str] | None = None,
    windows: list[int] | None = None,
    use_cache: bool = True,
    log_to_mlflow: bool = True,
) -> dict[str, pl.DataFrame]:
    """Compute home/away split H2H features.

    For each player-opponent and team-opponent pair, computes:
    - Home vs away performance splits
    - Home advantage factors (home_avg / away_avg - 1)
    - Away degradation factors (away_avg / home_avg - 1)
    - Venue-specific rolling averages

    Args:
        player_stats: Player gameweek stats with 'was_home' column.
        matches: Match results with home/away team data.
        seasons: List of seasons to include.
        windows: Rolling window sizes.
        use_cache: Whether to use cached data.
        log_to_mlflow: Whether to log to MLflow.

    Returns:
        Dict with 'player_home_away', 'team_home_away',
        'player_advantage', 'team_advantage'.
    """
    if windows is None:
        windows = [3, 5, 10]
    if seasons is None:
        seasons = ["2021-22", "2022-23", "2023-24", "2024-25"]

    cache_params = {"seasons": seasons, "windows": windows}
    cache_path = _cache_key("compute_home_away_h2h", cache_params)

    if use_cache and _is_cache_valid(cache_path):
        logger.info("Using cached home/away H2H features")
        return _load_cache(cache_path)  # type: ignore[return-value]

    # Player home/away splits
    player_ha = _compute_player_home_away(player_stats, windows)

    # Team home/away splits
    team_ha = _compute_team_home_away(matches, windows)

    # Home advantage factors
    player_adv = _compute_player_advantage(player_ha)
    team_adv = _compute_team_advantage(team_ha)

    result = {
        "player_home_away": player_ha,
        "team_home_away": team_ha,
        "player_advantage": player_adv,
        "team_advantage": team_adv,
    }

    # Save cache (combine all into single file)
    if player_ha.shape[0] > 0:
        _save_cache(cache_path, player_ha)

    if log_to_mlflow:
        _log_home_away_to_mlflow(result)

    return result


def _compute_player_home_away(
    player_stats: pl.DataFrame,
    windows: list[int],
) -> pl.DataFrame:
    """Compute player home/away split H2H features."""
    if player_stats.is_empty() or "was_home" not in player_stats.columns:
        return pl.DataFrame()

    # Filter to available metrics
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
    }

    mean_cols = [
        c
        for c in ADVANTAGE_METRICS
        if c in available_cols
        and player_stats.schema[c] in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
    ]
    sum_cols = [c for c in ["goals_scored", "assists", "bonus"] if c in available_cols]

    # Extra numeric columns
    predefined = set(ADVANTAGE_METRICS) | {"goals_scored", "assists", "bonus"}
    extra_numeric = [
        c
        for c in available_cols
        if c not in predefined
        and player_stats.schema[c] in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
    ]

    all_mean_cols = mean_cols + extra_numeric
    all_sum_cols = sum_cols

    # Home stats
    home_stats = player_stats.filter(pl.col("was_home"))
    away_stats = player_stats.filter(~pl.col("was_home"))

    # Overall home/away averages
    home_agg: list[pl.Expr] = []
    away_agg: list[pl.Expr] = []

    for col in all_mean_cols:
        home_agg.append(pl.col(col).mean().alias(f"home_avg_{col}"))
        away_agg.append(pl.col(col).mean().alias(f"away_avg_{col}"))
    for col in all_sum_cols:
        home_agg.append(pl.col(col).sum().alias(f"home_total_{col}"))
        away_agg.append(pl.col(col).sum().alias(f"away_total_{col}"))

    home_agg.append(pl.len().alias("home_appearances"))
    away_agg.append(pl.len().alias("away_appearances"))

    home_result = home_stats.group_by(["player_id", "opponent_team_id"]).agg(home_agg)
    away_result = away_stats.group_by(["player_id", "opponent_team_id"]).agg(away_agg)

    # Merge home and away
    result = home_result.join(
        away_result, on=["player_id", "opponent_team_id"], how="outer"
    )

    # Rolling windows for home/away
    for w in windows:
        # Home rolling
        home_recent = (
            home_stats.sort(["player_id", "opponent_team_id", "gameweek"])
            .group_by(["player_id", "opponent_team_id"])
            .tail(w)
        )
        if not home_recent.is_empty():
            home_roll_exprs = [
                pl.col(col).mean().alias(f"home_{col}_rolling_{w}")
                for col in all_mean_cols
            ]
            home_roll = home_recent.group_by(["player_id", "opponent_team_id"]).agg(
                home_roll_exprs
            )
            result = result.join(
                home_roll, on=["player_id", "opponent_team_id"], how="left"
            )

        # Away rolling
        away_recent = (
            away_stats.sort(["player_id", "opponent_team_id", "gameweek"])
            .group_by(["player_id", "opponent_team_id"])
            .tail(w)
        )
        if not away_recent.is_empty():
            away_roll_exprs = [
                pl.col(col).mean().alias(f"away_{col}_rolling_{w}")
                for col in all_mean_cols
            ]
            away_roll = away_recent.group_by(["player_id", "opponent_team_id"]).agg(
                away_roll_exprs
            )
            result = result.join(
                away_roll, on=["player_id", "opponent_team_id"], how="left"
            )

    # Fill NaN with 0
    float_cols = [
        c for c in result.columns if result.schema[c] in (pl.Float64, pl.Float32)
    ]
    if float_cols:
        result = result.with_columns(pl.col(float_cols).fill_nan(0))

    return result


def _compute_team_home_away(
    matches: pl.DataFrame,
    windows: list[int],
) -> pl.DataFrame:
    """Compute team home/away split H2H features."""
    if matches.is_empty():
        return pl.DataFrame()

    available_cols = set(matches.columns) - {
        "home_team_id",
        "away_team_id",
        "gameweek",
        "season",
        "fixture_id",
        "kickoff_time",
    }

    mean_cols = [
        c
        for c in TEAM_ADVANTAGE_METRICS
        if c in available_cols
        and matches.schema[c] in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
    ]

    # Extra numeric columns
    predefined = set(TEAM_ADVANTAGE_METRICS)
    extra_numeric = [
        c
        for c in available_cols
        if c not in predefined
        and matches.schema[c] in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
    ]

    all_mean_cols = mean_cols + extra_numeric

    # Home team perspective (team playing at home)
    home_exprs = [
        pl.col(col).mean().alias(f"team_home_avg_{col}") for col in all_mean_cols
    ]
    home_exprs.append(pl.len().alias("team_home_matches"))

    home_result = matches.group_by(["home_team_id", "away_team_id"]).agg(home_exprs)

    # Away team perspective: create a view where we swap team IDs and column prefixes
    away_matches = matches.with_columns(
        pl.col("away_team_id").alias("home_team_id"),
        pl.col("home_team_id").alias("away_team_id"),
    )

    # Rename metric columns: home_X becomes away_X, away_X becomes home_X
    rename_map: dict[str, str] = {}
    for col in all_mean_cols:
        if col.startswith("home_"):
            rename_map[col] = col.replace("home_", "away_", 1)
        elif col.startswith("away_"):
            rename_map[col] = col.replace("away_", "home_", 1)

    if rename_map:
        away_matches = away_matches.rename(rename_map)

    away_exprs = [
        pl.col(col).mean().alias(f"team_away_avg_{col}") for col in all_mean_cols
    ]
    away_exprs.append(pl.len().alias("team_away_matches"))

    away_result = away_matches.group_by(["home_team_id", "away_team_id"]).agg(
        away_exprs
    )

    result = home_result.join(
        away_result, on=["home_team_id", "away_team_id"], how="outer"
    )

    # Rolling windows
    for w in windows:
        home_recent = (
            matches.sort(["home_team_id", "away_team_id", "gameweek"])
            .group_by(["home_team_id", "away_team_id"])
            .tail(w)
        )
        if not home_recent.is_empty():
            home_roll = [
                pl.col(col).mean().alias(f"team_home_{col}_rolling_{w}")
                for col in all_mean_cols
            ]
            home_roll_agg = home_recent.group_by(["home_team_id", "away_team_id"]).agg(
                home_roll
            )
            result = result.join(
                home_roll_agg, on=["home_team_id", "away_team_id"], how="left"
            )

    # Fill NaN with 0
    float_cols = [
        c for c in result.columns if result.schema[c] in (pl.Float64, pl.Float32)
    ]
    if float_cols:
        result = result.with_columns(pl.col(float_cols).fill_nan(0))

    return result


def _compute_player_advantage(
    player_ha: pl.DataFrame,
) -> pl.DataFrame:
    """Compute home advantage and away degradation factors for players."""
    if player_ha.is_empty():
        return pl.DataFrame()

    # Get unique metrics from home_avg_ columns
    home_cols = [c for c in player_ha.columns if c.startswith("home_avg_")]
    metrics = [c.replace("home_avg_", "") for c in home_cols]

    advantage_exprs: list[pl.Expr] = []
    for metric in metrics:
        home_col = f"home_avg_{metric}"
        away_col = f"away_avg_{metric}"

        if home_col in player_ha.columns and away_col in player_ha.columns:
            # Home advantage: (home - away) / away
            advantage_exprs.append(
                (
                    (pl.col(home_col) - pl.col(away_col))
                    / pl.col(away_col).clip(lower_bound=0.01)
                ).alias(f"home_advantage_{metric}")
            )
            # Away degradation: (away - home) / home
            advantage_exprs.append(
                (
                    (pl.col(away_col) - pl.col(home_col))
                    / pl.col(home_col).clip(lower_bound=0.01)
                ).alias(f"away_degradation_{metric}")
            )

    if not advantage_exprs:
        return player_ha

    result = player_ha.with_columns(advantage_exprs)

    # Fill NaN/inf with 0
    float_cols = [
        c for c in result.columns if result.schema[c] in (pl.Float64, pl.Float32)
    ]
    if float_cols:
        result = result.with_columns(pl.col(float_cols).fill_nan(0))

    return result


def _compute_team_advantage(
    team_ha: pl.DataFrame,
) -> pl.DataFrame:
    """Compute home advantage and away degradation factors for teams."""
    if team_ha.is_empty():
        return pl.DataFrame()

    # Get unique metrics from team_home_avg_ columns
    home_cols = [c for c in team_ha.columns if c.startswith("team_home_avg_")]
    metrics = [c.replace("team_home_avg_", "") for c in home_cols]

    advantage_exprs: list[pl.Expr] = []
    for metric in metrics:
        home_col = f"team_home_avg_{metric}"
        away_col = f"team_away_avg_{metric}"

        if home_col in team_ha.columns and away_col in team_ha.columns:
            advantage_exprs.append(
                (
                    (pl.col(home_col) - pl.col(away_col))
                    / pl.col(away_col).clip(lower_bound=0.01)
                ).alias(f"team_home_advantage_{metric}")
            )
            advantage_exprs.append(
                (
                    (pl.col(away_col) - pl.col(home_col))
                    / pl.col(home_col).clip(lower_bound=0.01)
                ).alias(f"team_away_degradation_{metric}")
            )

    if not advantage_exprs:
        return team_ha

    result = team_ha.with_columns(advantage_exprs)

    float_cols = [
        c for c in result.columns if result.schema[c] in (pl.Float64, pl.Float32)
    ]
    if float_cols:
        result = result.with_columns(pl.col(float_cols).fill_nan(0))

    return result


def _log_home_away_to_mlflow(result: dict[str, pl.DataFrame]) -> None:
    """Log home/away H2H feature metrics to MLflow."""
    try:
        from src.utils.mlflow_client import _get_mlflow

        mlflow = _get_mlflow()
        if mlflow is None:
            logger.debug("MLflow not available, skipping home/away logging")
            return

        mlflow.set_tracking_uri("mlruns")
        mlflow.set_experiment("fpl_feature_engineering")
        with mlflow.start_run(run_name="home_away_h2h"):
            for name, df in result.items():
                mlflow.log_param(f"{name}_rows", df.shape[0])
                mlflow.log_param(f"{name}_columns", df.shape[1])
    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to log home/away H2H to MLflow: %s", e)


def clear_cache() -> None:
    """Clear all cached home/away H2H data."""
    if CACHE_DIR.exists():
        for f in CACHE_DIR.glob("*.parquet"):
            f.unlink()
        logger.info("Cleared home/away H2H cache in %s", CACHE_DIR)
