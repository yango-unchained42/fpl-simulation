"""Head-to-head metrics module.

Computes opponent-specific H2H features for players and teams across ALL
available metrics (core, defensive, advanced, ICT, market). Includes
recency weighting, rolling windows, and home/away splits.
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

# Metrics to compute H2H averages for (player vs opponent team)
PLAYER_H2H_MEAN_METRICS = [
    # Core
    "total_points",
    "minutes",
    "expected_goals",
    "expected_assists",
    "expected_goal_involvements",
    "expected_goals_conceded",
    "ict_index",
    "influence",
    "creativity",
    "threat",
    # Defensive
    "clean_sheets",
    "goals_conceded",
    "saves",
    "tackles",
    "clearances_blocks_interceptions",
    "recoveries",
    "defensive_contribution",
    # Advanced (Understat)
    "xg",
    "xa",
    "xg_chain",
    "xg_buildup",
    "key_passes",
    "shots",
    # Market
    "value",
    "selected",
    "transfers_in",
    "transfers_out",
    "form",
    # Discipline
    "yellow_cards",
    "red_cards",
    "own_goals",
    "penalties_missed",
    "penalties_saved",
]

PLAYER_H2H_SUM_METRICS = [
    "goals_scored",
    "assists",
    "bonus",
]

# Metrics to compute H2H averages for (team vs opponent team)
TEAM_H2H_MEAN_METRICS = [
    # Attacking
    "home_xg",
    "away_xg",
    "home_goals",
    "away_goals",
    "home_shots",
    "away_shots",
    "home_ppda",
    "away_ppda",
    "home_deep_completions",
    "away_deep_completions",
    "home_expected_points",
    "away_expected_points",
    "home_np_xg",
    "away_np_xg",
]

TEAM_H2H_SUM_METRICS = [
    "home_goals",
    "away_goals",
]

# Columns to exclude from H2H computation
EXCLUDE_COLUMNS = {
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
    "home_team_id",
    "away_team_id",
    "kickoff_time",
    "modified",
    "fixture",
    "round",
    "GW",
    "element",
    "player",
    "league_id",
    "season_id",
    "team_id",
    "game_id",
    "position_id",
    # Per-90 rates (already normalized)
    "clean_sheets_per_90",
    "goals_conceded_per_90",
    "saves_per_90",
    "expected_goals_conceded_per_90",
    "expected_goals_per_90",
    "expected_assists_per_90",
    "expected_goal_involvements_per_90",
    "defensive_contribution_per_90",
    # Rankings
    "now_cost_rank",
    "now_cost_rank_type",
    "points_per_game_rank",
    "points_per_game_rank_type",
    "form_rank",
    "form_rank_type",
    "selected_rank",
    "selected_rank_type",
    "ict_index_rank",
    "ict_index_rank_type",
    "influence_rank",
    "influence_rank_type",
    "creativity_rank",
    "creativity_rank_type",
    "threat_rank",
    "threat_rank_type",
}


def _cache_key(func_name: str, params: dict[str, Any]) -> Path:
    """Generate cache file path based on function name and parameters."""
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


def compute_player_vs_team(
    player_stats: pl.DataFrame,
    seasons: list[str] | None = None,
    windows: list[int] | None = None,
    use_cache: bool = True,
    log_to_mlflow: bool = True,
) -> pl.DataFrame:
    """Compute player vs opponent team H2H metrics for ALL available metrics.

    For each (player_id, opponent_team_id) pair, computes:
    - Overall averages for all metrics
    - Rolling averages for last 3/5/10 meetings
    - Home/away split averages
    - Recent form (last 5 meetings)

    Args:
        player_stats: DataFrame with player gameweek stats. Must have
            'player_id', 'opponent_team_id', and 'gameweek' columns.
        seasons: List of seasons to include. Defaults to last 3.
        windows: Rolling window sizes. Defaults to [3, 5, 10].
        use_cache: Whether to use cached data if available.
        log_to_mlflow: Whether to log to MLflow.

    Returns:
        DataFrame with player vs team H2H metrics.
    """
    if windows is None:
        windows = [3, 5, 10]
    if seasons is None:
        seasons = ["2021-22", "2022-23", "2023-24", "2024-25"]

    cache_params = {"seasons": seasons, "windows": windows}
    cache_path = _cache_key("compute_player_vs_team_v2", cache_params)

    if use_cache and _is_cache_valid(cache_path):
        logger.info("Using cached player vs team H2H metrics")
        return _load_cache(cache_path)

    if player_stats.is_empty():
        return pl.DataFrame()

    # Filter to requested seasons
    if "season" in player_stats.columns:
        player_stats = player_stats.filter(pl.col("season").is_in(seasons))

    # Determine available metrics
    available_cols = set(player_stats.columns) - EXCLUDE_COLUMNS
    mean_cols = [c for c in PLAYER_H2H_MEAN_METRICS if c in available_cols]
    sum_cols = [c for c in PLAYER_H2H_SUM_METRICS if c in available_cols]

    # Auto-detect extra numeric columns
    predefined = set(PLAYER_H2H_MEAN_METRICS) | set(PLAYER_H2H_SUM_METRICS)
    extra_numeric = [
        c
        for c in available_cols
        if c not in predefined
        and player_stats.schema[c] in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
    ]

    all_mean_cols = mean_cols + extra_numeric
    all_sum_cols = sum_cols

    logger.info(
        "Computing player vs team H2H: %d mean, %d sum, %d extra metrics",
        len(all_mean_cols),
        len(all_sum_cols),
        len(extra_numeric),
    )

    # Sort by player, opponent, gameweek
    if "gameweek" in player_stats.columns:
        player_stats = player_stats.sort(["player_id", "opponent_team_id", "gameweek"])

    # Build aggregation expressions
    agg_exprs: list[pl.Expr] = []

    # Overall averages
    for col in all_mean_cols:
        agg_exprs.append(pl.col(col).mean().alias(f"avg_{col}"))
    for col in all_sum_cols:
        agg_exprs.append(pl.col(col).sum().alias(f"total_{col}"))
    agg_exprs.append(pl.len().alias("appearances"))

    # Overall H2H
    overall = player_stats.group_by(["player_id", "opponent_team_id"]).agg(agg_exprs)

    # Rolling windows (last N meetings)
    for w in windows:
        # Get last w meetings per player-opponent pair
        recent = player_stats.group_by(["player_id", "opponent_team_id"]).tail(w)
        rolling_exprs: list[pl.Expr] = []
        for col in all_mean_cols:
            rolling_exprs.append(pl.col(col).mean().alias(f"{col}_h2h_last_{w}"))
        for col in all_sum_cols:
            rolling_exprs.append(pl.col(col).sum().alias(f"{col}_h2h_sum_last_{w}"))

        if rolling_exprs:
            rolling = recent.group_by(["player_id", "opponent_team_id"]).agg(
                rolling_exprs
            )
            overall = overall.join(
                rolling, on=["player_id", "opponent_team_id"], how="left"
            )

    # Home/away splits
    if "was_home" in player_stats.columns:
        for suffix, condition in [
            ("home", pl.col("was_home")),
            ("away", ~pl.col("was_home")),
        ]:
            split_data = player_stats.filter(condition)
            if not split_data.is_empty():
                split_exprs: list[pl.Expr] = []
                for col in all_mean_cols:
                    split_exprs.append(pl.col(col).mean().alias(f"avg_{col}_{suffix}"))
                for col in all_sum_cols:
                    split_exprs.append(pl.col(col).sum().alias(f"total_{col}_{suffix}"))

                split_agg = split_data.group_by(["player_id", "opponent_team_id"]).agg(
                    split_exprs
                )
                overall = overall.join(
                    split_agg, on=["player_id", "opponent_team_id"], how="left"
                )

    # Recent form (last 5 meetings)
    if "gameweek" in player_stats.columns:
        recent_5 = player_stats.group_by(["player_id", "opponent_team_id"]).tail(5)
        if not recent_5.is_empty():
            form_exprs: list[pl.Expr] = []
            for col in all_mean_cols:
                form_exprs.append(pl.col(col).mean().alias(f"recent_{col}_last_5"))
            for col in all_sum_cols:
                form_exprs.append(pl.col(col).sum().alias(f"recent_{col}_sum_last_5"))

            form = recent_5.group_by(["player_id", "opponent_team_id"]).agg(form_exprs)
            overall = overall.join(
                form, on=["player_id", "opponent_team_id"], how="left"
            )

    # Fill NaN with 0
    float_cols = [
        c for c in overall.columns if overall.schema[c] in (pl.Float64, pl.Float32)
    ]
    if float_cols:
        overall = overall.with_columns(pl.col(float_cols).fill_nan(0))

    _save_cache(cache_path, overall)
    logger.info(
        "Computed player vs team H2H: %d pairs, %d columns",
        overall.shape[0],
        overall.shape[1],
    )

    if log_to_mlflow:
        _log_h2h_to_mlflow("player_vs_team", overall)

    return overall


def compute_team_h2h(
    matches: pl.DataFrame,
    seasons: list[str] | None = None,
    windows: list[int] | None = None,
    use_cache: bool = True,
    log_to_mlflow: bool = True,
) -> pl.DataFrame:
    """Compute team vs opponent team H2H metrics for ALL available metrics.

    For each (home_team_id, away_team_id) pair, computes:
    - Overall averages for all metrics
    - Rolling averages for last 3/5/10 meetings
    - Home/away perspective splits
    - Recent form (last 5 meetings)

    Args:
        matches: DataFrame with match results. Must have 'home_team_id',
            'away_team_id', and 'gameweek' columns.
        seasons: List of seasons to include. Defaults to last 3.
        windows: Rolling window sizes. Defaults to [3, 5, 10].
        use_cache: Whether to use cached data if available.
        log_to_mlflow: Whether to log to MLflow.

    Returns:
        DataFrame with team vs team H2H metrics.
    """
    if windows is None:
        windows = [3, 5, 10]
    if seasons is None:
        seasons = ["2021-22", "2022-23", "2023-24", "2024-25"]

    cache_params = {"seasons": seasons, "windows": windows}
    cache_path = _cache_key("compute_team_h2h_v2", cache_params)

    if use_cache and _is_cache_valid(cache_path):
        logger.info("Using cached team H2H metrics")
        return _load_cache(cache_path)

    if matches.is_empty():
        return pl.DataFrame()

    # Filter to requested seasons
    if "season" in matches.columns:
        matches = matches.filter(pl.col("season").is_in(seasons))

    # Determine available metrics
    available_cols = set(matches.columns) - {
        "home_team_id",
        "away_team_id",
        "gameweek",
        "season",
        "fixture_id",
        "kickoff_time",
    }
    mean_cols = [c for c in TEAM_H2H_MEAN_METRICS if c in available_cols]
    sum_cols = [c for c in TEAM_H2H_SUM_METRICS if c in available_cols]

    # Auto-detect extra numeric columns
    predefined = set(TEAM_H2H_MEAN_METRICS) | set(TEAM_H2H_SUM_METRICS)
    extra_numeric = [
        c
        for c in available_cols
        if c not in predefined
        and matches.schema[c] in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
    ]

    all_mean_cols = mean_cols + extra_numeric
    all_sum_cols = sum_cols

    logger.info(
        "Computing team H2H: %d mean, %d sum, %d extra metrics",
        len(all_mean_cols),
        len(all_sum_cols),
        len(extra_numeric),
    )

    # Sort by team pair, gameweek
    if "gameweek" in matches.columns:
        matches = matches.sort(["home_team_id", "away_team_id", "gameweek"])

    # Build aggregation expressions
    agg_exprs: list[pl.Expr] = []
    for col in all_mean_cols:
        agg_exprs.append(pl.col(col).mean().alias(f"avg_{col}"))
    for col in all_sum_cols:
        agg_exprs.append(pl.col(col).sum().alias(f"total_{col}"))
    agg_exprs.append(pl.len().alias("total_matches"))

    # Overall H2H
    overall = matches.group_by(["home_team_id", "away_team_id"]).agg(agg_exprs)

    # Rolling windows
    for w in windows:
        recent = matches.group_by(["home_team_id", "away_team_id"]).tail(w)
        rolling_exprs: list[pl.Expr] = []
        for col in all_mean_cols:
            rolling_exprs.append(pl.col(col).mean().alias(f"{col}_h2h_last_{w}"))
        for col in all_sum_cols:
            rolling_exprs.append(pl.col(col).sum().alias(f"{col}_h2h_sum_last_{w}"))

        if rolling_exprs:
            rolling = recent.group_by(["home_team_id", "away_team_id"]).agg(
                rolling_exprs
            )
            overall = overall.join(
                rolling, on=["home_team_id", "away_team_id"], how="left"
            )

    # Recent form (last 5)
    if "gameweek" in matches.columns:
        recent_5 = matches.group_by(["home_team_id", "away_team_id"]).tail(5)
        if not recent_5.is_empty():
            form_exprs: list[pl.Expr] = []
            for col in all_mean_cols:
                form_exprs.append(pl.col(col).mean().alias(f"recent_{col}_last_5"))
            for col in all_sum_cols:
                form_exprs.append(pl.col(col).sum().alias(f"recent_{col}_sum_last_5"))

            form = recent_5.group_by(["home_team_id", "away_team_id"]).agg(form_exprs)
            overall = overall.join(
                form, on=["home_team_id", "away_team_id"], how="left"
            )

    # Fill NaN with 0
    float_cols = [
        c for c in overall.columns if overall.schema[c] in (pl.Float64, pl.Float32)
    ]
    if float_cols:
        overall = overall.with_columns(pl.col(float_cols).fill_nan(0))

    _save_cache(cache_path, overall)
    logger.info(
        "Computed team H2H: %d pairs, %d columns",
        overall.shape[0],
        overall.shape[1],
    )

    if log_to_mlflow:
        _log_h2h_to_mlflow("team_h2h", overall)

    return overall


def compute_h2h_features(
    matches: pl.DataFrame,
    player_stats: pl.DataFrame,
    seasons: list[str] | None = None,
    windows: list[int] | None = None,
    use_cache: bool = True,
    write_db: bool = True,
    log_to_mlflow: bool = True,
) -> dict[str, pl.DataFrame]:
    """Compute all H2H features (player vs team + team vs team).

    Args:
        matches: Match-level DataFrame for team H2H.
        player_stats: Player-level DataFrame for player vs team.
        seasons: List of seasons to include.
        windows: Rolling window sizes.
        use_cache: Whether to use cached data.
        write_db: Whether to write results to Supabase.
        log_to_mlflow: Whether to log to MLflow.

    Returns:
        Dict with keys 'team_h2h' and 'player_vs_team'.
    """
    team_h2h = compute_team_h2h(matches, seasons, windows, use_cache, log_to_mlflow)
    player_vs_team = compute_player_vs_team(
        player_stats, seasons, windows, use_cache, log_to_mlflow
    )

    if write_db:
        _write_h2h_to_supabase(team_h2h, player_vs_team)

    return {
        "team_h2h": team_h2h,
        "player_vs_team": player_vs_team,
    }


def _write_h2h_to_supabase(
    team_h2h: pl.DataFrame,
    player_vs_team: pl.DataFrame,
) -> None:
    """Write H2H features to Supabase."""
    try:
        from src.data.database import write_to_supabase

        if not team_h2h.is_empty():
            write_to_supabase("h2h_team_metrics", team_h2h, upsert=True)
        if not player_vs_team.is_empty():
            write_to_supabase("h2h_player_vs_team", player_vs_team, upsert=True)
    except Exception as e:  # noqa: BLE001
        logger.warning("Supabase write failed for H2H features: %s", e)


def _log_h2h_to_mlflow(name: str, df: pl.DataFrame) -> None:
    """Log H2H feature metrics to MLflow."""
    try:
        from src.utils.mlflow_client import _get_mlflow

        mlflow = _get_mlflow()
        if mlflow is None:
            logger.debug("MLflow not available, skipping H2H logging")
            return

        mlflow.set_tracking_uri("mlruns")
        mlflow.set_experiment("fpl_feature_engineering")
        with mlflow.start_run(run_name=f"h2h_{name}"):
            mlflow.log_param(f"{name}_pairs", df.shape[0])
            mlflow.log_param(f"{name}_columns", df.shape[1])
    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to log H2H features to MLflow: %s", e)


def clear_cache() -> None:
    """Clear all cached H2H data."""
    if CACHE_DIR.exists():
        for f in CACHE_DIR.glob("*.parquet"):
            f.unlink()
        logger.info("Cleared H2H cache in %s", CACHE_DIR)
