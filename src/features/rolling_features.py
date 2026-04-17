"""Rolling features module.

Computes rolling averages and sums (3/5/10 GW) for ALL player performance
metrics. Outputs a single flat feature table for ML training.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

FEATURES_DIR = Path("data/processed")
FEATURES_FILE = FEATURES_DIR / "features.parquet"

# Metrics that should use rolling MEAN (rate-based)
MEAN_METRICS = [
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

# Metrics that should use rolling SUM (cumulative)
SUM_METRICS = [
    "goals_scored",
    "assists",
    "bonus",
]

# Metrics to exclude from rolling (IDs, timestamps, booleans, etc.)
EXCLUDE_COLUMNS = {
    "player_id",
    "gameweek",
    "fixture_id",
    "opponent_team_id",
    "was_home",
    "kickoff_time",
    "season",
    "name",
    "web_name",
    "position",
    "team",
    "element_type",
    "status",
    "modified",
    "fixture",
    "round",
    "GW",
    "element",
    "player",
    "understat_name",
    "fpl_name",
    "fpl_player_id",
    "understat_player_id",
    "league_id",
    "season_id",
    "team_id",
    "game_id",
    "position_id",
    # Per-90 rates (already normalized, don't roll)
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
    # Strings
    "news",
    "photo",
    "code",
    "opta_code",
    "scout_news_link",
    "birth_date",
    "team_join_date",
    "corners_and_indirect_freekicks_text",
    "direct_freekicks_text",
    "penalties_text",
}

WINDOWS = [3, 5, 10]


def compute_rolling_features(
    df: pl.DataFrame,
    windows: list[int] | None = None,
    use_cache: bool = True,
    log_to_mlflow: bool = True,
) -> pl.DataFrame:
    """Compute rolling average/sum features for ALL available metrics.

    For each metric in the DataFrame (excluding IDs, timestamps, etc.),
    computes rolling mean or sum over 3/5/10 gameweek windows per player.

    Args:
        df: DataFrame with player gameweek stats. Must have 'player_id'
            and 'gameweek' columns.
        windows: List of window sizes. Defaults to [3, 5, 10].
        use_cache: Whether to use cached features if available.
        log_to_mlflow: Whether to log feature count and timing to MLflow.

    Returns:
        DataFrame with original columns plus rolling feature columns.
    """
    t0 = time.time()

    if windows is None:
        windows = WINDOWS

    # Check cache
    cache_path = FEATURES_FILE
    if use_cache and cache_path.exists():
        logger.info("Using cached features from %s", cache_path)
        return pl.read_parquet(cache_path)

    if df.is_empty():
        logger.warning("Empty input DataFrame, returning empty features")
        return df

    # Ensure sorted by player and gameweek
    if "player_id" in df.columns and "gameweek" in df.columns:
        df = df.sort(["player_id", "gameweek"])

    # Determine which metrics are available
    available_cols = set(df.columns) - EXCLUDE_COLUMNS
    mean_cols = [c for c in MEAN_METRICS if c in available_cols]
    sum_cols = [c for c in SUM_METRICS if c in available_cols]

    # Also include any numeric columns not in our predefined lists
    predefined = set(MEAN_METRICS) | set(SUM_METRICS)
    extra_numeric = [
        c
        for c in available_cols
        if c not in predefined
        and df.schema[c] in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
    ]

    all_mean_cols = mean_cols + extra_numeric
    all_sum_cols = sum_cols

    logger.info(
        "Computing rolling features: %d mean metrics, %d sum metrics, "
        "%d extra numeric, %d windows",
        len(all_mean_cols),
        len(all_sum_cols),
        len(extra_numeric),
        len(windows),
    )

    # Build rolling expressions
    exprs: list[pl.Expr] = []

    for w in windows:
        # Rolling mean (handles partial windows at season start)
        for col in all_mean_cols:
            col_name = f"{col}_rolling_mean_{w}"
            # Use shift + cumsum approach for partial windows
            exprs.append(
                pl.col(col)
                .rolling_mean(window_size=w)
                .over("player_id")
                .alias(col_name)
            )

        # Rolling sum
        for col in all_sum_cols:
            col_name = f"{col}_rolling_sum_{w}"
            exprs.append(
                pl.col(col).rolling_sum(window_size=w).over("player_id").alias(col_name)
            )

    if not exprs:
        logger.warning("No rolling features to compute")
        return df

    # Apply rolling features
    df = df.with_columns(exprs)

    # Fill None values from partial windows with expanding mean/sum
    for w in windows:
        for col in all_mean_cols:
            col_name = f"{col}_rolling_mean_{w}"
            # For partial windows, use cumsum / count
            cumsum = pl.col(col).cum_sum().over("player_id")
            count = pl.arange(1, pl.len() + 1).over("player_id")
            expanding = cumsum / count.cast(pl.Float64)
            df = df.with_columns(
                pl.when(pl.col(col_name).is_null())
                .then(expanding)
                .otherwise(pl.col(col_name))
                .alias(col_name)
            )

        for col in all_sum_cols:
            col_name = f"{col}_rolling_sum_{w}"
            # For partial windows, use cumsum
            cumsum = pl.col(col).cum_sum().over("player_id")
            df = df.with_columns(
                pl.when(pl.col(col_name).is_null())
                .then(cumsum)
                .otherwise(pl.col(col_name))
                .alias(col_name)
            )

    # Fill NaN with 0 (for players with no data in window)
    float_cols = [c for c in df.columns if df.schema[c] in (pl.Float64, pl.Float32)]
    if float_cols:
        df = df.with_columns(pl.col(float_cols).fill_nan(0))

    elapsed = time.time() - t0
    n_features = len(exprs)
    logger.info(
        "Computed %d rolling features in %.1fs (%d rows, %d total columns)",
        n_features,
        elapsed,
        df.shape[0],
        df.shape[1],
    )

    # Save cache
    FEATURES_DIR.mkdir(parents=True, exist_ok=True)
    df.write_parquet(cache_path)
    logger.info("Saved features to %s", cache_path)

    if log_to_mlflow:
        _log_features_to_mlflow(df, n_features, elapsed)

    return df


def _log_features_to_mlflow(
    df: pl.DataFrame,
    n_features: int,
    elapsed: float,
) -> None:
    """Log feature engineering metrics to MLflow."""
    try:
        from src.utils.mlflow_client import _get_mlflow

        mlflow = _get_mlflow()
        if mlflow is None:
            logger.debug("MLflow not available, skipping feature logging")
            return

        mlflow.set_tracking_uri("mlruns")
        mlflow.set_experiment("fpl_feature_engineering")
        with mlflow.start_run(run_name="rolling_features"):
            mlflow.log_param("total_rows", df.shape[0])
            mlflow.log_param("total_columns", df.shape[1])
            mlflow.log_param("rolling_features", n_features)
            mlflow.log_metric("computation_time_seconds", elapsed)
            mlflow.log_metric("features_per_row", n_features / max(df.shape[0], 1))
    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to log features to MLflow: %s", e)
