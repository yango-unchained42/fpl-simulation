"""Team rolling features module.

Computes rolling averages (3/5/10 GW) for team statistics to capture
team form. Includes home/away splits. Output is joined to player features
via opponent_team_id + gameweek.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

FEATURES_DIR = Path("data/processed")
TEAM_FEATURES_FILE = FEATURES_DIR / "team_features.parquet"

# Team metrics to compute rolling features for
TEAM_MEAN_METRICS = [
    # Attacking
    "xg",
    "xa",
    "goals_scored",
    "shots",
    "key_passes",
    "deep_completions",
    # Defensive
    "xg_conceded",
    "goals_conceded",
    "clean_sheets",
    "ppda",
    "tackles",
    "interceptions",
    # Advanced
    "np_xg",  # non-penalty xG
    "xg_buildup",
    "xg_chain",
    "expected_points",
    # FPL team-level
    "strength",
    "strength_overall_home",
    "strength_overall_away",
    "strength_attack_home",
    "strength_attack_away",
    "strength_defence_home",
    "strength_defence_away",
]

TEAM_SUM_METRICS = [
    "goals_scored",
    "clean_sheets",
    "shots",
    "tackles",
]

# Columns to exclude
TEAM_EXCLUDE_COLUMNS = {
    "team_id",
    "gameweek",
    "fixture_id",
    "home_team_id",
    "away_team_id",
    "opponent_team_id",
    "season",
    "team",
    "name",
    "short_name",
    "kickoff_time",
    "finished",
    "started",
    "team_h_score",
    "team_a_score",
    "was_home",
    "home_team",
    "away_team",
}

WINDOWS = [3, 5, 10]


def compute_team_rolling_features(
    team_data: pl.DataFrame,
    windows: list[int] | None = None,
    use_cache: bool = True,
    log_to_mlflow: bool = True,
) -> pl.DataFrame:
    """Compute rolling features for team statistics.

    Computes rolling mean/sum for all available team metrics across
    3/5/10 gameweek windows. Includes separate home/away splits.

    Args:
        team_data: DataFrame with team match stats. Must have 'team_id'
            and 'gameweek' columns. If 'was_home' column exists,
            home/away splits are computed.
        windows: List of window sizes. Defaults to [3, 5, 10].
        use_cache: Whether to use cached features if available.
        log_to_mlflow: Whether to log feature count and timing to MLflow.

    Returns:
        DataFrame with team_id, gameweek, and rolling feature columns.
    """
    t0 = time.time()

    if windows is None:
        windows = WINDOWS

    cache_path = TEAM_FEATURES_FILE
    if use_cache and cache_path.exists():
        logger.info("Using cached team features from %s", cache_path)
        return pl.read_parquet(cache_path)

    if team_data.is_empty():
        logger.warning("Empty team data, returning empty features")
        return team_data

    # Ensure sorted
    if "team_id" in team_data.columns and "gameweek" in team_data.columns:
        team_data = team_data.sort(["team_id", "gameweek"])

    available_cols = set(team_data.columns) - TEAM_EXCLUDE_COLUMNS
    mean_cols = [c for c in TEAM_MEAN_METRICS if c in available_cols]
    sum_cols = [c for c in TEAM_SUM_METRICS if c in available_cols]

    # Auto-detect extra numeric columns
    predefined = set(TEAM_MEAN_METRICS) | set(TEAM_SUM_METRICS)
    extra_numeric = [
        c
        for c in available_cols
        if c not in predefined
        and team_data.schema[c] in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
    ]

    all_mean_cols = mean_cols + extra_numeric
    all_sum_cols = sum_cols

    logger.info(
        "Computing team rolling features: %d mean, %d sum, "
        "%d extra numeric, %d windows",
        len(all_mean_cols),
        len(all_sum_cols),
        len(extra_numeric),
        len(windows),
    )

    # Build rolling expressions
    exprs: list[pl.Expr] = []

    for w in windows:
        for col in all_mean_cols:
            col_name = f"team_{col}_rolling_mean_{w}"
            exprs.append(
                pl.col(col).rolling_mean(window_size=w).over("team_id").alias(col_name)
            )

        for col in all_sum_cols:
            col_name = f"team_{col}_rolling_sum_{w}"
            exprs.append(
                pl.col(col).rolling_sum(window_size=w).over("team_id").alias(col_name)
            )

    # Home/away splits
    if "was_home" in team_data.columns:
        for w in windows:
            for col in all_mean_cols:
                col_name_home = f"team_{col}_home_rolling_mean_{w}"
                col_name_away = f"team_{col}_away_rolling_mean_{w}"

                exprs.append(
                    pl.when(pl.col("was_home"))
                    .then(pl.col(col).rolling_mean(window_size=w).over("team_id"))
                    .otherwise(None)
                    .alias(col_name_home)
                )
                exprs.append(
                    pl.when(~pl.col("was_home"))
                    .then(pl.col(col).rolling_mean(window_size=w).over("team_id"))
                    .otherwise(None)
                    .alias(col_name_away)
                )

    if not exprs:
        logger.warning("No team rolling features to compute")
        return team_data

    team_data = team_data.with_columns(exprs)

    # Fill NaN with 0
    float_cols = [
        c for c in team_data.columns if team_data.schema[c] in (pl.Float64, pl.Float32)
    ]
    if float_cols:
        team_data = team_data.with_columns(pl.col(float_cols).fill_nan(0))

    # Fill partial window nulls with expanding mean/sum
    for w in windows:
        for col in all_mean_cols:
            col_name = f"team_{col}_rolling_mean_{w}"
            if col_name in team_data.columns:
                cumsum = pl.col(col).cum_sum().over("team_id")
                count = pl.arange(1, pl.len() + 1).over("team_id")
                expanding = cumsum / count.cast(pl.Float64)
                team_data = team_data.with_columns(
                    pl.when(pl.col(col_name).is_null())
                    .then(expanding)
                    .otherwise(pl.col(col_name))
                    .alias(col_name)
                )

        for col in all_sum_cols:
            col_name = f"team_{col}_rolling_sum_{w}"
            if col_name in team_data.columns:
                cumsum = pl.col(col).cum_sum().over("team_id")
                team_data = team_data.with_columns(
                    pl.when(pl.col(col_name).is_null())
                    .then(cumsum)
                    .otherwise(pl.col(col_name))
                    .alias(col_name)
                )

    elapsed = time.time() - t0
    n_features = len(exprs)
    logger.info(
        "Computed %d team rolling features in %.1fs (%d rows, %d columns)",
        n_features,
        elapsed,
        team_data.shape[0],
        team_data.shape[1],
    )

    # Save cache
    FEATURES_DIR.mkdir(parents=True, exist_ok=True)
    team_data.write_parquet(cache_path)
    logger.info("Saved team features to %s", cache_path)

    if log_to_mlflow:
        _log_team_features_to_mlflow(team_data, n_features, elapsed)

    return team_data


def _log_team_features_to_mlflow(
    df: pl.DataFrame,
    n_features: int,
    elapsed: float,
) -> None:
    """Log team feature engineering metrics to MLflow."""
    try:
        from src.utils.mlflow_client import _get_mlflow

        mlflow = _get_mlflow()
        if mlflow is None:
            logger.debug("MLflow not available, skipping team feature logging")
            return

        mlflow.set_tracking_uri("mlruns")
        mlflow.set_experiment("fpl_feature_engineering")
        with mlflow.start_run(run_name="team_rolling_features"):
            mlflow.log_param("total_rows", df.shape[0])
            mlflow.log_param("total_columns", df.shape[1])
            mlflow.log_param("team_rolling_features", n_features)
            mlflow.log_metric("computation_time_seconds", elapsed)
    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to log team features to MLflow: %s", e)
