"""Training dataset builder.

Integrates all features from previous sprints, creates target variable
(next gameweek points), handles multicollinearity, and creates
time-based train/validation/test splits.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import polars as pl

logger = logging.getLogger(__name__)

DATASET_DIR = Path("data/processed")
DATASET_FILE = DATASET_DIR / "training_dataset.parquet"

# Columns to exclude from feature set
EXCLUDE_COLS = {
    "fixture_id",
    "name",
    "web_name",
    "position",
    "team",
    "element_type",
    "status",
    "season",
    "kickoff_time",
    "modified",
    "next_gw_points",  # target
    "player",
    "understat_name",
    "fpl_name",
    "fpl_player_id",
    "understat_player_id",
    # Duplicate/correlated columns (will be filtered)
    "home_team_id",
    "away_team_id",
    "opponent_team_id",
}

# Target column
TARGET_COL = "next_gw_points"


def build_training_dataset(
    player_stats: pl.DataFrame,
    rolling_features: pl.DataFrame | None = None,
    h2h_features: pl.DataFrame | None = None,
    form_features: pl.DataFrame | None = None,
    context_features: pl.DataFrame | None = None,
    use_cache: bool = True,
) -> pl.DataFrame:
    """Build the final training dataset by merging all feature sources.

    Creates target variable (next gameweek points) by shifting points
    forward by 1 gameweek per player.

    Args:
        player_stats: Base player gameweek statistics.
        rolling_features: Rolling average features from Sprint 3.
        h2h_features: H2H features from Sprint 4.
        form_features: Form metrics from Sprint 5.
        context_features: Contextual features from Sprint 5.
        use_cache: Whether to use cached dataset if available.

    Returns:
        DataFrame with all features and target variable.
    """
    cache_path = DATASET_FILE

    if use_cache and cache_path.exists():
        logger.info("Using cached training dataset")
        return pl.read_parquet(cache_path)

    if player_stats.is_empty():
        return pl.DataFrame()

    # Ensure sorted by player and gameweek
    if "player_id" in player_stats.columns and "gameweek" in player_stats.columns:
        player_stats = player_stats.sort(["player_id", "gameweek"])

    # Create target variable: shift points forward by 1 GW per player
    if "total_points" in player_stats.columns:
        points_col = "total_points"
    elif "points" in player_stats.columns:
        points_col = "points"
    else:
        points_col = None

    if points_col:
        player_stats = player_stats.with_columns(
            pl.col(points_col).shift(-1).over("player_id").alias(TARGET_COL)
        )

    # Start with base stats
    dataset = player_stats.clone()

    # Merge rolling features
    if rolling_features is not None and not rolling_features.is_empty():
        merge_cols = ["player_id", "gameweek"]
        if all(c in rolling_features.columns for c in merge_cols):
            dataset = dataset.join(
                rolling_features, on=merge_cols, how="left", suffix="_rolling"
            )

    # Merge H2H features
    if h2h_features is not None and not h2h_features.is_empty():
        merge_cols = ["player_id", "gameweek"]
        if all(c in h2h_features.columns for c in merge_cols):
            dataset = dataset.join(
                h2h_features, on=merge_cols, how="left", suffix="_h2h"
            )

    # Merge form features
    if form_features is not None and not form_features.is_empty():
        merge_cols = ["player_id", "gameweek"]
        if all(c in form_features.columns for c in merge_cols):
            dataset = dataset.join(
                form_features, on=merge_cols, how="left", suffix="_form"
            )

    # Merge context features
    if context_features is not None and not context_features.is_empty():
        merge_cols = ["player_id", "gameweek"]
        if all(c in context_features.columns for c in merge_cols):
            dataset = dataset.join(
                context_features, on=merge_cols, how="left", suffix="_context"
            )

    # Remove excluded columns (but keep target and key columns)
    cols_to_drop = [c for c in EXCLUDE_COLS if c in dataset.columns and c != TARGET_COL]
    if cols_to_drop:
        dataset = dataset.drop(cols_to_drop)

    # Remove rows with null target (last GW per player has no next GW)
    if TARGET_COL in dataset.columns:
        dataset = dataset.filter(pl.col(TARGET_COL).is_not_null())

    # Save cache
    DATASET_DIR.mkdir(parents=True, exist_ok=True)
    dataset.write_parquet(cache_path)
    logger.info(
        "Built training dataset: %d rows, %d columns, saved to %s",
        dataset.shape[0],
        dataset.shape[1],
        cache_path,
    )

    return dataset


def compute_feature_correlations(
    df: pl.DataFrame,
    threshold: float = 0.95,
) -> list[tuple[str, str, float]]:
    """Compute feature correlations and identify highly correlated pairs.

    Args:
        df: DataFrame with numeric features.
        threshold: Correlation threshold for flagging pairs.

    Returns:
        List of (feature1, feature2, correlation) tuples for pairs
        exceeding the threshold.
    """
    # Select only numeric columns
    numeric_cols = [
        c
        for c in df.columns
        if df.schema[c] in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
    ]

    if len(numeric_cols) < 2:
        return []

    # Compute correlation matrix
    corr_matrix = df.select(numeric_cols).to_pandas().corr()

    # Find highly correlated pairs
    high_corr_pairs: list[tuple[str, str, float]] = []
    for i in range(len(corr_matrix.columns)):
        for j in range(i + 1, len(corr_matrix.columns)):
            corr_val = float(abs(corr_matrix.iloc[i, j]))
            if corr_val >= threshold:
                high_corr_pairs.append(
                    (str(corr_matrix.columns[i]), str(corr_matrix.columns[j]), corr_val)
                )

    logger.info(
        "Found %d highly correlated feature pairs (threshold=%.2f)",
        len(high_corr_pairs),
        threshold,
    )

    return high_corr_pairs


def create_time_based_splits(
    df: pl.DataFrame,
    train_ratio: float = 0.70,
    val_ratio: float = 0.15,
    test_ratio: float = 0.15,
    time_col: str = "gameweek",
) -> dict[str, pl.DataFrame]:
    """Create time-based train/validation/test splits.

    Splits data by time column to prevent data leakage.
    Example: GW 1-26 = train, GW 27-32 = val, GW 33-38 = test

    Args:
        df: DataFrame with time column.
        train_ratio: Proportion of data for training.
        val_ratio: Proportion of data for validation.
        test_ratio: Proportion of data for testing.
        time_col: Column to use for time-based splitting.

    Returns:
        Dict with 'train', 'val', 'test' DataFrames.
    """
    if time_col not in df.columns:
        raise ValueError(f"Time column '{time_col}' not found in DataFrame")

    # Get unique time values sorted
    unique_times = sorted(df[time_col].unique().to_list())
    n_times = len(unique_times)

    train_end = int(n_times * train_ratio)
    val_end = int(n_times * (train_ratio + val_ratio))

    train_times = unique_times[:train_end]
    val_times = unique_times[train_end:val_end]
    test_times = unique_times[val_end:]

    splits = {
        "train": df.filter(pl.col(time_col).is_in(train_times)),
        "val": df.filter(pl.col(time_col).is_in(val_times)),
        "test": df.filter(pl.col(time_col).is_in(test_times)),
    }

    logger.info(
        "Time-based splits: train=%d rows, val=%d rows, test=%d rows",
        splits["train"].shape[0],
        splits["val"].shape[0],
        splits["test"].shape[0],
    )

    return splits


def compute_feature_importance_baseline(
    df: pl.DataFrame,
    target_col: str = TARGET_COL,
) -> dict[str, float]:
    """Compute baseline feature importance using simple correlation.

    Args:
        df: DataFrame with features and target.
        target_col: Target column name.

    Returns:
        Dict mapping feature names to absolute correlation with target.
    """
    if target_col not in df.columns:
        return {}

    # Select only numeric columns
    numeric_cols = [
        c
        for c in df.columns
        if c != target_col
        and df.schema[c] in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
    ]

    if not numeric_cols:
        return {}

    # Compute correlation with target
    corr_series = df.select(numeric_cols + [target_col]).to_pandas().corr()[target_col]

    importance: dict[str, float] = {}
    for col in numeric_cols:
        if col in corr_series.index:
            val = corr_series[col]
            if isinstance(val, (int, float)) and not np.isnan(val):
                importance[col] = abs(float(val))

    # Sort by importance
    importance = dict(sorted(importance.items(), key=lambda x: x[1], reverse=True))

    logger.info(
        "Computed baseline feature importance for %d features",
        len(importance),
    )

    return importance
