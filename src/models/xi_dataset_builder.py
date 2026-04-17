"""Starting XI prediction dataset builder.

Prepares features and target variable for the Starting XI prediction model.
Target: Binary (1 = started/played significant minutes, 0 = did not start).
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

DATASET_DIR = Path("data/processed")
XI_DATASET_FILE = DATASET_DIR / "xi_training_dataset.parquet"

# Minutes threshold to consider a player as having "started"
MINUTES_THRESHOLD = 60

# Columns to exclude from feature set
EXCLUDE_COLS = {
    "player_id",
    "gameweek",
    "fixture_id",
    "name",
    "web_name",
    "position",  # Will be one-hot encoded
    "team",
    "element_type",  # Will be one-hot encoded
    "status",
    "season",
    "kickoff_time",
    "modified",
    "minutes",  # Used to create target, shouldn't be a feature (leakage)
}


def build_xi_dataset(
    player_stats: pl.DataFrame,
    rolling_features: pl.DataFrame | None = None,
    use_cache: bool = True,
) -> pl.DataFrame:
    """Build the training dataset for XI prediction.

    Creates binary target variable `is_starter` based on minutes played.
    Merges with rolling features and handles categorical encoding.

    Args:
        player_stats: Base player gameweek statistics. Must have 'minutes' column.
        rolling_features: Optional DataFrame with rolling features.
        use_cache: Whether to use cached dataset if available.

    Returns:
        DataFrame with features and binary target.
    """
    cache_path = XI_DATASET_FILE

    if use_cache and cache_path.exists():
        logger.info("Using cached XI dataset")
        return pl.read_parquet(cache_path)

    if player_stats.is_empty():
        return pl.DataFrame()

    # 1. Create Target Variable
    # Player is considered a starter if they played >= 60 minutes
    player_stats = player_stats.with_columns(
        (pl.col("minutes") >= MINUTES_THRESHOLD).cast(pl.Int64).alias("is_starter")
    )

    # 2. Merge Rolling Features
    dataset = player_stats.clone()
    if rolling_features is not None and not rolling_features.is_empty():
        merge_cols = ["player_id", "gameweek"]
        if all(c in rolling_features.columns for c in merge_cols):
            dataset = dataset.join(rolling_features, on=merge_cols, how="left")

    # 3. One-Hot Encode Categoricals (Position, Element Type)
    if "element_type" in dataset.columns:
        dataset = dataset.with_columns(
            pl.col("element_type").cast(pl.Utf8).alias("pos_code")
        )
        # LightGBM handles categorical ints well.
        # For now, keep it simple and drop string cols.
        dataset = dataset.drop("pos_code")

    # 4. Remove Excluded Columns
    cols_to_drop = [c for c in EXCLUDE_COLS if c in dataset.columns]
    if cols_to_drop:
        dataset = dataset.drop(cols_to_drop)

    # 5. Remove rows with null target (shouldn't happen if minutes exists)
    dataset = dataset.filter(pl.col("is_starter").is_not_null())

    # Save cache
    DATASET_DIR.mkdir(parents=True, exist_ok=True)
    dataset.write_parquet(cache_path)
    starter_mean = dataset["is_starter"].mean()
    starter_pct = (
        float(starter_mean) * 100.0  # type: ignore[arg-type]
        if starter_mean is not None
        else 0.0
    )
    logger.info(
        "Built XI dataset: %d rows, %d columns, starter ratio: %.2f%%",
        dataset.shape[0],
        dataset.shape[1],
        starter_pct,
    )

    return dataset


def compute_class_weights(
    df: pl.DataFrame,
    target_col: str = "is_starter",
) -> dict[str, float]:
    """Compute class weights for imbalanced dataset.

    Args:
        df: DataFrame with target column.
        target_col: Name of target column.

    Returns:
        Dict with 'scale_pos_weight' for LightGBM.
    """
    if target_col not in df.columns:
        return {}

    counts = df[target_col].value_counts()
    # Find counts for 0 and 1
    count_0 = counts.filter(pl.col(target_col) == 0)["count"].sum()
    count_1 = counts.filter(pl.col(target_col) == 1)["count"].sum()

    if count_0 == 0 or count_1 == 0:
        return {"scale_pos_weight": 1.0}

    # scale_pos_weight = count(0) / count(1)
    weight = float(count_0) / float(count_1)

    logger.info(
        "Class weights computed: 0=%d, 1=%d, scale_pos_weight=%.2f",
        count_0,
        count_1,
        weight,
    )

    return {"scale_pos_weight": weight}
