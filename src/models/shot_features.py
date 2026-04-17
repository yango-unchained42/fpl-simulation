"""Granular Understat shot feature engineering.

Engineers advanced features from Understat shot-level data including
shot quality, box entry rate, penalty involvement, set piece taking,
shot frequency, and conversion rate.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/processed/understat_features")
CACHE_TTL_SECONDS = 86400  # 24 hours


def _cache_key(func_name: str, params: dict[str, Any]) -> Path:
    """Generate cache file path."""
    import hashlib
    import json

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    params_str = json.dumps(params, sort_keys=True)
    key_hash = hashlib.md5(params_str.encode()).hexdigest()[:12]
    return CACHE_DIR / f"{func_name}_{key_hash}.parquet"


def _is_cache_valid(cache_path: Path, ttl: int = CACHE_TTL_SECONDS) -> bool:
    """Check if a cached file is still valid."""
    import time

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


def engineer_shot_quality_features(
    shot_data: pl.DataFrame,
    player_mapping: pl.DataFrame | None = None,
    use_cache: bool = True,
) -> pl.DataFrame:
    """Engineer shot quality features from granular Understat shot data.

    Calculates per-player-per-gameweek features:
    - avg_shot_xg: Average xG per shot (shot quality score)
    - box_entry_rate: Shots inside box / total shots
    - penalty_involvement: Count of penalty shots
    - shot_frequency: Total shots per gameweek
    - conversion_rate: Goals / shots
    - set_piece_taking: Count of set piece shots
    - key_passes: From match stats
    - deep_completions: From match stats

    Args:
        shot_data: DataFrame with granular shot events. Must have columns:
            player_id, gameweek, xg, situation, body_part, goal, location.
        player_mapping: Optional mapping from Understat player_id to FPL id.
        use_cache: Whether to use cached features if available.

    Returns:
        DataFrame with engineered features per (player_id, gameweek).
    """

    cache_params = {"n_shots": shot_data.shape[0]}
    cache_path = _cache_key("shot_quality_features", cache_params)

    if use_cache and _is_cache_valid(cache_path):
        logger.info("Using cached shot quality features")
        return _load_cache(cache_path)

    # Group by player and gameweek
    group_cols = ["player_id", "gameweek"]
    if not all(c in shot_data.columns for c in group_cols):
        logger.warning("Missing required columns: %s", group_cols)
        return pl.DataFrame()

    # Box entry pattern
    box_pattern = (
        "Six yard|Penalty area|Left side of six yard box|"
        "Right side of six yard box|Centre of six yard box|"
        "Left side of penalty area|Right side of penalty area|"
        "Centre of penalty area"
    )

    features = shot_data.group_by(group_cols).agg(
        # Shot quality score
        pl.col("xg").mean().alias("avg_shot_xg"),
        pl.col("xg").sum().alias("total_xg"),
        # Shot frequency
        pl.len().alias("shot_frequency"),
        # Conversion rate
        (pl.col("goal").sum() / pl.len()).alias("conversion_rate"),
        # Box entry rate (shots inside penalty area)
        pl.when(pl.col("location").str.contains(box_pattern))
        .then(1)
        .otherwise(0)
        .mean()
        .alias("box_entry_rate"),
        # Penalty involvement
        pl.when(pl.col("situation").str.contains("Penalty"))
        .then(1)
        .otherwise(0)
        .sum()
        .alias("penalty_involvement"),
        # Set piece taking
        pl.when(pl.col("situation").str.contains("Set Piece|Direct Freekick|Corner"))
        .then(1)
        .otherwise(0)
        .sum()
        .alias("set_piece_taking"),
        # Body part diversity
        pl.col("body_part").n_unique().alias("body_part_diversity"),
    )

    # Apply player mapping if provided
    if player_mapping is not None and "understat_player_id" in player_mapping.columns:
        features = (
            features.join(
                player_mapping,
                left_on="player_id",
                right_on="understat_player_id",
                how="left",
            )
            .drop("player_id")
            .rename({"fpl_player_id": "player_id"})
        )

    # Save cache
    _save_cache(cache_path, features)
    logger.info(
        "Engineered shot quality features: %d player-gameweek pairs, %d features",
        features.shape[0],
        features.shape[1],
    )

    return features


def merge_shot_features_with_dataset(
    main_dataset: pl.DataFrame,
    shot_features: pl.DataFrame,
    on_cols: list[str] | None = None,
) -> pl.DataFrame:
    """Merge shot quality features with the main training dataset.

    Args:
        main_dataset: Main training dataset with (player_id, gameweek).
        shot_features: Engineered shot quality features.
        on_cols: Columns to join on. Defaults to ["player_id", "gameweek"].

    Returns:
        Merged DataFrame with shot features.
    """
    if on_cols is None:
        on_cols = ["player_id", "gameweek"]

    # Ensure join columns exist
    if not all(c in shot_features.columns for c in on_cols):
        logger.warning("Shot features missing join columns: %s", on_cols)
        return main_dataset

    # Left join to keep all main dataset rows
    merged = main_dataset.join(shot_features, on=on_cols, how="left")

    # Fill null shot features with 0 (player had no shots)
    shot_feature_cols = [
        "avg_shot_xg",
        "total_xg",
        "shot_frequency",
        "conversion_rate",
        "box_entry_rate",
        "penalty_involvement",
        "set_piece_taking",
        "body_part_diversity",
    ]
    for col in shot_feature_cols:
        if col in merged.columns:
            merged = merged.with_columns(pl.col(col).fill_null(0.0))

    logger.info(
        "Merged shot features: %d rows, %d columns",
        merged.shape[0],
        merged.shape[1],
    )

    return merged
