"""Data validation utilities.

Provides validation functions for data quality checks across
the pipeline.
"""

from __future__ import annotations

import logging

import polars as pl

logger = logging.getLogger(__name__)


def validate_player_ids(df: pl.DataFrame, valid_ids: set[int]) -> pl.DataFrame:
    """Filter DataFrame to only include valid player IDs.

    Args:
        df: Input DataFrame with player_id column.
        valid_ids: Set of valid player IDs.

    Returns:
        Filtered DataFrame.
    """
    if "player_id" not in df.columns:
        logger.warning("No player_id column found")
        return df
    return df.filter(pl.col("player_id").is_in(list(valid_ids)))


def validate_gameweek_range(df: pl.DataFrame, min_gw: int = 1, max_gw: int = 38) -> pl.DataFrame:
    """Validate and filter gameweek values.

    Args:
        df: Input DataFrame with gameweek column.
        min_gw: Minimum valid gameweek.
        max_gw: Maximum valid gameweek.

    Returns:
        Filtered DataFrame.
    """
    if "gameweek" not in df.columns:
        logger.warning("No gameweek column found")
        return df
    return df.filter(
        (pl.col("gameweek") >= min_gw) & (pl.col("gameweek") <= max_gw)
    )


def check_data_completeness(
    df: pl.DataFrame,
    threshold: float = 0.95,
) -> dict[str, float]:
    """Check completeness of each column.

    Args:
        df: Input DataFrame.
        threshold: Minimum completeness ratio to pass.

    Returns:
        Dict mapping column names to completeness ratios.
    """
    completeness: dict[str, float] = {}
    for col in df.columns:
        non_null = df.select(pl.col(col).drop_nulls()).height
        completeness[col] = non_null / max(df.height, 1)
    return completeness
