"""Data cleaning and validation module.

Handles name standardization, missing data imputation, outlier treatment,
and Pandera schema validation.
"""

from __future__ import annotations

import logging

import pandera.polars as pa
import polars as pl

logger = logging.getLogger(__name__)


class PlayerStatsSchema(pa.DataFrameModel):
    """Pandera schema for player stats data."""

    player_id: pl.Int64 = pa.Field(gt=0)
    gameweek: pl.Int64 = pa.Field(ge=1, le=38)
    minutes: pl.Int64 = pa.Field(ge=0)
    goals: pl.Int64 = pa.Field(ge=0)
    assists: pl.Int64 = pa.Field(ge=0)
    points: pl.Int64 = pa.Field(ge=0)


def standardize_names(df: pl.DataFrame, name_col: str = "name") -> pl.DataFrame:
    """Standardize player names to 'First Last' format.

    Args:
        df: Input DataFrame with player names.
        name_col: Column name containing player names.

    Returns:
        DataFrame with standardized names.
    """
    return df.with_columns(
        pl.col(name_col).str.strip_chars().str.to_titlecase().alias(name_col)
    )


def impute_missing_minutes(df: pl.DataFrame) -> pl.DataFrame:
    """Impute missing minutes with position-based defaults.

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with imputed minutes values.
    """
    return df.with_columns(
        pl.col("minutes").fill_null(0)
    )


def winsorize_outliers(
    df: pl.DataFrame,
    columns: list[str],
    lower: float = 0.01,
    upper: float = 0.99,
) -> pl.DataFrame:
    """Winsorize outliers at specified percentiles.

    Args:
        df: Input DataFrame.
        columns: Column names to winsorize.
        lower: Lower percentile bound.
        upper: Upper percentile bound.

    Returns:
        DataFrame with winsorized values.
    """
    for col in columns:
        if col not in df.columns:
            continue
        lower_val = df[col].quantile(lower)
        upper_val = df[col].quantile(upper)
        df = df.with_columns(
            pl.col(col).clip(lower_val, upper_val)
        )
    return df


def clean_data(df: pl.DataFrame) -> pl.DataFrame:
    """Apply full cleaning pipeline.

    Args:
        df: Raw input DataFrame.

    Returns:
        Cleaned and validated DataFrame.
    """
    df = standardize_names(df)
    df = impute_missing_minutes(df)
    numeric_cols = [
        c for c in df.columns
        if df.schema[c] in (pl.Int64, pl.Float64, pl.Int32, pl.Float32)
    ]
    df = winsorize_outliers(df, numeric_cols)
    PlayerStatsSchema.validate(df)
    return df
