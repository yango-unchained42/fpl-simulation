"""Missing data imputation module.

Implements multiple imputation strategies for player and team statistics
using Polars. Tracks imputation decisions, confidence scoring, and
prevents over-imputation.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)

# Maximum percentage of nulls allowed before rejecting imputation
MAX_NULL_RATIO = 0.5


@dataclass
class ImputationReport:
    """Report tracking all imputation decisions."""

    columns_imputed: list[str] = field(default_factory=list)
    rows_imputed: dict[str, int] = field(default_factory=dict)
    strategies_used: dict[str, str] = field(default_factory=dict)
    columns_rejected: list[str] = field(default_factory=list)
    total_nulls_before: int = 0
    total_nulls_after: int = 0

    def summary(self) -> str:
        """Generate a human-readable summary of imputation."""
        lines = ["Imputation Report", "=" * 40]
        lines.append(f"  Total nulls before: {self.total_nulls_before}")
        lines.append(f"  Total nulls after:  {self.total_nulls_after}")
        lines.append(f"  Columns imputed:    {len(self.columns_imputed)}")
        lines.append(f"  Columns rejected:   {len(self.columns_rejected)}")
        for col in self.columns_imputed:
            strategy = self.strategies_used.get(col, "unknown")
            rows = self.rows_imputed.get(col, 0)
            lines.append(f"    {col}: {strategy} ({rows} rows)")
        if self.columns_rejected:
            lines.append("  Rejected columns (>50% nulls):")
            for col in self.columns_rejected:
                lines.append(f"    {col}")
        return "\n".join(lines)


def _count_nulls(df: pl.DataFrame) -> int:
    """Count total null values in a DataFrame.

    Args:
        df: Input DataFrame.

    Returns:
        Total number of null values.
    """
    return sum(df[col].null_count() for col in df.columns)


def impute_forward_fill(
    df: pl.DataFrame,
    columns: list[str] | None = None,
    sort_by: str | None = None,
    group_by: str | None = None,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Forward-fill missing values (for time-series data).

    Args:
        df: Input DataFrame.
        columns: Columns to impute. Defaults to all numeric columns.
        sort_by: Column to sort by before filling (e.g., "gameweek").
        group_by: Column to group by before filling (e.g., "player_id").

    Returns:
        Tuple of (imputed DataFrame, dict of rows imputed per column).
    """
    if columns is None:
        columns = [
            c
            for c in df.columns
            if df.schema[c] in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
        ]

    cols_to_fill = [c for c in columns if c in df.columns]
    imputed_counts: dict[str, int] = {}

    for col in cols_to_fill:
        null_before = df[col].null_count()
        if null_before == 0:
            continue

        if group_by and sort_by and group_by in df.columns and sort_by in df.columns:
            df = df.sort([group_by, sort_by])
            df = df.with_columns(pl.col(col).forward_fill().over(group_by).alias(col))
        elif sort_by and sort_by in df.columns:
            df = df.sort(sort_by)
            df = df.with_columns(pl.col(col).forward_fill().alias(col))
        else:
            df = df.with_columns(pl.col(col).forward_fill().alias(col))

        imputed_counts[col] = null_before - df[col].null_count()

    return df, imputed_counts


def impute_mean(
    df: pl.DataFrame,
    columns: list[str] | None = None,
    group_by: str | None = None,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Impute missing values with mean (for numerical features).

    Args:
        df: Input DataFrame.
        columns: Columns to impute. Defaults to all numeric columns.
        group_by: Column to group by before computing mean (e.g., "position").

    Returns:
        Tuple of (imputed DataFrame, dict of rows imputed per column).
    """
    if columns is None:
        columns = [
            c
            for c in df.columns
            if df.schema[c] in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
        ]

    cols_to_fill = [c for c in columns if c in df.columns]
    imputed_counts: dict[str, int] = {}

    for col in cols_to_fill:
        null_before = df[col].null_count()
        if null_before == 0:
            continue

        if group_by and group_by in df.columns:
            fill_df = df.group_by(group_by).agg(pl.col(col).mean().alias("_fill"))
            df = df.join(fill_df, on=group_by, how="left")
            df = df.with_columns(
                pl.when(pl.col(col).is_null())
                .then(pl.col("_fill"))
                .otherwise(pl.col(col))
                .alias(col)
            )
            df = df.drop("_fill")
        else:
            fill_scalar = df[col].mean()
            if fill_scalar is not None:
                df = df.with_columns(
                    pl.col(col).fill_null(float(fill_scalar)).alias(col)  # type: ignore[arg-type]
                )

        imputed_counts[col] = null_before - df[col].null_count()

    return df, imputed_counts


def impute_median(
    df: pl.DataFrame,
    columns: list[str] | None = None,
    group_by: str | None = None,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Impute missing values with median (for numerical features with outliers).

    Args:
        df: Input DataFrame.
        columns: Columns to impute. Defaults to all numeric columns.
        group_by: Column to group by before computing median.

    Returns:
        Tuple of (imputed DataFrame, dict of rows imputed per column).
    """
    if columns is None:
        columns = [
            c
            for c in df.columns
            if df.schema[c] in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
        ]

    cols_to_fill = [c for c in columns if c in df.columns]
    imputed_counts: dict[str, int] = {}

    for col in cols_to_fill:
        null_before = df[col].null_count()
        if null_before == 0:
            continue

        if group_by and group_by in df.columns:
            fill_df = df.group_by(group_by).agg(pl.col(col).median().alias("_fill"))
            df = df.join(fill_df, on=group_by, how="left")
            df = df.with_columns(
                pl.when(pl.col(col).is_null())
                .then(pl.col("_fill"))
                .otherwise(pl.col(col))
                .alias(col)
            )
            df = df.drop("_fill")
        else:
            fill_scalar = df[col].median()
            if fill_scalar is not None:
                df = df.with_columns(
                    pl.col(col).fill_null(float(fill_scalar)).alias(col)  # type: ignore[arg-type]
                )

        imputed_counts[col] = null_before - df[col].null_count()

    return df, imputed_counts


def impute_mode(
    df: pl.DataFrame,
    columns: list[str] | None = None,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Impute missing values with mode (for categorical features).

    Args:
        df: Input DataFrame.
        columns: Columns to impute. Defaults to all string columns.

    Returns:
        Tuple of (imputed DataFrame, dict of rows imputed per column).
    """
    if columns is None:
        columns = [c for c in df.columns if df.schema[c] in (pl.String, pl.Categorical)]

    cols_to_fill = [c for c in columns if c in df.columns]
    imputed_counts: dict[str, int] = {}

    for col in cols_to_fill:
        null_before = df[col].null_count()
        if null_before == 0:
            continue

        mode_val = df[col].mode()
        if not mode_val.is_empty():
            fill_value = mode_val[0]
            df = df.with_columns(pl.col(col).fill_null(fill_value).alias(col))

        imputed_counts[col] = null_before - df[col].null_count()

    return df, imputed_counts


def impute_constant(
    df: pl.DataFrame,
    column: str,
    value: Any,
) -> tuple[pl.DataFrame, int]:
    """Impute missing values with a constant.

    Args:
        df: Input DataFrame.
        column: Column to impute.
        value: Constant fill value.

    Returns:
        Tuple of (imputed DataFrame, number of rows imputed).
    """
    if column not in df.columns:
        return df, 0

    null_before = df[column].null_count()
    df = df.with_columns(pl.col(column).fill_null(value).alias(column))
    return df, null_before - df[column].null_count()


def run_imputation(
    df: pl.DataFrame,
    strategies: dict[str, str] | None = None,
    max_null_ratio: float = MAX_NULL_RATIO,
    log_to_mlflow: bool = True,
) -> tuple[pl.DataFrame, ImputationReport]:
    """Run imputation pipeline with multiple strategies.

    Args:
        df: Input DataFrame.
        strategies: Dict mapping column names to strategy names.
            Valid strategies: 'forward_fill', 'mean', 'median', 'mode', 'constant'.
            Columns not in dict default to 'mean' for numeric, 'mode' for string.
        max_null_ratio: Maximum ratio of nulls allowed before rejecting a column.
        log_to_mlflow: Whether to log results to MLflow.

    Returns:
        Tuple of (imputed DataFrame, ImputationReport).
    """
    report = ImputationReport()
    report.total_nulls_before = _count_nulls(df)

    if strategies is None:
        strategies = {}

    for col in df.columns:
        null_count = df[col].null_count()
        null_ratio = null_count / df.height if df.height > 0 else 0

        if null_count == 0:
            continue

        # Over-imputation prevention
        if null_ratio > max_null_ratio:
            logger.warning(
                "Column %s has %.1f%% nulls (>%d%%), skipping imputation",
                col,
                null_ratio * 100,
                max_null_ratio * 100,
            )
            report.columns_rejected.append(col)
            continue

        # Determine strategy
        strategy = strategies.get(col)
        if strategy is None:
            dtype = df.schema[col]
            if dtype in (pl.Float64, pl.Float32, pl.Int64, pl.Int32):
                strategy = "mean"
            else:
                strategy = "mode"

        # Apply strategy
        if strategy == "forward_fill":
            df, counts = impute_forward_fill(df, columns=[col])
        elif strategy == "mean":
            df, counts = impute_mean(df, columns=[col])
        elif strategy == "median":
            df, counts = impute_median(df, columns=[col])
        elif strategy == "mode":
            df, counts = impute_mode(df, columns=[col])
        else:
            logger.warning("Unknown strategy '%s' for column %s", strategy, col)
            continue

        if counts.get(col, 0) > 0:
            report.columns_imputed.append(col)
            report.rows_imputed[col] = counts[col]
            report.strategies_used[col] = strategy

    report.total_nulls_after = _count_nulls(df)

    logger.info("\n%s", report.summary())

    if log_to_mlflow:
        _log_imputation_to_mlflow(report)

    return df, report


def _log_imputation_to_mlflow(report: ImputationReport) -> None:
    """Log imputation results to MLflow.

    Args:
        report: ImputationReport with imputation details.
    """
    try:
        from src.utils.mlflow_client import _get_mlflow

        mlflow = _get_mlflow()
        if mlflow is None:
            logger.debug("MLflow not available, skipping imputation logging")
            return

        mlflow.set_tracking_uri("mlruns")
        mlflow.set_experiment("fpl_data_cleaning")
        with mlflow.start_run(run_name="data_imputation"):
            mlflow.log_param("total_nulls_before", report.total_nulls_before)
            mlflow.log_param("total_nulls_after", report.total_nulls_after)
            mlflow.log_param("columns_imputed", len(report.columns_imputed))
            mlflow.log_param("columns_rejected", len(report.columns_rejected))
            if report.total_nulls_before > 0:
                reduction = 1.0 - report.total_nulls_after / report.total_nulls_before
                mlflow.log_metric("null_reduction_ratio", reduction)
            for col in report.columns_imputed:
                mlflow.log_param(f"strategy_{col}", report.strategies_used[col])
                mlflow.log_param(f"rows_imputed_{col}", report.rows_imputed[col])
    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to log imputation to MLflow: %s", e)
