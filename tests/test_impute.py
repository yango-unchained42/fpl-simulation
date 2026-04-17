"""Tests for missing data imputation module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.data.impute import (
    ImputationReport,
    impute_constant,
    impute_forward_fill,
    impute_mean,
    impute_median,
    impute_mode,
    run_imputation,
)


class TestImputationReport:
    """Tests for ImputationReport dataclass."""

    def test_summary_format(self) -> None:
        """Test that summary generates readable output."""
        report = ImputationReport(
            columns_imputed=["goals", "assists"],
            rows_imputed={"goals": 5, "assists": 3},
            strategies_used={"goals": "mean", "assists": "median"},
            total_nulls_before=10,
            total_nulls_after=2,
        )
        summary = report.summary()
        assert "Imputation Report" in summary
        assert "goals" in summary
        assert "mean" in summary
        assert "10" in summary

    def test_rejected_columns_in_summary(self) -> None:
        """Test that rejected columns appear in summary."""
        report = ImputationReport(
            columns_rejected=["xg", "xa"],
            total_nulls_before=100,
            total_nulls_after=100,
        )
        summary = report.summary()
        assert "xg" in summary
        assert "Rejected" in summary


class TestImputeForwardFill:
    """Tests for forward-fill imputation."""

    def test_basic_forward_fill(self) -> None:
        """Test basic forward fill on single column."""
        df = pl.DataFrame({"value": [1.0, None, None, 4.0, None]})
        result, counts = impute_forward_fill(df, columns=["value"])
        assert result["value"].to_list() == [1.0, 1.0, 1.0, 4.0, 4.0]
        assert counts["value"] == 3

    def test_forward_fill_with_group_by(self) -> None:
        """Test forward fill respects group boundaries."""
        df = pl.DataFrame(
            {
                "player_id": [1, 1, 1, 2, 2, 2],
                "gameweek": [1, 2, 3, 1, 2, 3],
                "points": [6.0, None, None, 2.0, None, None],
            }
        )
        result, counts = impute_forward_fill(
            df, columns=["points"], sort_by="gameweek", group_by="player_id"
        )
        assert result["points"].to_list() == [6.0, 6.0, 6.0, 2.0, 2.0, 2.0]

    def test_no_nulls_unchanged(self) -> None:
        """Test DataFrame with no nulls is unchanged."""
        df = pl.DataFrame({"value": [1.0, 2.0, 3.0]})
        result, counts = impute_forward_fill(df, columns=["value"])
        assert result["value"].to_list() == [1.0, 2.0, 3.0]
        assert counts == {}


class TestImputeMean:
    """Tests for mean imputation."""

    def test_mean_imputation(self) -> None:
        """Test mean imputation on single column."""
        df = pl.DataFrame({"value": [1.0, 2.0, None, 4.0]})
        result, counts = impute_mean(df, columns=["value"])
        assert result["value"].to_list() == [1.0, 2.0, pytest.approx(7 / 3), 4.0]
        assert counts["value"] == 1

    def test_mean_imputation_with_group_by(self) -> None:
        """Test mean imputation with grouping."""
        df = pl.DataFrame(
            {
                "position": ["FWD", "FWD", "MID", "MID"],
                "goals": [10, None, 5, None],
            }
        )
        result, counts = impute_mean(df, columns=["goals"], group_by="position")
        assert result["goals"].to_list()[1] == pytest.approx(10.0)
        assert result["goals"].to_list()[3] == pytest.approx(5.0)

    def test_no_nulls_unchanged(self) -> None:
        """Test DataFrame with no nulls is unchanged."""
        df = pl.DataFrame({"value": [1.0, 2.0, 3.0]})
        result, counts = impute_mean(df, columns=["value"])
        assert result["value"].to_list() == [1.0, 2.0, 3.0]
        assert counts == {}


class TestImputeMedian:
    """Tests for median imputation."""

    def test_median_imputation(self) -> None:
        """Test median imputation on single column."""
        df = pl.DataFrame({"value": [1.0, 2.0, None, 100.0]})
        result, counts = impute_median(df, columns=["value"])
        # Median of [1, 2, 100] is 2
        assert result["value"].to_list() == [1.0, 2.0, 2.0, 100.0]
        assert counts["value"] == 1

    def test_median_with_outliers(self) -> None:
        """Test median is robust to outliers."""
        df = pl.DataFrame({"value": [1.0, 2.0, None, 1000.0]})
        result, _ = impute_median(df, columns=["value"])
        # Median should be 2, not affected by 1000
        assert result["value"].to_list()[2] == 2.0


class TestImputeMode:
    """Tests for mode imputation."""

    def test_mode_imputation(self) -> None:
        """Test mode imputation on categorical column."""
        df = pl.DataFrame({"position": ["FWD", "MID", "MID", None, "FWD"]})
        result, counts = impute_mode(df, columns=["position"])
        mode_val = result["position"].to_list()[3]
        assert mode_val in ("FWD", "MID")  # Both appear twice
        assert counts["position"] == 1

    def test_mode_imputation_clear_winner(self) -> None:
        """Test mode imputation with clear winner."""
        df = pl.DataFrame({"position": ["FWD", "FWD", "FWD", None, "MID"]})
        result, counts = impute_mode(df, columns=["position"])
        assert result["position"].to_list()[3] == "FWD"
        assert counts["position"] == 1


class TestImputeConstant:
    """Tests for constant imputation."""

    def test_constant_imputation(self) -> None:
        """Test constant value imputation."""
        df = pl.DataFrame({"value": [1.0, None, 3.0, None]})
        result, count = impute_constant(df, "value", 0.0)
        assert result["value"].to_list() == [1.0, 0.0, 3.0, 0.0]
        assert count == 2

    def test_constant_imputation_missing_column(self) -> None:
        """Test constant imputation on non-existent column."""
        df = pl.DataFrame({"value": [1.0, None]})
        result, count = impute_constant(df, "nonexistent", 0.0)
        assert result.shape == df.shape
        assert count == 0


class TestRunImputation:
    """Tests for the full imputation pipeline."""

    def test_auto_strategy_selection(self) -> None:
        """Test that strategies are auto-selected based on column type."""
        df = pl.DataFrame(
            {
                "numeric_col": [1.0, 2.0, None, 4.0],
                "string_col": ["A", "A", None, "B"],
            }
        )
        result, report = run_imputation(df, log_to_mlflow=False)
        assert "numeric_col" in report.columns_imputed
        assert "string_col" in report.columns_imputed
        assert report.strategies_used["numeric_col"] == "mean"
        assert report.strategies_used["string_col"] == "mode"

    def test_custom_strategies(self) -> None:
        """Test custom strategy specification."""
        df = pl.DataFrame(
            {
                "col_a": [1.0, 2.0, None, 4.0],
                "col_b": [10.0, None, 30.0, None],
            }
        )
        result, report = run_imputation(
            df,
            strategies={"col_a": "median", "col_b": "forward_fill"},
            log_to_mlflow=False,
        )
        assert report.strategies_used["col_a"] == "median"
        assert report.strategies_used["col_b"] == "forward_fill"

    def test_over_imputation_prevention(self) -> None:
        """Test that columns with too many nulls are rejected."""
        df = pl.DataFrame(
            {
                "good_col": [1.0, 2.0, None, 4.0],
                "bad_col": [None, None, None, 1.0],  # 75% nulls
            }
        )
        result, report = run_imputation(df, max_null_ratio=0.5, log_to_mlflow=False)
        assert "good_col" in report.columns_imputed
        assert "bad_col" in report.columns_rejected

    def test_null_reduction_tracked(self) -> None:
        """Test that null reduction is tracked in report."""
        df = pl.DataFrame({"value": [1.0, 2.0, None, 4.0]})
        result, report = run_imputation(df, log_to_mlflow=False)
        assert report.total_nulls_before == 1
        assert report.total_nulls_after == 0

    def test_logs_to_mlflow_when_enabled(self) -> None:
        """Test that results are logged to MLflow when enabled."""
        mock_mlflow = MagicMock()
        mock_mlflow.start_run.return_value.__enter__ = MagicMock()
        mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

        df = pl.DataFrame({"value": [1.0, 2.0, None, 4.0]})

        with patch("src.utils.mlflow_client._get_mlflow", return_value=mock_mlflow):
            run_imputation(df, log_to_mlflow=True)

        mock_mlflow.log_param.assert_called()
        mock_mlflow.log_metric.assert_called()

    def test_skips_mlflow_when_disabled(self) -> None:
        """Test that MLflow logging is skipped when disabled."""
        df = pl.DataFrame({"value": [1.0, 2.0, None, 4.0]})

        with patch("src.utils.mlflow_client._get_mlflow") as mock_get:
            run_imputation(df, log_to_mlflow=False)
            mock_get.assert_not_called()
