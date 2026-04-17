"""Tests for model evaluation module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from src.models.model_evaluation import (
    compare_with_baseline,
    compute_error_by_category,
    compute_feature_importance,
    compute_regression_metrics,
    compute_residuals,
    evaluate_model,
)


class TestRegressionMetrics:
    """Tests for compute_regression_metrics function."""

    def test_perfect_predictions(self) -> None:
        """Test metrics for perfect predictions."""
        y_true = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        y_pred = np.array([1.0, 2.0, 3.0, 4.0, 5.0])

        metrics = compute_regression_metrics(y_true, y_pred)
        assert metrics["rmse"] == pytest.approx(0.0)
        assert metrics["mae"] == pytest.approx(0.0)
        assert metrics["r2"] == pytest.approx(1.0)

    def test_imperfect_predictions(self) -> None:
        """Test metrics for imperfect predictions."""
        y_true = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        y_pred = np.array([1.5, 2.5, 3.5, 4.5, 5.5])

        metrics = compute_regression_metrics(y_true, y_pred)
        assert metrics["mae"] == pytest.approx(0.5)
        assert metrics["rmse"] == pytest.approx(0.5)

    def test_mape_with_zeros(self) -> None:
        """Test MAPE handling of zero values."""
        y_true = np.array([0.0, 2.0, 0.0, 4.0, 0.0])
        y_pred = np.array([0.5, 2.5, 0.5, 4.5, 0.5])

        metrics = compute_regression_metrics(y_true, y_pred)
        # Should not raise division by zero
        assert "mape" in metrics


class TestResiduals:
    """Tests for compute_residuals function."""

    def test_residual_calculation(self) -> None:
        """Test that residuals are calculated correctly."""
        y_true = np.array([10.0, 20.0, 30.0])
        y_pred = np.array([8.0, 22.0, 28.0])

        analysis = compute_residuals(y_true, y_pred)
        expected_residuals = np.array([2.0, -2.0, 2.0])
        np.testing.assert_array_almost_equal(analysis["residuals"], expected_residuals)
        assert analysis["mean_residual"] == pytest.approx(2.0 / 3)

    def test_bias_calculation(self) -> None:
        """Test that bias is calculated correctly."""
        y_true = np.array([10.0, 20.0, 30.0])
        y_pred = np.array([8.0, 18.0, 28.0])  # Underpredicts by 2

        analysis = compute_residuals(y_true, y_pred)
        assert analysis["bias"] == pytest.approx(2.0)


class TestErrorByCategory:
    """Tests for compute_error_by_category function."""

    def test_category_breakdown(self) -> None:
        """Test error breakdown by category."""
        y_true = np.array([10.0, 20.0, 30.0, 40.0])
        y_pred = np.array([10.0, 20.0, 30.0, 40.0])  # Perfect
        categories = np.array(["A", "A", "B", "B"])

        results = compute_error_by_category(y_true, y_pred, categories)
        assert "A" in results
        assert "B" in results
        assert results["A"]["mae"] == pytest.approx(0.0)
        assert results["A"]["count"] == 2

    def test_imperfect_category_errors(self) -> None:
        """Test error calculation for imperfect predictions."""
        y_true = np.array([10.0, 20.0])
        y_pred = np.array([12.0, 18.0])
        categories = np.array(["A", "A"])

        results = compute_error_by_category(y_true, y_pred, categories)
        assert results["A"]["mae"] == pytest.approx(2.0)


class TestFeatureImportance:
    """Tests for compute_feature_importance function."""

    def test_returns_top_n_features(self) -> None:
        """Test that top N features are returned."""
        import lightgbm as lgb

        model = lgb.LGBMRegressor(n_estimators=10, random_state=42)
        X_train = np.random.rand(50, 10)
        y_train = np.random.rand(50) * 10
        model.fit(X_train, y_train)

        feature_names = [f"feat_{i}" for i in range(10)]
        importance = compute_feature_importance(model, feature_names, top_n=5)
        assert len(importance) == 5
        # Should be sorted by importance
        values = list(importance.values())
        assert values == sorted(values, reverse=True)


class TestBaselineComparison:
    """Tests for compare_with_baseline function."""

    def test_model_better_than_baseline(self) -> None:
        """Test that a good model beats the mean baseline."""
        y_true = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        y_pred = np.array([1.1, 2.1, 3.1, 4.1, 5.1])  # Good predictions

        results = compare_with_baseline(y_true, y_pred)
        assert "model" in results
        assert "mean_baseline" in results
        assert "median_baseline" in results
        # Model should have lower MAE than mean baseline
        assert results["model"]["mae"] < results["mean_baseline"]["mae"]

    def test_improvement_percentage(self) -> None:
        """Test improvement calculation."""
        y_true = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        y_pred = np.array([1.0, 2.0, 3.0, 4.0, 5.0])  # Perfect

        results = compare_with_baseline(y_true, y_pred)
        assert results["improvement_over_mean_baseline_pct"] > 0


class TestEvaluateModel:
    """Tests for evaluate_model function."""

    def test_comprehensive_evaluation(self) -> None:
        """Test comprehensive model evaluation."""
        import lightgbm as lgb

        model = lgb.LGBMRegressor(n_estimators=10, random_state=42)
        X_train = np.random.rand(100, 5)
        y_train = np.random.rand(100) * 10
        model.fit(X_train, y_train)

        X_test = np.random.rand(20, 5)
        y_test = np.random.rand(20) * 10
        feature_names = ["a", "b", "c", "d", "e"]

        results = evaluate_model(
            model, X_test, y_test, feature_names, log_to_mlflow=False
        )
        assert "metrics" in results
        assert "residuals" in results
        assert "feature_importance" in results
        assert "baseline_comparison" in results
        assert "rmse" in results["metrics"]
        assert "mae" in results["metrics"]
        assert "r2" in results["metrics"]

    def test_logs_to_mlflow(self) -> None:
        """Test that evaluation is logged to MLflow."""
        import lightgbm as lgb

        mock_mlflow = MagicMock()
        mock_mlflow.start_run.return_value.__enter__ = MagicMock()
        mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

        model = lgb.LGBMRegressor(n_estimators=10, random_state=42)
        X_train = np.random.rand(50, 5)
        y_train = np.random.rand(50) * 10
        model.fit(X_train, y_train)

        X_test = np.random.rand(10, 5)
        y_test = np.random.rand(10) * 10
        feature_names = ["a", "b", "c", "d", "e"]

        with patch("src.utils.mlflow_client._get_mlflow", return_value=mock_mlflow):
            evaluate_model(model, X_test, y_test, feature_names, log_to_mlflow=True)

        mock_mlflow.log_metric.assert_called()
