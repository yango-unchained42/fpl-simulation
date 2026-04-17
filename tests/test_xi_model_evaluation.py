"""Tests for Starting XI model evaluation module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from src.models.xi_model_evaluation import (
    compute_classification_metrics,
    compute_confusion_matrix,
    compute_feature_importance,
    evaluate_xi_model,
)


class TestClassificationMetrics:
    """Tests for compute_classification_metrics function."""

    def test_perfect_predictions(self) -> None:
        """Test metrics for perfect predictions."""
        y_true = np.array([1, 0, 1, 0, 1])
        y_pred = np.array([1, 0, 1, 0, 1])
        y_proba = np.array([0.9, 0.1, 0.8, 0.2, 0.95])

        metrics = compute_classification_metrics(y_true, y_pred, y_proba)
        assert metrics["accuracy"] == pytest.approx(1.0)
        assert metrics["precision"] == pytest.approx(1.0)
        assert metrics["recall"] == pytest.approx(1.0)
        assert metrics["f1"] == pytest.approx(1.0)
        assert metrics["roc_auc"] == pytest.approx(1.0)

    def test_imperfect_predictions(self) -> None:
        """Test metrics for imperfect predictions."""
        y_true = np.array([1, 0, 1, 0, 1])
        y_pred = np.array([1, 0, 0, 0, 1])  # One false negative
        y_proba = np.array([0.9, 0.1, 0.4, 0.2, 0.95])

        metrics = compute_classification_metrics(y_true, y_pred, y_proba)
        assert metrics["accuracy"] == pytest.approx(0.8)
        assert metrics["recall"] < 1.0

    def test_handles_zero_division(self) -> None:
        """Test that metrics handle zero division gracefully."""
        y_true = np.array([0, 0, 0, 0, 0])
        y_pred = np.array([0, 0, 0, 0, 0])
        y_proba = np.array([0.1, 0.2, 0.1, 0.3, 0.2])

        metrics = compute_classification_metrics(y_true, y_pred, y_proba)
        assert "precision" in metrics
        assert "recall" in metrics


class TestConfusionMatrix:
    """Tests for compute_confusion_matrix function."""

    def test_perfect_predictions(self) -> None:
        """Test confusion matrix for perfect predictions."""
        y_true = np.array([1, 0, 1, 0])
        y_pred = np.array([1, 0, 1, 0])

        result = compute_confusion_matrix(y_true, y_pred)
        assert result["true_positives"] == 2
        assert result["true_negatives"] == 2
        assert result["false_positives"] == 0
        assert result["false_negatives"] == 0

    def test_imperfect_predictions(self) -> None:
        """Test confusion matrix for imperfect predictions."""
        y_true = np.array([1, 0, 1, 0])
        y_pred = np.array([1, 1, 0, 0])  # FP and FN

        result = compute_confusion_matrix(y_true, y_pred)
        assert result["true_positives"] == 1
        assert result["true_negatives"] == 1
        assert result["false_positives"] == 1
        assert result["false_negatives"] == 1


class TestFeatureImportance:
    """Tests for compute_feature_importance function."""

    def test_returns_top_n_features(self) -> None:
        """Test that top N features are returned."""
        import lightgbm as lgb

        model = lgb.LGBMClassifier(n_estimators=10, random_state=42)
        x_train = np.random.rand(50, 10)
        y_train = np.random.randint(0, 2, size=50).astype(np.int64)
        model.fit(x_train, y_train)

        feature_names = [f"feat_{i}" for i in range(10)]
        importance = compute_feature_importance(model, feature_names, top_n=5)
        assert len(importance) == 5
        # Should be sorted by importance
        values = list(importance.values())
        assert values == sorted(values, reverse=True)


class TestEvaluateXIModel:
    """Tests for evaluate_xi_model function."""

    def test_comprehensive_evaluation(self) -> None:
        """Test comprehensive model evaluation."""
        import lightgbm as lgb

        model = lgb.LGBMClassifier(n_estimators=10, random_state=42)
        x_train = np.random.rand(100, 5)
        y_train = np.random.randint(0, 2, size=100).astype(np.int64)
        model.fit(x_train, y_train)

        x_test = np.random.rand(20, 5)
        y_test = np.random.randint(0, 2, size=20).astype(np.int64)
        feature_names = ["a", "b", "c", "d", "e"]

        results = evaluate_xi_model(
            model, x_test, y_test, feature_names, log_to_mlflow=False
        )
        assert "metrics" in results
        assert "confusion_matrix" in results
        assert "feature_importance" in results
        assert "accuracy" in results["metrics"]
        assert "roc_auc" in results["metrics"]
        assert "true_positives" in results["confusion_matrix"]

    def test_logs_to_mlflow(self) -> None:
        """Test that evaluation is logged to MLflow."""
        import lightgbm as lgb

        mock_mlflow = MagicMock()
        mock_mlflow.start_run.return_value.__enter__ = MagicMock()
        mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

        model = lgb.LGBMClassifier(n_estimators=10, random_state=42)
        x_train = np.random.rand(50, 5)
        y_train = np.random.randint(0, 2, size=50).astype(np.int64)
        model.fit(x_train, y_train)

        x_test = np.random.rand(10, 5)
        y_test = np.random.randint(0, 2, size=10).astype(np.int64)
        feature_names = ["a", "b", "c", "d", "e"]

        with patch("src.utils.mlflow_client._get_mlflow", return_value=mock_mlflow):
            evaluate_xi_model(model, x_test, y_test, feature_names, log_to_mlflow=True)

        mock_mlflow.log_metric.assert_called()
