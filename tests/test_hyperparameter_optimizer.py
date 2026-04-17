"""Tests for hyperparameter optimization module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from src.models.hyperparameter_optimizer import (
    load_optimization_results,
    optimize_hyperparameters,
    save_optimization_results,
)


class TestOptimizeHyperparameters:
    """Tests for optimize_hyperparameters function."""

    def test_basic_optimization(self) -> None:
        """Test basic hyperparameter optimization."""
        X_train = np.random.rand(100, 5)
        y_train = np.random.rand(100) * 10

        model, best_params, info = optimize_hyperparameters(
            X_train, y_train, n_iter=2, cv_folds=2, log_to_mlflow=False
        )
        assert "n_estimators" in best_params
        assert "learning_rate" in best_params
        assert info["n_iter"] == 2
        assert info["cv_folds"] == 2
        assert info["optimization_time_seconds"] > 0

    def test_custom_param_distributions(self) -> None:
        """Test optimization with custom search space."""
        X_train = np.random.rand(50, 3)
        y_train = np.random.rand(50) * 10

        custom_params = {"n_estimators": [50, 100], "max_depth": [3]}
        model, best_params, info = optimize_hyperparameters(
            X_train,
            y_train,
            param_distributions=custom_params,
            n_iter=2,
            cv_folds=2,
            log_to_mlflow=False,
        )
        assert "max_depth" in best_params
        assert best_params["max_depth"] == 3

    def test_returns_best_estimator(self) -> None:
        """Test that the returned model is the best estimator."""
        X_train = np.random.rand(50, 3)
        y_train = np.random.rand(50) * 10

        model, best_params, info = optimize_hyperparameters(
            X_train, y_train, n_iter=2, cv_folds=2, log_to_mlflow=False
        )
        # Should be a trained LGBMRegressor
        assert hasattr(model, "predict")
        predictions = model.predict(X_train)
        assert predictions.shape[0] == X_train.shape[0]


class TestSaveLoadOptimizationResults:
    """Tests for saving and loading optimization results."""

    @pytest.fixture(autouse=True)
    def _clean_results_dir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Use a temporary results directory."""
        monkeypatch.setattr(
            "src.models.hyperparameter_optimizer.OPTIMIZATION_FILE",
            tmp_path / "optimization_results.json",
        )
        monkeypatch.setattr(
            "src.models.hyperparameter_optimizer.MODEL_DIR",
            tmp_path,
        )

    def test_save_and_load_results(self) -> None:
        """Test that results can be saved and loaded."""
        best_params = {"n_estimators": 100, "learning_rate": 0.05}
        info = {"best_score": -1.5, "optimization_time_seconds": 10.0}

        save_optimization_results(best_params, info)

        loaded_params, loaded_info = load_optimization_results()
        assert loaded_params == best_params
        assert loaded_info == info

    def test_load_missing_results_raises(self) -> None:
        """Test that loading missing results raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_optimization_results(results_path=Path("/nonexistent/results.json"))


class TestLogsToMlflow:
    """Tests for MLflow logging."""

    def test_logs_optimization_to_mlflow(self) -> None:
        """Test that optimization is logged to MLflow."""
        mock_mlflow = MagicMock()
        mock_mlflow.start_run.return_value.__enter__ = MagicMock()
        mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

        X_train = np.random.rand(50, 3)
        y_train = np.random.rand(50) * 10

        with patch("src.utils.mlflow_client._get_mlflow", return_value=mock_mlflow):
            optimize_hyperparameters(
                X_train, y_train, n_iter=2, cv_folds=2, log_to_mlflow=True
            )

        mock_mlflow.log_param.assert_called()
        mock_mlflow.log_metric.assert_called()
