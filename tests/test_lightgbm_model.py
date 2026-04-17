"""Tests for LightGBM player performance prediction model."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from src.models.lightgbm_model import (
    get_feature_importance,
    load_model,
    predict_points,
    save_model,
    train_player_model,
)


class TestTrainPlayerModel:
    """Tests for train_player_model function."""

    def test_basic_training(self) -> None:
        """Test basic model training."""
        X_train = np.random.rand(100, 5)
        y_train = np.random.rand(100) * 10

        model, info = train_player_model(X_train, y_train, log_to_mlflow=False)
        assert info["n_features"] == 5
        assert info["n_train_samples"] == 100
        assert info["training_time_seconds"] > 0

    def test_training_with_validation(self) -> None:
        """Test training with validation set for early stopping."""
        X_train = np.random.rand(100, 5)
        y_train = np.random.rand(100) * 10
        X_val = np.random.rand(20, 5)
        y_val = np.random.rand(20) * 10

        model, info = train_player_model(
            X_train, y_train, X_val, y_val, log_to_mlflow=False
        )
        assert info["n_val_samples"] == 20
        assert "best_iteration" in info

    def test_custom_params(self) -> None:
        """Test training with custom hyperparameters."""
        X_train = np.random.rand(50, 3)
        y_train = np.random.rand(50) * 10

        custom_params = {"n_estimators": 100, "max_depth": 5}
        model, info = train_player_model(
            X_train, y_train, params=custom_params, log_to_mlflow=False
        )
        assert info["n_estimators"] == 100


class TestPredictPoints:
    """Tests for predict_points function."""

    def test_basic_prediction(self) -> None:
        """Test basic prediction."""
        X = np.random.rand(10, 5)
        # Create a simple model
        import lightgbm as lgb

        model = lgb.LGBMRegressor(n_estimators=10, random_state=42)
        X_train = np.random.rand(50, 5)
        y_train = np.random.rand(50) * 10
        model.fit(X_train, y_train)

        predictions = predict_points(model, X)
        assert predictions.shape[0] == 10
        # All predictions should be >= 0
        assert np.all(predictions >= 0)

    def test_predictions_clipped_to_zero(self) -> None:
        """Test that negative predictions are clipped to 0."""
        import lightgbm as lgb

        # Train a model that might predict negative values
        model = lgb.LGBMRegressor(n_estimators=10, random_state=42)
        X_train = np.random.rand(50, 5)
        y_train = np.zeros(50)  # All zeros
        model.fit(X_train, y_train)

        X = np.random.rand(10, 5) * 100  # Extreme values
        predictions = predict_points(model, X)
        assert np.all(predictions >= 0)


class TestSaveLoadModel:
    """Tests for model serialization."""

    @pytest.fixture(autouse=True)
    def _clean_model_dir(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary model directory."""
        monkeypatch.setattr("src.models.lightgbm_model.MODEL_DIR", tmp_path / "models")
        monkeypatch.setattr(
            "src.models.lightgbm_model.MODEL_FILE",
            tmp_path / "models" / "player_predictor.joblib",
        )
        monkeypatch.setattr(
            "src.models.lightgbm_model.FEATURE_NAMES_FILE",
            tmp_path / "models" / "feature_names.txt",
        )

    def test_save_and_load_model(self) -> None:
        """Test that model can be saved and loaded."""
        import lightgbm as lgb

        model = lgb.LGBMRegressor(n_estimators=10, random_state=42)
        X_train = np.random.rand(50, 5)
        y_train = np.random.rand(50) * 10
        model.fit(X_train, y_train)

        feature_names = ["a", "b", "c", "d", "e"]
        save_model(model, feature_names)

        loaded_model, loaded_features = load_model()
        assert loaded_features == feature_names
        # Predictions should be the same
        X_test = np.random.rand(5, 5)
        pred1 = model.predict(X_test)
        pred2 = loaded_model.predict(X_test)
        np.testing.assert_array_almost_equal(pred1, pred2)

    def test_load_missing_model_raises(self) -> None:
        """Test that loading a missing model raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_model(model_path=Path("/nonexistent/model.joblib"))


class TestFeatureImportance:
    """Tests for get_feature_importance function."""

    def test_returns_top_n_features(self) -> None:
        """Test that top N features are returned."""
        import lightgbm as lgb

        model = lgb.LGBMRegressor(n_estimators=10, random_state=42)
        X_train = np.random.rand(50, 10)
        y_train = np.random.rand(50) * 10
        model.fit(X_train, y_train)

        feature_names = [f"feat_{i}" for i in range(10)]
        importance = get_feature_importance(model, feature_names, top_n=5)
        assert len(importance) == 5
        # Should be sorted by importance
        values = list(importance.values())
        assert values == sorted(values, reverse=True)


class TestLogsToMlflow:
    """Tests for MLflow logging."""

    def test_logs_training_to_mlflow(self) -> None:
        """Test that training is logged to MLflow."""
        mock_mlflow = MagicMock()
        mock_mlflow.start_run.return_value.__enter__ = MagicMock()
        mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

        X_train = np.random.rand(50, 5)
        y_train = np.random.rand(50) * 10

        with patch("src.utils.mlflow_client._get_mlflow", return_value=mock_mlflow):
            train_player_model(
                X_train, y_train, feature_names=["a", "b", "c", "d", "e"]
            )

        mock_mlflow.log_param.assert_called()
        mock_mlflow.log_metric.assert_called()
