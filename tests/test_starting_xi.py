"""Tests for Starting XI prediction model."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from src.models.starting_xi import (
    get_feature_importance,
    load_model,
    predict_start_probability,
    save_model,
    train_starting_xi_model,
)


class TestTrainStartingXIModel:
    """Tests for train_starting_xi_model function."""

    def test_basic_training(self) -> None:
        """Test basic model training."""
        x_train = np.random.rand(100, 5)
        y_train = np.random.randint(0, 2, size=100).astype(np.int64)

        model, info = train_starting_xi_model(x_train, y_train, log_to_mlflow=False)
        assert info["n_features"] == 5
        assert info["n_train_samples"] == 100
        assert info["training_time_seconds"] > 0

    def test_training_with_validation(self) -> None:
        """Test training with validation set for early stopping."""
        x_train = np.random.rand(100, 5)
        y_train = np.random.randint(0, 2, size=100).astype(np.int64)
        x_val = np.random.rand(20, 5)
        y_val = np.random.randint(0, 2, size=20).astype(np.int64)

        model, info = train_starting_xi_model(
            x_train, y_train, x_val, y_val, log_to_mlflow=False
        )
        assert info["n_val_samples"] == 20
        assert "best_iteration" in info


class TestPredictStartProbability:
    """Tests for predict_start_probability function."""

    def test_basic_prediction(self) -> None:
        """Test basic prediction."""
        import lightgbm as lgb

        model = lgb.LGBMClassifier(n_estimators=10, random_state=42)
        x_train = np.random.rand(50, 5)
        y_train = np.random.randint(0, 2, size=50).astype(np.int64)
        model.fit(x_train, y_train)

        x = np.random.rand(10, 5)
        predictions = predict_start_probability(model, x)
        assert predictions.shape[0] == 10
        # All predictions should be between 0 and 1
        assert np.all((predictions >= 0) & (predictions <= 1))


class TestSaveLoadModel:
    """Tests for model serialization."""

    @pytest.fixture(autouse=True)
    def _clean_model_dir(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary model directory."""
        monkeypatch.setattr("src.models.starting_xi.MODEL_DIR", tmp_path / "models")
        monkeypatch.setattr(
            "src.models.starting_xi.MODEL_FILE",
            tmp_path / "models" / "starting_xi.joblib",
        )
        monkeypatch.setattr(
            "src.models.starting_xi.FEATURE_NAMES_FILE",
            tmp_path / "models" / "xi_feature_names.txt",
        )

    def test_save_and_load_model(self) -> None:
        """Test that model can be saved and loaded."""
        import lightgbm as lgb

        model = lgb.LGBMClassifier(n_estimators=10, random_state=42)
        x_train = np.random.rand(50, 5)
        y_train = np.random.randint(0, 2, size=50).astype(np.int64)
        model.fit(x_train, y_train)

        feature_names = ["a", "b", "c", "d", "e"]
        save_model(model, feature_names)

        loaded_model, loaded_features = load_model()
        assert loaded_features == feature_names
        # Predictions should be the same
        x_test = np.random.rand(5, 5)
        pred1 = model.predict_proba(x_test)[:, 1]
        pred2 = loaded_model.predict_proba(x_test)[:, 1]
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

        model = lgb.LGBMClassifier(n_estimators=10, random_state=42)
        x_train = np.random.rand(50, 10)
        y_train = np.random.randint(0, 2, size=50).astype(np.int64)
        model.fit(x_train, y_train)

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

        x_train = np.random.rand(50, 5)
        y_train = np.random.randint(0, 2, size=50).astype(np.int64)

        with patch("src.utils.mlflow_client._get_mlflow", return_value=mock_mlflow):
            train_starting_xi_model(
                x_train, y_train, feature_names=["a", "b", "c", "d", "e"]
            )

        mock_mlflow.log_param.assert_called()
        mock_mlflow.log_metric.assert_called()
