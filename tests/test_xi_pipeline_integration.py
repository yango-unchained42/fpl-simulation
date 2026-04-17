"""Integration tests for Starting XI prediction pipeline."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from src.models.starting_xi import (
    predict_start_probability,
    train_starting_xi_model,
)
from src.models.xi_dataset_builder import build_xi_dataset
from src.models.xi_model_evaluation import evaluate_xi_model


class TestXIPipelineIntegration:
    """Tests for the complete Starting XI prediction pipeline."""

    @pytest.fixture(autouse=True)
    def _clean_dirs(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use temporary directories for all model artifacts."""
        monkeypatch.setattr("src.models.xi_dataset_builder.DATASET_DIR", tmp_path)
        monkeypatch.setattr(
            "src.models.xi_dataset_builder.XI_DATASET_FILE",
            tmp_path / "xi_dataset.parquet",
        )
        monkeypatch.setattr("src.models.starting_xi.MODEL_DIR", tmp_path)
        monkeypatch.setattr(
            "src.models.starting_xi.MODEL_FILE", tmp_path / "starting_xi.joblib"
        )
        monkeypatch.setattr(
            "src.models.starting_xi.FEATURE_NAMES_FILE",
            tmp_path / "xi_feature_names.txt",
        )

    def test_full_pipeline_flow(self) -> None:
        """Test the full flow from raw data to evaluation."""
        # 1. Create synthetic raw data
        n_samples = 200
        stats = pl.DataFrame(
            {
                "player_id": list(range(n_samples)),
                "gameweek": [1] * n_samples,
                "minutes": np.random.randint(0, 90, n_samples),
                "total_points": np.random.randint(0, 20, n_samples),
                "xg": np.random.rand(n_samples),
                "xa": np.random.rand(n_samples),
            }
        )

        # 2. Build Dataset
        dataset = build_xi_dataset(stats, use_cache=False)
        assert "is_starter" in dataset.columns
        assert "minutes" not in dataset.columns  # Should be removed (leakage)

        # 3. Prepare features
        feature_cols = [
            c
            for c in dataset.columns
            if c not in ("player_id", "gameweek", "is_starter")
        ]
        X = dataset.select(feature_cols).to_numpy()
        y = dataset.select("is_starter").to_numpy().ravel().astype(np.int64)

        # 4. Train Model
        model, info = train_starting_xi_model(X, y, log_to_mlflow=False)
        assert info["n_features"] == len(feature_cols)
        assert info["n_train_samples"] == n_samples

        # 5. Predict
        probas = predict_start_probability(model, X)
        assert probas.shape[0] == n_samples
        # All probabilities must be between 0 and 1
        assert np.all((probas >= 0) & (probas <= 1))

        # 6. Evaluate
        results = evaluate_xi_model(
            model, X, y, feature_names=feature_cols, log_to_mlflow=False
        )
        assert "metrics" in results
        assert "accuracy" in results["metrics"]
        assert "roc_auc" in results["metrics"]
        assert "confusion_matrix" in results
        assert "feature_importance" in results

    def test_pipeline_with_rolling_features(self) -> None:
        """Test pipeline integration with rolling features."""
        stats = pl.DataFrame(
            {
                "player_id": [1, 1, 1],
                "gameweek": [1, 2, 3],
                "minutes": [90, 90, 90],
                "total_points": [6, 8, 10],
            }
        )
        rolling = pl.DataFrame(
            {
                "player_id": [1, 1, 1],
                "gameweek": [1, 2, 3],
                "xg_rolling_mean_3": [0.5, 0.6, 0.7],
            }
        )

        dataset = build_xi_dataset(stats, rolling_features=rolling, use_cache=False)
        assert "xg_rolling_mean_3" in dataset.columns

        feature_cols = [
            c
            for c in dataset.columns
            if c not in ("player_id", "gameweek", "is_starter")
        ]
        X = dataset.select(feature_cols).to_numpy()
        y = dataset.select("is_starter").to_numpy().ravel().astype(np.int64)

        model, _ = train_starting_xi_model(X, y, log_to_mlflow=False)
        probas = predict_start_probability(model, X)
        assert probas.shape[0] == 3

    def test_pipeline_with_class_imbalance(self) -> None:
        """Test pipeline handles class imbalance correctly."""
        # Create imbalanced data: 90% starters, 10% non-starters
        n_samples = 100
        stats = pl.DataFrame(
            {
                "player_id": list(range(n_samples)),
                "gameweek": [1] * n_samples,
                "minutes": [90] * 90 + [10] * 10,  # 90 starters
                "total_points": np.random.randint(0, 20, n_samples),
            }
        )

        dataset = build_xi_dataset(stats, use_cache=False)
        starter_ratio = dataset["is_starter"].mean()
        assert starter_ratio == pytest.approx(0.9)

        feature_cols = [
            c
            for c in dataset.columns
            if c not in ("player_id", "gameweek", "is_starter")
        ]
        X = dataset.select(feature_cols).to_numpy()
        y = dataset.select("is_starter").to_numpy().ravel().astype(np.int64)

        # Model should train without error despite imbalance
        model, info = train_starting_xi_model(X, y, log_to_mlflow=False)
        assert info["n_features"] == len(feature_cols)

        # Predictions should still be valid probabilities
        probas = predict_start_probability(model, X)
        assert np.all((probas >= 0) & (probas <= 1))
