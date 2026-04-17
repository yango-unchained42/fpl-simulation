"""Tests for Starting XI prediction dataset builder."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from src.models.xi_dataset_builder import (
    build_xi_dataset,
    compute_class_weights,
)


class TestBuildXIDataset:
    """Tests for build_xi_dataset function."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary dataset file."""
        monkeypatch.setattr(
            "src.models.xi_dataset_builder.XI_DATASET_FILE",
            tmp_path / "xi_training_dataset.parquet",
        )
        monkeypatch.setattr(
            "src.models.xi_dataset_builder.DATASET_DIR",
            tmp_path,
        )

    def test_creates_target_variable(self) -> None:
        """Test that target variable is created correctly."""
        stats = pl.DataFrame(
            {
                "player_id": [1, 1, 1],
                "gameweek": [1, 2, 3],
                "minutes": [90, 15, 70],
                "total_points": [6, 2, 8],
            }
        )
        result = build_xi_dataset(stats, use_cache=False)
        assert "is_starter" in result.columns
        # 90 >= 60 -> 1, 15 < 60 -> 0, 70 >= 60 -> 1
        assert result["is_starter"].to_list() == [1, 0, 1]
        # minutes should be excluded (leakage)
        assert "minutes" not in result.columns

    def test_removes_excluded_columns(self) -> None:
        """Test that excluded columns are removed."""
        stats = pl.DataFrame(
            {
                "player_id": [1, 1],
                "gameweek": [1, 2],
                "minutes": [90, 90],
                "name": ["Saka", "Saka"],
                "total_points": [6, 8],
            }
        )
        result = build_xi_dataset(stats, use_cache=False)
        assert "name" not in result.columns
        assert (
            "minutes" not in result.columns
        )  # Used for target, removed to prevent leakage
        assert "total_points" in result.columns

    def test_merges_rolling_features(self) -> None:
        """Test that rolling features are merged correctly."""
        stats = pl.DataFrame(
            {
                "player_id": [1, 1],
                "gameweek": [1, 2],
                "minutes": [90, 90],
                "total_points": [6, 8],
            }
        )
        rolling = pl.DataFrame(
            {
                "player_id": [1, 1],
                "gameweek": [1, 2],
                "xg_rolling_mean_3": [0.5, 0.65],
            }
        )
        result = build_xi_dataset(stats, rolling_features=rolling, use_cache=False)
        assert "xg_rolling_mean_3" in result.columns

    def test_caching(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that dataset is cached and reused."""
        monkeypatch.setattr(
            "src.models.xi_dataset_builder.XI_DATASET_FILE",
            tmp_path / "xi_training_dataset.parquet",
        )
        monkeypatch.setattr(
            "src.models.xi_dataset_builder.DATASET_DIR",
            tmp_path,
        )

        stats = pl.DataFrame(
            {
                "player_id": [1, 1],
                "gameweek": [1, 2],
                "minutes": [90, 90],
                "total_points": [6, 8],
            }
        )

        result1 = build_xi_dataset(stats, use_cache=True)
        assert result1.shape[0] == 2

        # Second call should load from cache
        result2 = build_xi_dataset(stats, use_cache=True)
        assert result2.shape[0] == 2


class TestComputeClassWeights:
    """Tests for compute_class_weights function."""

    def test_computes_weights_for_imbalanced_data(self) -> None:
        """Test that weights are computed correctly for imbalanced data."""
        # 10 starters, 2 non-starters
        df = pl.DataFrame(
            {
                "is_starter": [1] * 10 + [0] * 2,
            }
        )
        weights = compute_class_weights(df)
        # scale_pos_weight = count(0) / count(1) = 2 / 10 = 0.2
        assert weights["scale_pos_weight"] == pytest.approx(0.2)

    def test_computes_weights_for_balanced_data(self) -> None:
        """Test that weights are 1.0 for balanced data."""
        df = pl.DataFrame(
            {
                "is_starter": [1] * 5 + [0] * 5,
            }
        )
        weights = compute_class_weights(df)
        assert weights["scale_pos_weight"] == pytest.approx(1.0)

    def test_handles_missing_target(self) -> None:
        """Test that missing target returns empty dict."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        weights = compute_class_weights(df)
        assert weights == {}
