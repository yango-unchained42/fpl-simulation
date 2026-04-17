"""Tests for training dataset builder."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from src.models.dataset_builder import (
    TARGET_COL,
    build_training_dataset,
    compute_feature_correlations,
    compute_feature_importance_baseline,
    create_time_based_splits,
)


class TestBuildTrainingDataset:
    """Tests for build_training_dataset function."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary dataset file."""
        monkeypatch.setattr(
            "src.models.dataset_builder.DATASET_FILE",
            tmp_path / "training_dataset.parquet",
        )
        monkeypatch.setattr(
            "src.models.dataset_builder.DATASET_DIR",
            tmp_path,
        )

    def test_creates_target_variable(self) -> None:
        """Test that target variable is created by shifting points."""
        stats = pl.DataFrame(
            {
                "player_id": [1, 1, 1, 2, 2, 2],
                "gameweek": [1, 2, 3, 1, 2, 3],
                "total_points": [6, 8, 10, 2, 4, 6],
            }
        )
        result = build_training_dataset(stats, use_cache=False)
        assert TARGET_COL in result.columns
        # Player 1: GW1 target = 8 (GW2 points), GW2 target = 10
        p1_gw1 = result.filter((pl.col("player_id") == 1) & (pl.col("gameweek") == 1))
        assert p1_gw1[TARGET_COL].to_list()[0] == 8

    def test_removes_excluded_columns(self) -> None:
        """Test that excluded columns are removed."""
        stats = pl.DataFrame(
            {
                "player_id": [1, 1],
                "gameweek": [1, 2],
                "total_points": [6, 8],
                "name": ["Saka", "Saka"],
                "web_name": ["Saka", "Saka"],
                "xg": [0.5, 0.8],
            }
        )
        result = build_training_dataset(stats, use_cache=False)
        assert "name" not in result.columns
        assert "web_name" not in result.columns
        assert "xg" in result.columns

    def test_merges_rolling_features(self) -> None:
        """Test that rolling features are merged correctly."""
        stats = pl.DataFrame(
            {
                "player_id": [1, 1],
                "gameweek": [1, 2],
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
        result = build_training_dataset(
            stats, rolling_features=rolling, use_cache=False
        )
        assert "xg_rolling_mean_3" in result.columns

    def test_removes_null_target_rows(self) -> None:
        """Test that rows with null target are removed."""
        stats = pl.DataFrame(
            {
                "player_id": [1, 1],
                "gameweek": [1, 2],
                "total_points": [6, 8],
            }
        )
        result = build_training_dataset(stats, use_cache=False)
        # Last GW per player has no next GW, so should be removed
        assert result.shape[0] == 1
        assert result["gameweek"].to_list()[0] == 1

    def test_caching(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that dataset is cached and reused."""
        monkeypatch.setattr(
            "src.models.dataset_builder.DATASET_FILE",
            tmp_path / "training_dataset.parquet",
        )
        monkeypatch.setattr(
            "src.models.dataset_builder.DATASET_DIR",
            tmp_path,
        )

        stats = pl.DataFrame(
            {
                "player_id": [1, 1],
                "gameweek": [1, 2],
                "total_points": [6, 8],
            }
        )

        result1 = build_training_dataset(stats, use_cache=True)
        assert result1.shape[0] == 1

        # Second call should load from cache
        result2 = build_training_dataset(stats, use_cache=True)
        assert result2.shape[0] == 1


class TestFeatureCorrelations:
    """Tests for compute_feature_correlations function."""

    def test_identifies_high_correlation(self) -> None:
        """Test that highly correlated features are identified."""
        df = pl.DataFrame(
            {
                "a": [1.0, 2.0, 3.0, 4.0, 5.0],
                "b": [1.0, 2.0, 3.0, 4.0, 5.0],  # Perfectly correlated with a
                "c": [5.0, 4.0, 3.0, 2.0, 1.0],  # Negatively correlated
            }
        )
        pairs = compute_feature_correlations(df, threshold=0.95)
        # a and b should be flagged
        assert any(p[0] == "a" and p[1] == "b" for p in pairs)

    def test_no_correlation_below_threshold(self) -> None:
        """Test that no pairs are flagged below threshold."""
        df = pl.DataFrame(
            {
                "a": [1.0, 2.0, 3.0, 4.0, 5.0],
                "b": [5.0, 1.0, 3.0, 2.0, 4.0],  # Low correlation
            }
        )
        pairs = compute_feature_correlations(df, threshold=0.95)
        assert len(pairs) == 0

    def test_empty_dataframe(self) -> None:
        """Test that empty DataFrame returns empty list."""
        df = pl.DataFrame({"a": []}, schema={"a": pl.Float64})
        pairs = compute_feature_correlations(df)
        assert pairs == []


class TestTimeBasedSplits:
    """Tests for create_time_based_splits function."""

    def test_creates_proper_splits(self) -> None:
        """Test that splits are created correctly."""
        df = pl.DataFrame(
            {
                "player_id": [1] * 38,
                "gameweek": list(range(1, 39)),
                "total_points": list(range(1, 39)),
            }
        )
        splits = create_time_based_splits(df)
        assert "train" in splits
        assert "val" in splits
        assert "test" in splits
        # Train should have ~70% of GWs (1-26)
        assert splits["train"]["gameweek"].max() <= 26
        # Test should have ~15% of GWs (33-38)
        assert splits["test"]["gameweek"].min() >= 33

    def test_raises_without_time_col(self) -> None:
        """Test that missing time column raises error."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        with pytest.raises(ValueError):
            create_time_based_splits(df)

    def test_no_data_leakage(self) -> None:
        """Test that there is no overlap between splits."""
        df = pl.DataFrame(
            {
                "player_id": [1] * 38,
                "gameweek": list(range(1, 39)),
                "total_points": list(range(1, 39)),
            }
        )
        splits = create_time_based_splits(df)
        train_gw = set(splits["train"]["gameweek"].to_list())
        val_gw = set(splits["val"]["gameweek"].to_list())
        test_gw = set(splits["test"]["gameweek"].to_list())
        assert train_gw.isdisjoint(val_gw)
        assert val_gw.isdisjoint(test_gw)
        assert train_gw.isdisjoint(test_gw)


class TestFeatureImportanceBaseline:
    """Tests for compute_feature_importance_baseline function."""

    def test_computes_importance(self) -> None:
        """Test that feature importance is computed."""
        df = pl.DataFrame(
            {
                "xg": [0.5, 0.8, 1.0, 0.3, 0.9],
                "xa": [0.2, 0.4, 0.1, 0.3, 0.5],
                "next_gw_points": [6, 10, 14, 2, 12],
            }
        )
        importance = compute_feature_importance_baseline(df)
        assert "xg" in importance
        assert "xa" in importance
        # xG should be more correlated with points than xA
        assert importance["xg"] > importance["xa"]

    def test_empty_dataframe(self) -> None:
        """Test that empty DataFrame returns empty dict."""
        df = pl.DataFrame(
            {"xg": [], "next_gw_points": []},
            schema={"xg": pl.Float64, "next_gw_points": pl.Float64},
        )
        importance = compute_feature_importance_baseline(df)
        assert importance == {}

    def test_missing_target(self) -> None:
        """Test that missing target returns empty dict."""
        df = pl.DataFrame({"xg": [0.5, 0.8]})
        importance = compute_feature_importance_baseline(df)
        assert importance == {}
