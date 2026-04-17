"""Tests for H2H metrics module."""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.features.h2h_metrics import (
    clear_cache,
    compute_h2h_features,
    compute_player_vs_team,
    compute_team_h2h,
)


class TestH2HCache:
    """Tests for H2H caching mechanism."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory for each test."""
        monkeypatch.setattr("src.features.h2h_metrics.CACHE_DIR", tmp_path / "h2h")

    def test_cache_saves_and_loads(self) -> None:
        """Test that cache saves DataFrame and loads it back."""
        from src.features.h2h_metrics import (
            _cache_key,
            _is_cache_valid,
            _load_cache,
            _save_cache,
        )

        cache_path = _cache_key("test_func", {"seasons": ["2023-24"]})
        test_df = pl.DataFrame({"a": [1], "b": [2]})
        _save_cache(cache_path, test_df)
        assert cache_path.exists()
        loaded = _load_cache(cache_path)
        assert loaded.shape == test_df.shape
        assert _is_cache_valid(cache_path)

    def test_cache_invalid_when_missing(self) -> None:
        """Test that cache is invalid when file doesn't exist."""
        from src.features.h2h_metrics import _cache_key, _is_cache_valid

        cache_path = _cache_key("test_func", {"key": "value"})
        assert not _is_cache_valid(cache_path)

    def test_cache_invalid_when_expired(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that cache is invalid when TTL expired."""
        import os

        from src.features.h2h_metrics import _cache_key, _is_cache_valid, _save_cache

        cache_path = _cache_key("test_func", {"key": "value"})
        test_df = pl.DataFrame({"a": [1]})
        _save_cache(cache_path, test_df)
        old_time = time.time() - 172800  # 48 hours ago
        os.utime(cache_path, (old_time, old_time))
        assert not _is_cache_valid(cache_path, ttl=86400)

    def test_clear_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that clear_cache removes all cached files."""
        from src.features.h2h_metrics import _cache_key, _save_cache

        cache_path1 = _cache_key("func1", {"a": 1})
        cache_path2 = _cache_key("func2", {"b": 2})
        _save_cache(cache_path1, pl.DataFrame({"a": [1]}))
        _save_cache(cache_path2, pl.DataFrame({"b": [2]}))
        clear_cache()
        assert not cache_path1.exists()
        assert not cache_path2.exists()


class TestComputeTeamH2H:
    """Tests for compute_team_h2h function."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory for each test."""
        monkeypatch.setattr("src.features.h2h_metrics.CACHE_DIR", tmp_path / "h2h")

    def test_basic_h2h_metrics(self) -> None:
        """Test basic H2H metric calculation."""
        matches = pl.DataFrame(
            {
                "home_team_id": [1, 1, 2],
                "away_team_id": [2, 2, 1],
                "home_goals": [2, 1, 0],
                "away_goals": [1, 0, 3],
                "season": ["2023-24", "2023-24", "2023-24"],
            }
        )
        result = compute_team_h2h(matches, use_cache=False, log_to_mlflow=False)
        assert result.shape[0] == 2  # Two team pairs
        # New naming convention: avg_home_goals, avg_away_goals, etc.
        assert "avg_home_goals" in result.columns
        assert "avg_away_goals" in result.columns
        assert "total_matches" in result.columns

    def test_filters_by_season(self) -> None:
        """Test that only requested seasons are included."""
        matches = pl.DataFrame(
            {
                "home_team_id": [1, 1],
                "away_team_id": [2, 2],
                "home_goals": [2, 1],
                "away_goals": [1, 0],
                "season": ["2022-23", "2023-24"],
            }
        )
        result = compute_team_h2h(
            matches, seasons=["2023-24"], use_cache=False, log_to_mlflow=False
        )
        assert result.shape[0] == 1

    def test_empty_matches_returns_empty(self) -> None:
        """Test that empty input returns empty DataFrame."""
        matches = pl.DataFrame(
            {
                "home_team_id": [],
                "away_team_id": [],
                "home_goals": [],
                "away_goals": [],
                "season": [],
            },
            schema={
                "home_team_id": pl.Int64,
                "away_team_id": pl.Int64,
                "home_goals": pl.Int64,
                "away_goals": pl.Int64,
                "season": pl.String,
            },
        )
        result = compute_team_h2h(matches, use_cache=False, log_to_mlflow=False)
        assert result.is_empty()

    def test_rolling_windows(self) -> None:
        """Test rolling window H2H features."""
        matches = pl.DataFrame(
            {
                "home_team_id": [1] * 5,
                "away_team_id": [2] * 5,
                "home_goals": [2, 1, 3, 0, 2],
                "away_goals": [1, 0, 2, 1, 1],
                "gameweek": [1, 2, 3, 4, 5],
                "season": ["2023-24"] * 5,
            }
        )
        result = compute_team_h2h(
            matches, windows=[3], use_cache=False, log_to_mlflow=False
        )
        # Should have rolling window columns
        assert "home_goals_h2h_last_3" in result.columns
        assert "away_goals_h2h_last_3" in result.columns

    def test_uses_cache_when_available(self) -> None:
        """Test that cached data is used when available."""
        from src.features.h2h_metrics import _cache_key, _save_cache

        cache_path = _cache_key(
            "compute_team_h2h_v2", {"seasons": ["2023-24"], "windows": [3, 5, 10]}
        )
        cached_df = pl.DataFrame(
            {
                "home_team_id": [1],
                "away_team_id": [2],
                "avg_home_goals": [2.0],
            }
        )
        _save_cache(cache_path, cached_df)

        with patch("src.features.h2h_metrics._save_cache") as mock_save:
            result = compute_team_h2h(
                pl.DataFrame(), seasons=["2023-24"], use_cache=True, log_to_mlflow=False
            )
            mock_save.assert_not_called()
            assert result.shape[0] == 1


class TestComputePlayerVsTeam:
    """Tests for compute_player_vs_team function."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory for each test."""
        monkeypatch.setattr("src.features.h2h_metrics.CACHE_DIR", tmp_path / "h2h")

    def test_basic_player_vs_team_metrics(self) -> None:
        """Test basic player vs team metric calculation."""
        stats = pl.DataFrame(
            {
                "player_id": [1, 1, 2],
                "opponent_team_id": [3, 3, 4],
                "total_points": [6, 8, 2],
                "xg": [0.5, 0.8, 0.1],
                "goals_scored": [1, 1, 0],
                "shots": [3, 4, 1],
                "season": ["2023-24", "2023-24", "2023-24"],
            }
        )
        result = compute_player_vs_team(stats, use_cache=False, log_to_mlflow=False)
        assert result.shape[0] == 2  # Two player-team pairs
        assert "avg_total_points" in result.columns
        assert "avg_xg" in result.columns
        assert "appearances" in result.columns

    def test_home_away_splits(self) -> None:
        """Test home/away split calculations."""
        stats = pl.DataFrame(
            {
                "player_id": [1, 1, 1, 1],
                "opponent_team_id": [3, 3, 3, 3],
                "total_points": [10, 8, 2, 4],
                "xg": [0.8, 0.6, 0.1, 0.2],
                "goals_scored": [1, 1, 0, 0],
                "shots": [4, 3, 1, 1],
                "was_home": [True, True, False, False],
                "season": ["2023-24"] * 4,
            }
        )
        result = compute_player_vs_team(stats, use_cache=False, log_to_mlflow=False)
        assert "avg_total_points_home" in result.columns
        assert "avg_total_points_away" in result.columns

    def test_rolling_windows(self) -> None:
        """Test rolling window H2H features."""
        stats = pl.DataFrame(
            {
                "player_id": [1] * 7,
                "opponent_team_id": [3] * 7,
                "total_points": [2, 4, 6, 8, 10, 12, 14],
                "xg": [0.1] * 7,
                "goals_scored": [0] * 7,
                "shots": [1] * 7,
                "gameweek": [1, 2, 3, 4, 5, 6, 7],
                "season": ["2023-24"] * 7,
            }
        )
        result = compute_player_vs_team(
            stats, windows=[3], use_cache=False, log_to_mlflow=False
        )
        assert "total_points_h2h_last_3" in result.columns
        assert "goals_scored_h2h_sum_last_3" in result.columns

    def test_recent_form(self) -> None:
        """Test recent form (last 5 meetings) calculation."""
        stats = pl.DataFrame(
            {
                "player_id": [1] * 7,
                "opponent_team_id": [3] * 7,
                "total_points": [2, 4, 6, 8, 10, 12, 14],
                "xg": [0.1] * 7,
                "goals_scored": [0] * 7,
                "shots": [1] * 7,
                "gameweek": [1, 2, 3, 4, 5, 6, 7],
                "season": ["2023-24"] * 7,
            }
        )
        result = compute_player_vs_team(stats, use_cache=False, log_to_mlflow=False)
        assert "recent_total_points_last_5" in result.columns
        # Last 5: [6, 8, 10, 12, 14] -> mean = 10
        assert result["recent_total_points_last_5"].to_list()[0] == 10.0

    def test_uses_cache_when_available(self) -> None:
        """Test that cached data is used when available."""
        from src.features.h2h_metrics import _cache_key, _save_cache

        cache_path = _cache_key(
            "compute_player_vs_team_v2",
            {"seasons": ["2023-24"], "windows": [3, 5, 10]},
        )
        cached_df = pl.DataFrame(
            {
                "player_id": [1],
                "opponent_team_id": [3],
                "avg_total_points": [6.0],
            }
        )
        _save_cache(cache_path, cached_df)

        with patch("src.features.h2h_metrics._save_cache") as mock_save:
            result = compute_player_vs_team(
                pl.DataFrame(), seasons=["2023-24"], use_cache=True, log_to_mlflow=False
            )
            mock_save.assert_not_called()
            assert result.shape[0] == 1


class TestComputeH2HFeatures:
    """Tests for compute_h2h_features function."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory for each test."""
        monkeypatch.setattr("src.features.h2h_metrics.CACHE_DIR", tmp_path / "h2h")

    def test_returns_both_dataframes(self) -> None:
        """Test that both team_h2h and player_vs_team are returned."""
        matches = pl.DataFrame(
            {
                "home_team_id": [1],
                "away_team_id": [2],
                "home_goals": [2],
                "away_goals": [1],
                "season": ["2023-24"],
            }
        )
        stats = pl.DataFrame(
            {
                "player_id": [1],
                "opponent_team_id": [2],
                "total_points": [6],
                "xg": [0.5],
                "goals_scored": [1],
                "shots": [3],
                "season": ["2023-24"],
            }
        )
        result = compute_h2h_features(
            matches, stats, use_cache=False, log_to_mlflow=False
        )
        assert "team_h2h" in result
        assert "player_vs_team" in result
        assert result["team_h2h"].shape[0] == 1
        assert result["player_vs_team"].shape[0] == 1

    def test_logs_to_mlflow_when_enabled(self) -> None:
        """Test that results are logged to MLflow when enabled."""
        mock_mlflow = MagicMock()
        mock_mlflow.start_run.return_value.__enter__ = MagicMock()
        mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

        matches = pl.DataFrame(
            {
                "home_team_id": [1],
                "away_team_id": [2],
                "home_goals": [2],
                "away_goals": [1],
                "season": ["2023-24"],
            }
        )
        stats = pl.DataFrame(
            {
                "player_id": [1],
                "opponent_team_id": [2],
                "total_points": [6],
                "xg": [0.5],
                "goals_scored": [1],
                "shots": [3],
                "season": ["2023-24"],
            }
        )

        with patch("src.utils.mlflow_client._get_mlflow", return_value=mock_mlflow):
            compute_h2h_features(matches, stats, use_cache=False)

        mock_mlflow.log_param.assert_called()

    def test_skips_mlflow_when_disabled(self) -> None:
        """Test that MLflow logging is skipped when disabled."""
        matches = pl.DataFrame(
            {
                "home_team_id": [1],
                "away_team_id": [2],
                "home_goals": [2],
                "away_goals": [1],
                "season": ["2023-24"],
            }
        )
        stats = pl.DataFrame(
            {
                "player_id": [1],
                "opponent_team_id": [2],
                "total_points": [6],
                "xg": [0.5],
                "goals_scored": [1],
                "shots": [3],
                "season": ["2023-24"],
            }
        )

        with patch("src.utils.mlflow_client._get_mlflow") as mock_get:
            compute_h2h_features(matches, stats, use_cache=False, log_to_mlflow=False)
            mock_get.assert_not_called()
