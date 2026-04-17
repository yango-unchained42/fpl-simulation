"""Tests for home/away H2H features module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.features.home_away_h2h import (
    _compute_player_advantage,
    _compute_player_home_away,
    _compute_team_advantage,
    _compute_team_home_away,
    compute_home_away_h2h,
)


class TestPlayerHomeAway:
    """Tests for player home/away split computation."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory."""
        monkeypatch.setattr("src.features.home_away_h2h.CACHE_DIR", tmp_path / "h2h")

    def test_home_away_splits(self) -> None:
        """Test that home and away stats are computed separately."""
        stats = pl.DataFrame(
            {
                "player_id": [1, 1, 1, 1],
                "opponent_team_id": [2, 2, 2, 2],
                "total_points": [10, 8, 2, 4],
                "xg": [0.8, 0.6, 0.1, 0.2],
                "gameweek": [1, 2, 3, 4],
                "was_home": [True, True, False, False],
            }
        )
        result = _compute_player_home_away(stats, windows=[3])
        assert "home_avg_total_points" in result.columns
        assert "away_avg_total_points" in result.columns
        assert "home_appearances" in result.columns
        assert "away_appearances" in result.columns

    def test_rolling_windows_home_away(self) -> None:
        """Test rolling windows for home/away splits."""
        stats = pl.DataFrame(
            {
                "player_id": [1] * 6,
                "opponent_team_id": [2] * 6,
                "total_points": [10, 8, 12, 2, 4, 6],
                "gameweek": [1, 2, 3, 4, 5, 6],
                "was_home": [True, True, True, False, False, False],
            }
        )
        result = _compute_player_home_away(stats, windows=[3])
        assert "home_total_points_rolling_3" in result.columns
        assert "away_total_points_rolling_3" in result.columns

    def test_empty_data(self) -> None:
        """Test that empty data returns empty DataFrame."""
        stats = pl.DataFrame(
            {
                "player_id": [],
                "opponent_team_id": [],
                "total_points": [],
                "gameweek": [],
                "was_home": [],
            },
            schema={
                "player_id": pl.Int64,
                "opponent_team_id": pl.Int64,
                "total_points": pl.Int64,
                "gameweek": pl.Int64,
                "was_home": pl.Boolean,
            },
        )
        result = _compute_player_home_away(stats, windows=[3])
        assert result.is_empty()

    def test_no_was_home_column(self) -> None:
        """Test that missing was_home returns empty DataFrame."""
        stats = pl.DataFrame(
            {
                "player_id": [1, 1],
                "opponent_team_id": [2, 2],
                "total_points": [10, 8],
            }
        )
        result = _compute_player_home_away(stats, windows=[3])
        assert result.is_empty()


class TestTeamHomeAway:
    """Tests for team home/away split computation."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory."""
        monkeypatch.setattr("src.features.home_away_h2h.CACHE_DIR", tmp_path / "h2h")

    def test_team_home_away_splits(self) -> None:
        """Test that team home/away stats are computed."""
        matches = pl.DataFrame(
            {
                "home_team_id": [1, 1, 2],
                "away_team_id": [2, 2, 1],
                "home_goals": [2, 1, 0],
                "away_goals": [1, 0, 3],
                "gameweek": [1, 2, 3],
            }
        )
        result = _compute_team_home_away(matches, windows=[3])
        assert "team_home_matches" in result.columns
        assert "team_away_matches" in result.columns

    def test_rolling_windows(self) -> None:
        """Test rolling windows for team home/away."""
        matches = pl.DataFrame(
            {
                "home_team_id": [1] * 5,
                "away_team_id": [2] * 5,
                "home_goals": [2, 1, 3, 0, 2],
                "away_goals": [1, 0, 2, 1, 1],
                "gameweek": [1, 2, 3, 4, 5],
            }
        )
        result = _compute_team_home_away(matches, windows=[3])
        assert "team_home_home_goals_rolling_3" in result.columns


class TestPlayerAdvantage:
    """Tests for home advantage factor computation."""

    def test_advantage_factors(self) -> None:
        """Test that home advantage factors are computed correctly."""
        df = pl.DataFrame(
            {
                "player_id": [1],
                "opponent_team_id": [2],
                "home_avg_total_points": [10.0],
                "away_avg_total_points": [5.0],
            }
        )
        result = _compute_player_advantage(df)
        assert "home_advantage_total_points" in result.columns
        assert "away_degradation_total_points" in result.columns
        # Home advantage: (10 - 5) / 5 = 1.0
        assert result["home_advantage_total_points"].to_list()[0] == pytest.approx(1.0)

    def test_empty_data(self) -> None:
        """Test that empty data returns empty DataFrame."""
        df = pl.DataFrame(
            {"player_id": [], "opponent_team_id": []},
            schema={"player_id": pl.Int64, "opponent_team_id": pl.Int64},
        )
        result = _compute_player_advantage(df)
        assert result.is_empty()


class TestTeamAdvantage:
    """Tests for team home advantage factor computation."""

    def test_team_advantage_factors(self) -> None:
        """Test that team home advantage factors are computed."""
        df = pl.DataFrame(
            {
                "home_team_id": [1],
                "away_team_id": [2],
                "team_home_avg_home_goals": [2.0],
                "team_away_avg_home_goals": [1.0],
            }
        )
        result = _compute_team_advantage(df)
        assert "team_home_advantage_home_goals" in result.columns
        assert "team_away_degradation_home_goals" in result.columns


class TestComputeHomeAwayH2H:
    """Tests for the main compute_home_away_h2h function."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory."""
        monkeypatch.setattr("src.features.home_away_h2h.CACHE_DIR", tmp_path / "h2h")

    def test_returns_all_dataframes(self) -> None:
        """Test that all four dataframes are returned."""
        player_stats = pl.DataFrame(
            {
                "player_id": [1, 1, 1, 1],
                "opponent_team_id": [2, 2, 2, 2],
                "total_points": [10, 8, 2, 4],
                "xg": [0.8, 0.6, 0.1, 0.2],
                "gameweek": [1, 2, 3, 4],
                "was_home": [True, True, False, False],
            }
        )
        matches = pl.DataFrame(
            {
                "home_team_id": [1, 2],
                "away_team_id": [2, 1],
                "home_goals": [2, 1],
                "away_goals": [1, 0],
                "gameweek": [1, 2],
            }
        )
        result = compute_home_away_h2h(
            player_stats, matches, use_cache=False, log_to_mlflow=False
        )
        assert "player_home_away" in result
        assert "team_home_away" in result
        assert "player_advantage" in result
        assert "team_advantage" in result

    def test_caching(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that results are cached and reused."""
        monkeypatch.setattr("src.features.home_away_h2h.CACHE_DIR", tmp_path / "h2h")
        monkeypatch.setattr("src.features.home_away_h2h.CACHE_TTL_SECONDS", 3600)

        player_stats = pl.DataFrame(
            {
                "player_id": [1, 1],
                "opponent_team_id": [2, 2],
                "total_points": [10, 8],
                "xg": [0.8, 0.6],
                "gameweek": [1, 2],
                "was_home": [True, False],
            }
        )
        matches = pl.DataFrame(
            {
                "home_team_id": [1],
                "away_team_id": [2],
                "home_goals": [2],
                "away_goals": [1],
                "gameweek": [1],
            }
        )

        result1 = compute_home_away_h2h(
            player_stats, matches, use_cache=True, log_to_mlflow=False
        )
        assert "player_home_away" in result1

    def test_logs_to_mlflow(self) -> None:
        """Test that results are logged to MLflow."""
        mock_mlflow = MagicMock()
        mock_mlflow.start_run.return_value.__enter__ = MagicMock()
        mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

        player_stats = pl.DataFrame(
            {
                "player_id": [1, 1],
                "opponent_team_id": [2, 2],
                "total_points": [10, 8],
                "xg": [0.8, 0.6],
                "gameweek": [1, 2],
                "was_home": [True, False],
            }
        )
        matches = pl.DataFrame(
            {
                "home_team_id": [1],
                "away_team_id": [2],
                "home_goals": [2],
                "away_goals": [1],
                "gameweek": [1],
            }
        )

        with patch("src.utils.mlflow_client._get_mlflow", return_value=mock_mlflow):
            compute_home_away_h2h(
                player_stats, matches, use_cache=False, log_to_mlflow=True
            )

        mock_mlflow.log_param.assert_called()
