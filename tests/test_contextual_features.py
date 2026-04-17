"""Tests for contextual features module."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from src.features.contextual_features import (
    compute_injury_suspension_impact,
    compute_international_break_impact,
    compute_rest_and_fatigue,
)


class TestRestAndFatigue:
    """Tests for rest and fatigue computation."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory."""
        monkeypatch.setattr(
            "src.features.contextual_features.CACHE_DIR", tmp_path / "context"
        )

    def test_basic_rest_fatigue(self) -> None:
        """Test basic rest and fatigue calculation."""
        stats = pl.DataFrame(
            {
                "player_id": [1, 1, 1],
                "gameweek": [1, 2, 3],
                "kickoff_time": [
                    "2024-08-17 12:30:00",
                    "2024-08-24 15:00:00",
                    "2024-08-31 17:30:00",
                ],
                "total_points": [6, 8, 10],
            }
        )
        result = compute_rest_and_fatigue(stats, use_cache=False, log_to_mlflow=False)
        assert "days_since_last_match" in result.columns

    def test_empty_data(self) -> None:
        """Test that empty data returns empty DataFrame."""
        stats = pl.DataFrame(
            {"player_id": [], "gameweek": [], "total_points": []},
            schema={
                "player_id": pl.Int64,
                "gameweek": pl.Int64,
                "total_points": pl.Int64,
            },
        )
        result = compute_rest_and_fatigue(stats, use_cache=False, log_to_mlflow=False)
        assert result.is_empty()

    def test_no_date_column(self) -> None:
        """Test that missing date column returns original DataFrame."""
        stats = pl.DataFrame(
            {
                "player_id": [1, 1],
                "gameweek": [1, 2],
                "total_points": [6, 8],
            }
        )
        result = compute_rest_and_fatigue(stats, use_cache=False, log_to_mlflow=False)
        # Should return original DataFrame unchanged
        assert result.shape[0] == 2
        assert "days_since_last_match" not in result.columns


class TestInjurySuspension:
    """Tests for injury/suspension impact computation."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory."""
        monkeypatch.setattr(
            "src.features.contextual_features.CACHE_DIR", tmp_path / "context"
        )

    def test_availability_flags(self) -> None:
        """Test that availability flags are computed correctly."""
        stats = pl.DataFrame(
            {
                "player_id": [1, 2, 3],
                "team": [1, 1, 2],
                "status": ["a", "d", "i"],
                "total_points": [6, 2, 0],
            }
        )
        result = compute_injury_suspension_impact(
            stats, use_cache=False, log_to_mlflow=False
        )
        assert "is_available" in result.columns
        assert "availability_score" in result.columns
        # Player 1 (a) should be available
        assert (
            result.filter(pl.col("player_id") == 1)["is_available"].to_list()[0] is True
        )
        # Player 3 (i) should not be available
        assert (
            result.filter(pl.col("player_id") == 3)["is_available"].to_list()[0]
            is False
        )

    def test_team_availability_rate(self) -> None:
        """Test team availability rate calculation."""
        stats = pl.DataFrame(
            {
                "player_id": [1, 2, 3, 4],
                "team": [1, 1, 1, 1],
                "status": ["a", "a", "i", "s"],
                "total_points": [6, 8, 0, 0],
            }
        )
        result = compute_injury_suspension_impact(
            stats, use_cache=False, log_to_mlflow=False
        )
        assert "team_availability_rate" in result.columns
        # 2 out of 4 available = 0.5
        assert result["team_availability_rate"].to_list()[0] == pytest.approx(0.5)

    def test_empty_data(self) -> None:
        """Test that empty data returns empty DataFrame."""
        stats = pl.DataFrame(
            {"player_id": [], "team": [], "status": []},
            schema={
                "player_id": pl.Int64,
                "team": pl.Int64,
                "status": pl.String,
            },
        )
        result = compute_injury_suspension_impact(
            stats, use_cache=False, log_to_mlflow=False
        )
        assert result.is_empty()


class TestInternationalBreak:
    """Tests for international break impact computation."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory."""
        monkeypatch.setattr(
            "src.features.contextual_features.CACHE_DIR", tmp_path / "context"
        )

    def test_basic_intl_break(self) -> None:
        """Test basic international break impact."""
        stats = pl.DataFrame(
            {
                "player_id": [1, 2, 3],
                "gameweek": [5, 5, 5],
                "total_points": [6, 8, 10],
            }
        )
        result = compute_international_break_impact(
            stats, use_cache=False, log_to_mlflow=False
        )
        assert "intl_break_flag" in result.columns
        assert "intl_minutes_total" in result.columns

    def test_intl_break_with_data(self) -> None:
        """Test international break with player data."""
        stats = pl.DataFrame(
            {
                "player_id": [1, 2, 3],
                "gameweek": [5, 5, 5],
                "total_points": [6, 8, 10],
            }
        )
        intl_players = pl.DataFrame(
            {
                "player_id": [1, 2],
                "minutes": [90, 45],
            }
        )
        result = compute_international_break_impact(
            stats,
            international_players=intl_players,
            use_cache=False,
            log_to_mlflow=False,
        )
        # Player 1 and 2 should have intl_break_flag = 1
        assert (
            result.filter(pl.col("player_id") == 1)["intl_break_flag"].to_list()[0] == 1
        )
        # Player 3 should have intl_break_flag = 0
        assert (
            result.filter(pl.col("player_id") == 3)["intl_break_flag"].to_list()[0] == 0
        )

    def test_empty_data(self) -> None:
        """Test that empty data returns empty DataFrame."""
        stats = pl.DataFrame(
            {"player_id": [], "gameweek": [], "total_points": []},
            schema={
                "player_id": pl.Int64,
                "gameweek": pl.Int64,
                "total_points": pl.Int64,
            },
        )
        result = compute_international_break_impact(
            stats, use_cache=False, log_to_mlflow=False
        )
        assert result.is_empty()
