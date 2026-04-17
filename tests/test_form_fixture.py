"""Tests for form metrics and fixture difficulty modules."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from src.features.fixture_difficulty import (
    compute_fixture_difficulty,
    compute_strength_of_schedule,
    compute_team_strength,
)
from src.features.form_metrics import (
    compute_player_form,
    compute_team_form,
)


class TestPlayerForm:
    """Tests for player form computation."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory."""
        monkeypatch.setattr("src.features.form_metrics.CACHE_DIR", tmp_path / "form")

    def test_basic_player_form(self) -> None:
        """Test basic player form calculation."""
        stats = pl.DataFrame(
            {
                "player_id": [1, 1, 1, 1, 1],
                "gameweek": [1, 2, 3, 4, 5],
                "total_points": [2, 6, 8, 10, 12],
                "xg": [0.1, 0.5, 0.8, 1.0, 1.2],
            }
        )
        result = compute_player_form(stats, use_cache=False, log_to_mlflow=False)
        # Should have form columns
        assert "total_points_form_7d" in result.columns
        assert "xg_form_7d" in result.columns

    def test_form_multiple_windows(self) -> None:
        """Test that multiple form windows are computed."""
        stats = pl.DataFrame(
            {
                "player_id": [1] * 15,
                "gameweek": list(range(1, 16)),
                "total_points": list(range(1, 16)),
            }
        )
        result = compute_player_form(stats, use_cache=False, log_to_mlflow=False)
        assert "total_points_form_7d" in result.columns
        assert "total_points_form_14d" in result.columns
        assert "total_points_form_30d" in result.columns

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
        result = compute_player_form(stats, use_cache=False, log_to_mlflow=False)
        assert result.is_empty()

    def test_multiple_players(self) -> None:
        """Test form is computed per player independently."""
        stats = pl.DataFrame(
            {
                "player_id": [1, 1, 1, 2, 2, 2],
                "gameweek": [1, 2, 3, 1, 2, 3],
                "total_points": [2, 4, 6, 10, 20, 30],
            }
        )
        result = compute_player_form(stats, use_cache=False, log_to_mlflow=False)
        p1_form = result.filter(pl.col("player_id") == 1)[
            "total_points_form_7d"
        ].to_list()[-1]
        p2_form = result.filter(pl.col("player_id") == 2)[
            "total_points_form_7d"
        ].to_list()[-1]
        assert p1_form < p2_form


class TestTeamForm:
    """Tests for team form computation."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory."""
        monkeypatch.setattr("src.features.form_metrics.CACHE_DIR", tmp_path / "form")

    def test_basic_team_form(self) -> None:
        """Test basic team form calculation."""
        matches = pl.DataFrame(
            {
                "home_team_id": [1, 1, 1],
                "away_team_id": [2, 2, 2],
                "home_goals": [2, 1, 3],
                "away_goals": [1, 0, 2],
                "gameweek": [1, 2, 3],
            }
        )
        result = compute_team_form(matches, use_cache=False, log_to_mlflow=False)
        assert "team_home_goals_form_7d" in result.columns

    def test_empty_data(self) -> None:
        """Test that empty data returns empty DataFrame."""
        matches = pl.DataFrame(
            {
                "home_team_id": [],
                "away_team_id": [],
                "home_goals": [],
                "gameweek": [],
            },
            schema={
                "home_team_id": pl.Int64,
                "away_team_id": pl.Int64,
                "home_goals": pl.Int64,
                "gameweek": pl.Int64,
            },
        )
        result = compute_team_form(matches, use_cache=False, log_to_mlflow=False)
        assert result.is_empty()


class TestFixtureDifficulty:
    """Tests for fixture difficulty computation."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory."""
        monkeypatch.setattr(
            "src.features.fixture_difficulty.CACHE_DIR", tmp_path / "fixture"
        )

    def test_basic_fixture_difficulty(self) -> None:
        """Test basic fixture difficulty calculation."""
        fixtures = pl.DataFrame(
            {
                "fixture_id": [1, 2],
                "home_team_id": [1, 3],
                "away_team_id": [2, 4],
                "gameweek": [1, 1],
                "team_h_difficulty": [3, 4],
                "team_a_difficulty": [4, 3],
            }
        )
        result = compute_fixture_difficulty(
            fixtures, use_cache=False, log_to_mlflow=False
        )
        assert "home_difficulty" in result.columns
        assert "away_difficulty" in result.columns
        assert "overall_difficulty" in result.columns

    def test_empty_fixtures(self) -> None:
        """Test that empty fixtures returns empty DataFrame."""
        fixtures = pl.DataFrame(
            {
                "home_team_id": [],
                "away_team_id": [],
                "gameweek": [],
            },
            schema={
                "home_team_id": pl.Int64,
                "away_team_id": pl.Int64,
                "gameweek": pl.Int64,
            },
        )
        result = compute_fixture_difficulty(
            fixtures, use_cache=False, log_to_mlflow=False
        )
        assert result.is_empty()

    def test_default_difficulty(self) -> None:
        """Test default difficulty when no ratings available."""
        fixtures = pl.DataFrame(
            {
                "home_team_id": [1],
                "away_team_id": [2],
                "gameweek": [1],
            }
        )
        result = compute_fixture_difficulty(
            fixtures, use_cache=False, log_to_mlflow=False
        )
        assert result["home_difficulty"].to_list()[0] == 3
        assert result["away_difficulty"].to_list()[0] == 3


class TestTeamStrength:
    """Tests for team strength computation."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory."""
        monkeypatch.setattr(
            "src.features.fixture_difficulty.CACHE_DIR", tmp_path / "fixture"
        )

    def test_basic_team_strength(self) -> None:
        """Test basic team strength calculation."""
        team_stats = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Arsenal", "Chelsea", "Liverpool"],
                "strength": [5, 4, 5],
            }
        )
        result = compute_team_strength(team_stats, use_cache=False, log_to_mlflow=False)
        assert result.shape[0] == 3

    def test_team_strength_with_matches(self) -> None:
        """Test team strength with match data."""
        team_stats = pl.DataFrame(
            {
                "id": [1, 2],
                "strength": [5, 4],
                "strength_attack_home": [50, 40],
                "strength_defence_away": [40, 50],
            }
        )
        matches = pl.DataFrame(
            {
                "home_team_id": [1, 1],
                "away_team_id": [2, 2],
                "home_goals": [2, 3],
                "away_goals": [1, 0],
                "home_xg": [2.0, 2.5],
                "away_xg": [0.8, 0.5],
            }
        )
        result = compute_team_strength(
            team_stats, matches, use_cache=False, log_to_mlflow=False
        )
        assert "dynamic_attack_strength" in result.columns


class TestStrengthOfSchedule:
    """Tests for strength of schedule computation."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory."""
        monkeypatch.setattr(
            "src.features.fixture_difficulty.CACHE_DIR", tmp_path / "fixture"
        )

    def test_basic_sos(self) -> None:
        """Test basic strength of schedule calculation."""
        fixtures = pl.DataFrame(
            {
                "home_team_id": [1, 1, 2],
                "away_team_id": [2, 3, 1],
                "gameweek": [1, 2, 1],
                "overall_difficulty": [3, 4, 3],
            }
        )
        result = compute_strength_of_schedule(
            fixtures, use_cache=False, log_to_mlflow=False
        )
        assert "overall_sos" in result.columns

    def test_empty_fixtures(self) -> None:
        """Test that empty fixtures returns empty DataFrame."""
        fixtures = pl.DataFrame(
            {
                "home_team_id": [],
                "away_team_id": [],
                "gameweek": [],
            },
            schema={
                "home_team_id": pl.Int64,
                "away_team_id": pl.Int64,
                "gameweek": pl.Int64,
            },
        )
        result = compute_strength_of_schedule(
            fixtures, use_cache=False, log_to_mlflow=False
        )
        assert result.is_empty()
