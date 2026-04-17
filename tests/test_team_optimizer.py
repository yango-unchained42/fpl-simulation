"""Tests for team optimizer module."""

from __future__ import annotations

import numpy as np
import pytest

from src.models.team_optimizer import (
    BUDGET,
    MAX_PER_CLUB,
    POSITION_COUNTS,
    SQUAD_SIZE,
    optimize_squad,
)


class TestOptimizeSquad:
    """Tests for optimize_squad function."""

    @pytest.fixture
    def sample_players(self) -> dict[str, Any]:
        """Create sample player data for testing."""
        return {
            "player_ids": list(range(1, 21)),
            "prices": np.array(
                [
                    5.0,
                    5.5,
                    4.5,
                    4.0,
                    6.0,
                    6.5,
                    5.0,
                    4.5,
                    7.0,
                    7.5,
                    5.0,
                    5.5,
                    4.5,
                    4.0,
                    6.0,
                    6.5,
                    5.0,
                    4.5,
                    7.0,
                    7.5,
                ]
            ),
            "expected_points": np.array(
                [
                    3.0,
                    3.5,
                    2.5,
                    2.0,
                    4.0,
                    4.5,
                    3.0,
                    2.5,
                    5.0,
                    5.5,
                    3.0,
                    3.5,
                    2.5,
                    2.0,
                    4.0,
                    4.5,
                    3.0,
                    2.5,
                    5.0,
                    5.5,
                ]
            ),
            "positions": [
                "GK",
                "GK",
                "DEF",
                "DEF",
                "DEF",
                "DEF",
                "DEF",
                "MID",
                "MID",
                "MID",
                "MID",
                "MID",
                "MID",
                "FWD",
                "FWD",
                "FWD",
                "FWD",
                "FWD",
                "FWD",
                "FWD",
            ],
            "team_ids": [1, 2, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3],
            "statuses": ["a"] * 20,
        }

    def test_basic_optimization(self, sample_players: dict[str, Any]) -> None:
        """Test basic squad optimization."""
        result = optimize_squad(
            player_ids=sample_players["player_ids"],
            prices=sample_players["prices"],
            expected_points=sample_players["expected_points"],
            positions=sample_players["positions"],
            team_ids=sample_players["team_ids"],
            statuses=sample_players["statuses"],
        )
        assert "squad" in result
        assert "captain" in result
        assert len(result["squad"]) == SQUAD_SIZE
        assert len(result["captain"]) == 1
        assert result["captain"][0] in result["squad"]

    def test_budget_constraint(self, sample_players: dict[str, Any]) -> None:
        """Test that budget constraint is respected."""
        result = optimize_squad(
            player_ids=sample_players["player_ids"],
            prices=sample_players["prices"],
            expected_points=sample_players["expected_points"],
            positions=sample_players["positions"],
            team_ids=sample_players["team_ids"],
            statuses=sample_players["statuses"],
        )
        assert result["total_cost"] <= BUDGET

    def test_position_constraints(self, sample_players: dict[str, Any]) -> None:
        """Test that position constraints are respected."""
        result = optimize_squad(
            player_ids=sample_players["player_ids"],
            prices=sample_players["prices"],
            expected_points=sample_players["expected_points"],
            positions=sample_players["positions"],
            team_ids=sample_players["team_ids"],
            statuses=sample_players["statuses"],
        )
        # Count positions in selected squad
        pos_counts: dict[str, int] = {"GK": 0, "DEF": 0, "MID": 0, "FWD": 0}
        for pid in result["squad"]:
            idx = sample_players["player_ids"].index(pid)
            pos = sample_players["positions"][idx]
            pos_counts[pos] += 1

        for pos, expected_count in POSITION_COUNTS.items():
            assert pos_counts[pos] == expected_count

    def test_max_per_club_constraint(self, sample_players: dict[str, Any]) -> None:
        """Test that max 3 players per club constraint is respected."""
        result = optimize_squad(
            player_ids=sample_players["player_ids"],
            prices=sample_players["prices"],
            expected_points=sample_players["expected_points"],
            positions=sample_players["positions"],
            team_ids=sample_players["team_ids"],
            statuses=sample_players["statuses"],
        )
        # Count players per club
        club_counts: dict[int, int] = {}
        for pid in result["squad"]:
            idx = sample_players["player_ids"].index(pid)
            tid = sample_players["team_ids"][idx]
            club_counts[tid] = club_counts.get(tid, 0) + 1

        for count in club_counts.values():
            assert count <= MAX_PER_CLUB

    def test_injury_filter(self, sample_players: dict[str, Any]) -> None:
        """Test that injured players are excluded."""
        statuses = ["a"] * 20
        statuses[0] = "i"  # First player injured
        statuses[5] = "d"  # Sixth player doubtful

        result = optimize_squad(
            player_ids=sample_players["player_ids"],
            prices=sample_players["prices"],
            expected_points=sample_players["expected_points"],
            positions=sample_players["positions"],
            team_ids=sample_players["team_ids"],
            statuses=statuses,
        )
        assert sample_players["player_ids"][0] not in result["squad"]
        assert sample_players["player_ids"][5] not in result["squad"]

    def test_alternative_solutions(self, sample_players: dict[str, Any]) -> None:
        """Test generation of alternative solutions."""
        result = optimize_squad(
            player_ids=sample_players["player_ids"],
            prices=sample_players["prices"],
            expected_points=sample_players["expected_points"],
            positions=sample_players["positions"],
            team_ids=sample_players["team_ids"],
            statuses=sample_players["statuses"],
            n_alternatives=2,
        )
        assert "alternatives" in result
        assert len(result["alternatives"]) == 2
        # Alternatives should have different squads
        for alt in result["alternatives"]:
            assert set(alt["squad"]) != set(result["squad"])

    def test_optimization_time(self, sample_players: dict[str, Any]) -> None:
        """Test that optimization completes in reasonable time."""
        result = optimize_squad(
            player_ids=sample_players["player_ids"],
            prices=sample_players["prices"],
            expected_points=sample_players["expected_points"],
            positions=sample_players["positions"],
            team_ids=sample_players["team_ids"],
            statuses=sample_players["statuses"],
        )
        assert result["optimization_time_seconds"] < 300  # < 5 minutes
