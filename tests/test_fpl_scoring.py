"""Tests for FPL scoring rules engine."""

from __future__ import annotations

from src.utils.fpl_scoring import (
    calculate_bps,
    calculate_fpl_points,
    simulate_bonus_points,
)


class TestCalculateBPS:
    """Tests for calculate_bps function."""

    def test_basic_bps(self) -> None:
        """Test basic BPS calculation."""
        bps = calculate_bps(goals=1, assists=1, clean_sheet=True)
        assert bps > 0

    def test_bps_with_negative_events(self) -> None:
        """Test BPS with negative events (cards, goals conceded)."""
        bps = calculate_bps(yellow_cards=2, red_cards=1, goals_conceded=3)
        assert bps == 0  # Should be clamped to 0

    def test_bps_goalkeeper(self) -> None:
        """Test BPS for goalkeeper with saves."""
        bps = calculate_bps(saves=9, clean_sheet=True)
        assert bps > 0

    def test_bps_defender(self) -> None:
        """Test BPS for defender with tackles and clearances."""
        bps = calculate_bps(tackles=5, interceptions=3, clearances=10, clean_sheet=True)
        assert bps > 0


class TestCalculateFPLPoints:
    """Tests for calculate_fpl_points function."""

    def test_midfielder_goals_assists(self) -> None:
        """Test midfielder scoring goals and assists."""
        points = calculate_fpl_points(position="MID", minutes=90, goals=1, assists=1)
        # 2 (minutes) + 5 (goal) + 3 (assist) = 10
        assert points == 10

    def test_defender_clean_sheet(self) -> None:
        """Test defender with clean sheet."""
        points = calculate_fpl_points(
            position="DEF", minutes=90, clean_sheet=True, goals_conceded=0
        )
        # 2 (minutes) + 4 (clean sheet) = 6
        assert points == 6

    def test_goalkeeper_saves(self) -> None:
        """Test goalkeeper with saves."""
        points = calculate_fpl_points(
            position="GK", minutes=90, saves=9, clean_sheet=True
        )
        # 2 (minutes) + 4 (clean sheet) + 3 (saves: 9//3) = 9
        assert points == 9

    def test_cards_penalty(self) -> None:
        """Test card point deductions."""
        points = calculate_fpl_points(
            position="MID", minutes=90, yellow_cards=1, red_cards=1
        )
        # 2 (minutes) - 1 (yellow) - 3 (red) = -2 -> clamped to 0
        assert points == 0

    def test_penalty_events(self) -> None:
        """Test penalty save and miss."""
        points = calculate_fpl_points(position="GK", minutes=90, penalty_save=True)
        # 2 (minutes) + 5 (penalty save) = 7
        assert points == 7

    def test_bonus_points(self) -> None:
        """Test bonus points addition."""
        points = calculate_fpl_points(position="MID", minutes=90, goals=1, bonus=3)
        # 2 (minutes) + 5 (goal) + 3 (bonus) = 10
        assert points == 10

    def test_sub_appearance(self) -> None:
        """Test substitute appearance (< 60 minutes)."""
        points = calculate_fpl_points(position="MID", minutes=30, goals=1)
        # 1 (minutes < 60) + 5 (goal) = 6
        assert points == 6


class TestSimulateBonusPoints:
    """Tests for simulate_bonus_points function."""

    def test_basic_bonus_allocation(self) -> None:
        """Test basic bonus point allocation."""
        bps_scores = {1: 50, 2: 40, 3: 30, 4: 20, 5: 10}
        bonus = simulate_bonus_points(bps_scores)
        assert bonus[1] == 3  # Top BPS gets 3
        assert bonus[2] == 2  # Second gets 2
        assert bonus[3] == 1  # Third gets 1
        assert bonus[4] == 0
        assert bonus[5] == 0

    def test_single_player(self) -> None:
        """Test bonus allocation with single player."""
        bps_scores = {1: 50}
        bonus = simulate_bonus_points(bps_scores)
        assert bonus[1] == 3

    def test_empty_scores(self) -> None:
        """Test bonus allocation with no players."""
        bonus = simulate_bonus_points({})
        assert bonus == {}
