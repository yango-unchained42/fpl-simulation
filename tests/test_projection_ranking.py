"""Tests for projection and ranking system."""

from __future__ import annotations

import numpy as np
import pytest

from src.models.projection_ranking import (
    PlayerProjection,
    generate_projections,
    get_captaincy_recommendations,
    get_differential_picks,
    projections_to_dataframe,
)


class TestGenerateProjections:
    """Tests for generate_projections function."""

    def test_basic_projections(self) -> None:
        """Test basic projection generation."""
        player_ids = [1, 2, 3]
        sim_results = [
            {"mean_points": 5.0},
            {"mean_points": 8.0},
            {"mean_points": 3.0},
        ]
        xi_probs = np.array([0.9, 0.95, 0.7])
        ownership = np.array([30.0, 50.0, 10.0])
        positions = ["MID", "FWD", "DEF"]

        projections = generate_projections(
            player_ids, sim_results, xi_probs, ownership, positions
        )
        assert len(projections) == 3
        # Sorted by combined score (descending)
        assert projections[0].player_id == 2  # 8.0 * 0.95 = 7.6
        assert projections[1].player_id == 1  # 5.0 * 0.9 = 4.5
        assert projections[2].player_id == 3  # 3.0 * 0.7 = 2.1

    def test_combined_score_calculation(self) -> None:
        """Test that combined score is expected_points * xi_probability."""
        player_ids = [1]
        sim_results = [{"mean_points": 10.0}]
        xi_probs = np.array([0.5])
        ownership = np.array([20.0])
        positions = ["MID"]

        projections = generate_projections(
            player_ids, sim_results, xi_probs, ownership, positions
        )
        assert projections[0].combined_score == pytest.approx(5.0)

    def test_differential_identification(self) -> None:
        """Test that differentials are correctly identified."""
        player_ids = [1, 2]
        sim_results = [{"mean_points": 5.0}, {"mean_points": 8.0}]
        xi_probs = np.array([0.9, 0.9])
        ownership = np.array([5.0, 50.0])  # Player 1 is differential
        positions = ["MID", "FWD"]

        projections = generate_projections(
            player_ids, sim_results, xi_probs, ownership, positions
        )
        assert projections[0].is_differential is False  # 50% ownership
        assert projections[1].is_differential is True  # 5% ownership

    def test_captaincy_picks(self) -> None:
        """Test that captaincy picks are correctly identified."""
        player_ids = [1, 2, 3, 4]
        sim_results = [
            {"mean_points": 5.0},
            {"mean_points": 8.0},
            {"mean_points": 3.0},
            {"mean_points": 7.0},
        ]
        xi_probs = np.array([0.9, 0.95, 0.7, 0.9])
        ownership = np.array([30.0, 50.0, 10.0, 40.0])
        positions = ["MID", "FWD", "DEF", "MID"]

        projections = generate_projections(
            player_ids, sim_results, xi_probs, ownership, positions, n_captain_picks=2
        )
        captain_picks = [p for p in projections if p.is_captain_pick]
        assert len(captain_picks) == 2
        # Top 2 by combined score: Player 2 (7.6), Player 4 (6.3)
        captain_ids = [p.player_id for p in captain_picks]
        assert 2 in captain_ids
        assert 4 in captain_ids

    def test_position_ranks(self) -> None:
        """Test that position ranks are correctly assigned."""
        player_ids = [1, 2, 3]
        sim_results = [
            {"mean_points": 5.0},
            {"mean_points": 8.0},
            {"mean_points": 3.0},
        ]
        xi_probs = np.array([0.9, 0.95, 0.7])
        ownership = np.array([30.0, 50.0, 10.0])
        positions = ["MID", "MID", "DEF"]

        projections = generate_projections(
            player_ids, sim_results, xi_probs, ownership, positions
        )
        # Player 2 is rank 1 MID, Player 1 is rank 2 MID
        mid_projs = [p for p in projections if p.position == "MID"]
        assert mid_projs[0].rank_by_position == 1
        assert mid_projs[1].rank_by_position == 2


class TestCaptaincyRecommendations:
    """Tests for get_captaincy_recommendations function."""

    def test_returns_top_picks(self) -> None:
        """Test that top captaincy picks are returned."""
        projections = [
            PlayerProjection(1, 5.0, 0.9, 4.5, 30.0, False, False, 2, 1, "MID"),
            PlayerProjection(2, 8.0, 0.95, 7.6, 50.0, False, True, 1, 1, "FWD"),
            PlayerProjection(3, 3.0, 0.7, 2.1, 10.0, True, False, 3, 1, "DEF"),
        ]
        recs = get_captaincy_recommendations(projections, top_n=2)
        assert len(recs) == 2
        assert recs[0]["player_id"] == 2
        assert recs[1]["player_id"] == 1

    def test_filters_low_xi_probability(self) -> None:
        """Test that players with low XI probability are filtered."""
        projections = [
            PlayerProjection(1, 5.0, 0.9, 4.5, 30.0, False, False, 1, 1, "MID"),
            PlayerProjection(2, 8.0, 0.3, 2.4, 50.0, False, False, 2, 1, "FWD"),
        ]
        recs = get_captaincy_recommendations(projections, top_n=2)
        assert len(recs) == 1
        assert recs[0]["player_id"] == 1

    def test_includes_reasoning(self) -> None:
        """Test that reasoning is included in recommendations."""
        projections = [
            PlayerProjection(1, 5.0, 0.9, 4.5, 5.0, True, True, 1, 1, "MID"),
        ]
        recs = get_captaincy_recommendations(projections, top_n=1)
        assert "reasoning" in recs[0]
        assert "Differential pick" in recs[0]["reasoning"]


class TestDifferentialPicks:
    """Tests for get_differential_picks function."""

    def test_returns_low_ownership_high_potential(self) -> None:
        """Test that differential picks are correctly identified."""
        projections = [
            PlayerProjection(1, 5.0, 0.9, 4.5, 5.0, True, False, 1, 1, "MID"),
            PlayerProjection(2, 8.0, 0.95, 7.6, 50.0, False, True, 2, 1, "FWD"),
            PlayerProjection(3, 3.0, 0.7, 2.1, 8.0, True, False, 3, 1, "DEF"),
        ]
        diffs = get_differential_picks(projections, top_n=2)
        assert len(diffs) == 2
        # Sorted by combined score: Player 1 (4.5), Player 3 (2.1)
        assert diffs[0]["player_id"] == 1
        assert diffs[1]["player_id"] == 3


class TestProjectionsToDataframe:
    """Tests for projections_to_dataframe function."""

    def test_converts_to_dataframe(self) -> None:
        """Test that projections are correctly converted to DataFrame."""
        projections = [
            PlayerProjection(1, 5.0, 0.9, 4.5, 30.0, False, False, 1, 1, "MID"),
            PlayerProjection(2, 8.0, 0.95, 7.6, 50.0, False, True, 2, 1, "FWD"),
        ]
        df = projections_to_dataframe(projections)
        assert df.shape == (2, 10)
        assert "player_id" in df.columns
        assert "combined_score" in df.columns
        assert df["player_id"].to_list() == [1, 2]
