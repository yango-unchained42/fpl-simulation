"""Tests for Monte Carlo simulation engine."""

from __future__ import annotations

import numpy as np
import pytest

from src.models.match_simulator import (
    PlayerSimulationResult,
    SimulationConfig,
    simulate_double_gameweek,
    simulate_player_points,
)


class TestSimulatePlayerPoints:
    """Tests for simulate_player_points function."""

    def test_basic_simulation(self) -> None:
        """Test basic simulation returns correct number of results."""
        player_ids = [1, 2, 3]
        expected_points = np.array([5.0, 8.0, 3.0])
        xi_probabilities = np.array([0.9, 0.95, 0.7])

        config = SimulationConfig(n_simulations=1000)
        results = simulate_player_points(
            player_ids, expected_points, xi_probabilities, config=config
        )
        assert len(results) == 3
        assert all(isinstance(r, PlayerSimulationResult) for r in results)
        assert results[0].player_id == 1
        assert results[1].player_id == 2
        assert results[2].player_id == 3

    def test_injury_filter(self) -> None:
        """Test that injured players get 0 points."""
        player_ids = [1, 2]
        expected_points = np.array([5.0, 8.0])
        xi_probabilities = np.array([0.9, 0.95])
        statuses = ["i", "a"]  # Player 1 injured

        config = SimulationConfig(n_simulations=1000)
        results = simulate_player_points(
            player_ids,
            expected_points,
            xi_probabilities,
            statuses=statuses,
            config=config,
        )
        # Injured player should have 0 mean points
        assert results[0].mean_points == pytest.approx(0.0)
        # Available player should have positive mean points
        assert results[1].mean_points > 0

    def test_xi_probability_affects_start_rate(self) -> None:
        """Test that XI probability affects start rate."""
        player_ids = [1]
        expected_points = np.array([5.0])
        xi_probabilities = np.array([0.1])  # Low start probability

        config = SimulationConfig(n_simulations=10000)
        results = simulate_player_points(
            player_ids, expected_points, xi_probabilities, config=config
        )
        # Start probability should be close to the input probability
        # (with some variance from sub appearances)
        assert results[0].start_probability < 0.2

    def test_reproducibility_with_seed(self) -> None:
        """Test that results are reproducible with same seed."""
        player_ids = [1, 2, 3]
        expected_points = np.array([5.0, 8.0, 3.0])
        xi_probabilities = np.array([0.9, 0.95, 0.7])

        config = SimulationConfig(n_simulations=1000, random_seed=42)
        results1 = simulate_player_points(
            player_ids, expected_points, xi_probabilities, config=config
        )
        results2 = simulate_player_points(
            player_ids, expected_points, xi_probabilities, config=config
        )

        # Results should be identical with same seed
        for r1, r2 in zip(results1, results2):
            assert r1.mean_points == pytest.approx(r2.mean_points)

    def test_percentile_ordering(self) -> None:
        """Test that percentiles are correctly ordered."""
        player_ids = [1]
        expected_points = np.array([5.0])
        xi_probabilities = np.array([0.9])

        config = SimulationConfig(n_simulations=10000)
        results = simulate_player_points(
            player_ids, expected_points, xi_probabilities, config=config
        )
        r = results[0]
        assert r.p10_points <= r.p25_points <= r.median_points
        assert r.median_points <= r.p75_points <= r.p90_points

    def test_points_floor(self) -> None:
        """Test that points floor is enforced for starters."""
        player_ids = [1]
        expected_points = np.array([5.0])
        xi_probabilities = np.array([1.0])  # 100% start rate

        config = SimulationConfig(n_simulations=1000, min_points_floor=1.0)
        results = simulate_player_points(
            player_ids, expected_points, xi_probabilities, config=config
        )
        # With 100% start rate, all simulated points should be >= 1.0
        assert results[0].raw_points.min() >= 1.0

    def test_points_cap(self) -> None:
        """Test that points cap is enforced."""
        player_ids = [1]
        expected_points = np.array([20.0])  # High expected points
        xi_probabilities = np.array([0.9])

        config = SimulationConfig(n_simulations=1000, max_points_cap=15.0)
        results = simulate_player_points(
            player_ids, expected_points, xi_probabilities, config=config
        )
        # All simulated points should be <= 15.0
        assert results[0].raw_points.max() <= 15.0


class TestSimulateDoubleGameweek:
    """Tests for simulate_double_gameweek function."""

    def test_dgw_higher_mean_points(self) -> None:
        """Test that DGW players have higher mean points."""
        player_ids = [1]
        gw1_points = np.array([5.0])
        gw2_points = np.array([5.0])
        xi_probabilities = np.array([0.9])

        # Single GW simulation
        single_config = SimulationConfig(n_simulations=10000)
        single_results = simulate_player_points(
            player_ids, gw1_points, xi_probabilities, config=single_config
        )
        # DGW simulation
        dgw_config = SimulationConfig(n_simulations=10000)
        dgw_results = simulate_double_gameweek(
            player_ids, gw1_points, gw2_points, xi_probabilities, config=dgw_config
        )

        # DGW mean should be approximately double single GW mean
        assert dgw_results[0].mean_points > single_results[0].mean_points * 1.5

    def test_dgw_injury_filter(self) -> None:
        """Test that injured DGW players get 0 points."""
        player_ids = [1]
        gw1_points = np.array([5.0])
        gw2_points = np.array([5.0])
        xi_probabilities = np.array([0.9])
        statuses = ["i"]

        config = SimulationConfig(n_simulations=1000)
        results = simulate_double_gameweek(
            player_ids,
            gw1_points,
            gw2_points,
            xi_probabilities,
            statuses=statuses,
            config=config,
        )
        assert results[0].mean_points == pytest.approx(0.0)

    def test_dgw_reproducibility_with_seed(self) -> None:
        """Test that DGW results are reproducible with same seed."""
        player_ids = [1]
        gw1_points = np.array([5.0])
        gw2_points = np.array([5.0])
        xi_probabilities = np.array([0.9])

        config = SimulationConfig(n_simulations=1000, random_seed=42)
        results1 = simulate_double_gameweek(
            player_ids, gw1_points, gw2_points, xi_probabilities, config=config
        )
        results2 = simulate_double_gameweek(
            player_ids, gw1_points, gw2_points, xi_probabilities, config=config
        )

        assert results1[0].mean_points == pytest.approx(results2[0].mean_points)
