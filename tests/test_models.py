"""Tests for ML model modules."""

from __future__ import annotations

import numpy as np
import pytest

from src.models.match_simulator import (
    PlayerSimulationResult,
    SimulationConfig,
    simulate_player_points,
)
from src.models.player_predictor import predict_points, prepare_features
from src.models.team_optimizer import optimize_squad


class TestPlayerPredictor:
    """Tests for player performance prediction."""

    def test_prepare_features(self) -> None:
        import polars as pl

        df = pl.DataFrame(
            {
                "player_id": [1, 2, 3],
                "gameweek": [1, 2, 3],
                "fixture_id": [1, 2, 3],
                "name": ["Saka", "Salah", "Haaland"],
                "points": [5, 10, 15],
                "rolling_3": [5.0, 7.5, 10.0],
            }
        )
        X, y, feature_cols = prepare_features(df, target_col="points")
        assert len(y) == 3
        assert len(feature_cols) == 1
        assert "rolling_3" in feature_cols

    def test_predict_points_returns_array(self) -> None:
        import lightgbm as lgb

        X = np.array([[1.0], [2.0], [3.0]])
        y = np.array([5.0, 10.0, 15.0])
        model = lgb.LGBMRegressor(n_estimators=10, random_state=42)
        model.fit(X, y)
        preds = predict_points(model, X)
        assert isinstance(preds, np.ndarray)
        assert len(preds) == 3


class TestMatchSimulator:
    """Tests for Monte Carlo player simulation."""

    def test_simulate_player_points_returns_results(self) -> None:
        player_ids = [1, 2, 3]
        expected_points = np.array([5.0, 8.0, 3.0])
        xi_probs = np.array([0.9, 0.95, 0.7])

        config = SimulationConfig(n_simulations=1000, random_seed=42)
        results = simulate_player_points(
            player_ids, expected_points, xi_probs, config=config
        )
        assert len(results) == 3
        assert all(isinstance(r, PlayerSimulationResult) for r in results)

    def test_probabilities_affect_start_rate(self) -> None:
        player_ids = [1]
        expected_points = np.array([5.0])
        xi_probs = np.array([0.1])  # Low start probability

        config = SimulationConfig(n_simulations=10000, random_seed=42)
        results = simulate_player_points(
            player_ids, expected_points, xi_probs, config=config
        )
        # Start probability should be close to the input probability
        assert results[0].start_probability < 0.2

    def test_reproducibility_with_seed(self) -> None:
        player_ids = [1, 2]
        expected_points = np.array([5.0, 8.0])
        xi_probs = np.array([0.9, 0.95])

        config = SimulationConfig(n_simulations=1000, random_seed=42)
        results1 = simulate_player_points(
            player_ids, expected_points, xi_probs, config=config
        )
        results2 = simulate_player_points(
            player_ids, expected_points, xi_probs, config=config
        )
        assert results1[0].mean_points == pytest.approx(results2[0].mean_points)

    def test_injury_filter(self) -> None:
        player_ids = [1, 2]
        expected_points = np.array([5.0, 8.0])
        xi_probs = np.array([0.9, 0.95])
        statuses = ["i", "a"]  # Player 1 injured

        config = SimulationConfig(n_simulations=1000, random_seed=42)
        results = simulate_player_points(
            player_ids, expected_points, xi_probs, statuses=statuses, config=config
        )
        assert results[0].mean_points == 0.0
        assert results[1].mean_points > 0


class TestTeamOptimizer:
    """Tests for ILP team optimization."""

    def test_optimize_squad_returns_valid_selection(self) -> None:
        player_ids = list(range(1, 51))
        prices = np.array([float(i % 10 + 4) for i in range(50)])
        expected_points = np.random.default_rng(42).random(50) * 10
        positions = (
            ["GK"] * 4
            + ["DEF"] * 15
            + ["MID"] * 15
            + ["FWD"] * 8
            + ["GK"] * 2
            + ["DEF"] * 5
        )
        positions = positions[:50]
        team_ids = [i % 20 + 1 for i in range(50)]
        statuses = ["a"] * 50

        result = optimize_squad(
            player_ids=player_ids,
            prices=prices,
            expected_points=expected_points,
            positions=positions,
            team_ids=team_ids,
            statuses=statuses,
        )
        assert "squad" in result
        assert "captain" in result
        assert len(result["squad"]) == 15
        assert len(result["captain"]) == 1

    def test_injured_players_excluded(self) -> None:
        player_ids = list(range(1, 51))
        prices = np.array([5.0] * 50)
        expected_points = np.array([float(i) for i in range(50)])
        positions = (
            ["GK"] * 4
            + ["DEF"] * 15
            + ["MID"] * 15
            + ["FWD"] * 8
            + ["GK"] * 2
            + ["DEF"] * 5
        )
        positions = positions[:50]
        team_ids = [i % 20 + 1 for i in range(50)]
        statuses = ["i"] * 10 + ["a"] * 40  # First 10 injured

        result = optimize_squad(
            player_ids=player_ids,
            prices=prices,
            expected_points=expected_points,
            positions=positions,
            team_ids=team_ids,
            statuses=statuses,
        )
        # Injured players (IDs 1-10) should not be in squad
        for pid in range(1, 11):
            assert pid not in result["squad"]
