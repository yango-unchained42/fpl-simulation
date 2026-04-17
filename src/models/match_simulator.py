"""Monte Carlo player simulation engine.

Simulates player point distributions using model predictions and
XI probability over configurable iterations. Handles injury filtering,
DGW (Double Gameweek) scenarios, and stores aggregated statistics.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import numpy as np
import numpy.typing as npt

logger = logging.getLogger(__name__)

# Default number of simulations
N_SIMULATIONS = 10_000


@dataclass
class PlayerSimulationResult:
    """Aggregated simulation results for a single player."""

    player_id: int
    mean_points: float
    median_points: float
    p10_points: float
    p25_points: float
    p75_points: float
    p90_points: float
    std_points: float
    start_probability: float
    n_sims: int
    raw_points: npt.NDArray[np.float64] = field(
        repr=False, default_factory=lambda: np.array([])
    )


@dataclass
class SimulationConfig:
    """Configuration for Monte Carlo simulation."""

    n_simulations: int = N_SIMULATIONS
    random_seed: int | None = 42
    injury_filter: bool = True
    xi_probability_threshold: float = 0.0  # No threshold, use probability weighting
    min_points_floor: float = 0.0  # Minimum points a player can score
    max_points_cap: float | None = None  # Maximum points cap (None = no cap)


def simulate_player_points(
    player_ids: list[int],
    expected_points: npt.NDArray[np.float64],
    xi_probabilities: npt.NDArray[np.float64],
    point_std: npt.NDArray[np.float64] | None = None,
    statuses: list[str] | None = None,
    config: SimulationConfig | None = None,
) -> list[PlayerSimulationResult]:
    """Run Monte Carlo simulation for player points.

    For each simulation:
    1. Check if player starts (based on XI probability)
    2. If starts, sample points from normal distribution
    3. If doesn't start, assign 0 or 1 point (sub appearance)

    Args:
        player_ids: List of player IDs.
        expected_points: Expected points per player (mean of distribution).
        xi_probabilities: Probability of starting (0-1) per player.
        point_std: Standard deviation of points per player. If None,
            uses sqrt(expected_points) as approximation.
        statuses: Optional player availability statuses ('a'=available).
        config: Simulation configuration.

    Returns:
        List of PlayerSimulationResult for each player.
    """
    cfg = config or SimulationConfig()
    n_players = len(player_ids)
    n_sims = cfg.n_simulations

    # Set random seed for reproducibility
    if cfg.random_seed is not None:
        np.random.seed(cfg.random_seed)

    # Default std: sqrt of expected points (Poisson-like variance)
    if point_std is None:
        point_std = np.sqrt(np.maximum(expected_points, 1.0))

    # Initialize results array
    all_points = np.zeros((n_players, n_sims), dtype=np.float64)

    for i in range(n_players):
        # Injury filter: unavailable players get 0 points
        if cfg.injury_filter and statuses and statuses[i] != "a":
            all_points[i, :] = 0.0
            continue

        # XI probability: binary sampling for each simulation
        starts = np.random.random(n_sims) < xi_probabilities[i]

        # Sample points for starters from normal distribution
        points = np.random.normal(expected_points[i], point_std[i], size=n_sims)

        # Apply floor and cap
        points = np.maximum(points, cfg.min_points_floor)
        if cfg.max_points_cap is not None:
            points = np.minimum(points, cfg.max_points_cap)

        # Non-starters get 0 points (or 1 point for sub appearance ~10% of time)
        sub_bonus = np.random.random(n_sims) < 0.1
        points[~starts] = np.where(sub_bonus[~starts], 1.0, 0.0)

        all_points[i, :] = points

    # Aggregate results
    results: list[PlayerSimulationResult] = []
    for i in range(n_players):
        pts = all_points[i, :]
        results.append(
            PlayerSimulationResult(
                player_id=player_ids[i],
                mean_points=float(np.mean(pts)),
                median_points=float(np.median(pts)),
                p10_points=float(np.percentile(pts, 10)),
                p25_points=float(np.percentile(pts, 25)),
                p75_points=float(np.percentile(pts, 75)),
                p90_points=float(np.percentile(pts, 90)),
                std_points=float(np.std(pts)),
                start_probability=float(np.mean(pts > 0)),
                n_sims=n_sims,
                raw_points=pts,
            )
        )

    logger.info("Simulated %d players x %d iterations", n_players, n_sims)

    return results


def simulate_double_gameweek(
    player_ids: list[int],
    gw1_expected_points: npt.NDArray[np.float64],
    gw2_expected_points: npt.NDArray[np.float64],
    xi_probabilities: npt.NDArray[np.float64],
    point_std: npt.NDArray[np.float64] | None = None,
    statuses: list[str] | None = None,
    config: SimulationConfig | None = None,
) -> list[PlayerSimulationResult]:
    """Run Monte Carlo simulation for Double Gameweek players.

    For DGW, players play 2 matches. Points are summed across both matches.

    Args:
        player_ids: List of player IDs.
        gw1_expected_points: Expected points for first match.
        gw2_expected_points: Expected points for second match.
        xi_probabilities: Probability of starting (applies to both matches).
        point_std: Standard deviation per player.
        statuses: Optional player availability statuses.
        config: Simulation configuration.

    Returns:
        List of PlayerSimulationResult with summed DGW points.
    """
    cfg = config or SimulationConfig()
    n_players = len(player_ids)
    n_sims = cfg.n_simulations

    if cfg.random_seed is not None:
        np.random.seed(cfg.random_seed)

    # Default std
    if point_std is None:
        point_std = np.sqrt(np.maximum(gw1_expected_points + gw2_expected_points, 1.0))

    all_points = np.zeros((n_players, n_sims), dtype=np.float64)

    for i in range(n_players):
        # Injury filter
        if cfg.injury_filter and statuses and statuses[i] != "a":
            all_points[i, :] = 0.0
            continue

        # XI probability for each match (independent)
        starts_gw1 = np.random.random(n_sims) < xi_probabilities[i]
        starts_gw2 = np.random.random(n_sims) < xi_probabilities[i]

        # Sample points for each match
        std_i = point_std[i] / np.sqrt(2)  # Split std across 2 matches
        points_gw1 = np.random.normal(gw1_expected_points[i], std_i, size=n_sims)
        points_gw2 = np.random.normal(gw2_expected_points[i], std_i, size=n_sims)

        # Apply floor
        points_gw1 = np.maximum(points_gw1, cfg.min_points_floor)
        points_gw2 = np.maximum(points_gw2, cfg.min_points_floor)

        # Non-starters get 0 or 1 point
        sub_bonus_1 = np.random.random(n_sims) < 0.1
        sub_bonus_2 = np.random.random(n_sims) < 0.1
        points_gw1[~starts_gw1] = np.where(sub_bonus_1[~starts_gw1], 1.0, 0.0)
        points_gw2[~starts_gw2] = np.where(sub_bonus_2[~starts_gw2], 1.0, 0.0)

        # Sum both matches
        all_points[i, :] = points_gw1 + points_gw2

    # Aggregate results
    results: list[PlayerSimulationResult] = []
    for i in range(n_players):
        pts = all_points[i, :]
        results.append(
            PlayerSimulationResult(
                player_id=player_ids[i],
                mean_points=float(np.mean(pts)),
                median_points=float(np.median(pts)),
                p10_points=float(np.percentile(pts, 10)),
                p25_points=float(np.percentile(pts, 25)),
                p75_points=float(np.percentile(pts, 75)),
                p90_points=float(np.percentile(pts, 90)),
                std_points=float(np.std(pts)),
                start_probability=float(np.mean(pts > 0)),
                n_sims=n_sims,
                raw_points=pts,
            )
        )

    logger.info("Simulated DGW for %d players x %d iterations", n_players, n_sims)

    return results
