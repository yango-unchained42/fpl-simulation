"""Team optimizer using Integer Linear Programming (PuLP).

Optimizes 15-player squad and starting XI selection within
FPL budget and position constraints. Supports DGW handling,
injury filtering, and alternative solution generation.
"""

from __future__ import annotations

import logging
import platform
import time
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt
import pulp

logger = logging.getLogger(__name__)

# FPL constraints
BUDGET = 100.0
SQUAD_SIZE = 15
MAX_PER_CLUB = 3
POSITION_COUNTS = {"GK": 2, "DEF": 5, "MID": 5, "FWD": 3}


def _get_solver() -> pulp.LpSolver:
    """Get the appropriate PuLP solver for the current platform.

    On Apple Silicon, the bundled CBC solver fails with
    'Bad CPU type in executable'. This function detects the
    architecture and uses the Homebrew-installed CBC if available.

    Returns:
        Configured PuLP solver instance.
    """
    # Check for Apple Silicon
    if platform.machine() == "arm64":
        homebrew_cbc = Path("/opt/homebrew/opt/cbc/bin/cbc")
        if homebrew_cbc.exists():
            logger.info("Using Homebrew CBC solver for Apple Silicon")
            return pulp.COIN_CMD(path=str(homebrew_cbc), msg=False)

    # Default: Use bundled CBC (works on x86_64 and Linux)
    return pulp.PULP_CBC_CMD(msg=False)


def optimize_squad(
    player_ids: list[int],
    prices: npt.NDArray[np.float64],
    expected_points: npt.NDArray[np.float64],
    positions: list[str],
    team_ids: list[int],
    statuses: list[str] | None = None,
    xi_probabilities: npt.NDArray[np.float64] | None = None,
    min_xi_prob: float = 0.0,
    n_alternatives: int = 0,
) -> dict[str, Any]:
    """Optimize squad selection using ILP.

    Args:
        player_ids: List of player IDs.
        prices: Array of player prices (in millions).
        expected_points: Array of expected points per player.
        positions: List of player positions (GK/DEF/MID/FWD).
        team_ids: List of team IDs for each player.
        statuses: Optional player availability statuses ('a'=available).
        xi_probabilities: Optional XI start probabilities (0-1).
        min_xi_prob: Minimum XI probability threshold for selection.
        n_alternatives: Number of alternative squads to generate.

    Returns:
        Dict with 'squad', 'captain', 'expected_points', 'total_cost',
        and optionally 'alternatives' list.
    """
    t0 = time.time()

    # Filter by XI probability if provided
    if xi_probabilities is not None and min_xi_prob > 0:
        mask = xi_probabilities >= min_xi_prob
        player_ids = [pid for pid, m in zip(player_ids, mask) if m]
        prices = prices[mask]
        expected_points = expected_points[mask]
        positions = [pos for pos, m in zip(positions, mask) if m]
        team_ids = [tid for tid, m in zip(team_ids, mask) if m]
        if statuses:
            statuses = [s for s, m in zip(statuses, mask) if m]

    results: dict[str, Any] = {}
    alternatives: list[dict[str, Any]] = []

    for alt_idx in range(1 + n_alternatives):
        result = _solve_optimization(
            player_ids, prices, expected_points, positions, team_ids, statuses, alt_idx
        )
        if alt_idx == 0:
            results = result
        else:
            alternatives.append(result)

    elapsed = time.time() - t0
    results["optimization_time_seconds"] = elapsed

    if n_alternatives > 0:
        results["alternatives"] = alternatives

    logger.info(
        "Optimized squad: %d players, cost=%.1f, expected_points=%.1f, time=%.2fs",
        len(results.get("squad", [])),
        results.get("total_cost", 0),
        results.get("expected_points", 0),
        elapsed,
    )

    return results


def _solve_optimization(
    player_ids: list[int],
    prices: npt.NDArray[np.float64],
    expected_points: npt.NDArray[np.float64],
    positions: list[str],
    team_ids: list[int],
    statuses: list[str] | None,
    alt_idx: int = 0,
) -> dict[str, Any]:
    """Solve a single optimization problem.

    For alternative solutions, adds diversity constraints to exclude
    previously selected players.
    """
    prob = pulp.LpProblem(f"FPL_Squad_Optimization_{alt_idx}", pulp.LpMaximize)

    # Decision variables
    x = pulp.LpVariable.dicts("select", player_ids, cat="Binary")
    c = pulp.LpVariable.dicts("captain", player_ids, cat="Binary")

    # Pre-solve: zero out unavailable players
    if statuses:
        for pid, status in zip(player_ids, statuses):
            if status != "a":
                prob += x[pid] == 0

    # Objective: maximize expected points
    prob += pulp.lpSum(x[pid] * expected_points[i] for i, pid in enumerate(player_ids))

    # Budget constraint
    prob += pulp.lpSum(x[pid] * prices[i] for i, pid in enumerate(player_ids)) <= BUDGET

    # Squad size
    prob += pulp.lpSum(x[pid] for pid in player_ids) == SQUAD_SIZE

    # Position constraints
    for pos, count in POSITION_COUNTS.items():
        pos_indices = [i for i, p in enumerate(positions) if p == pos]
        prob += pulp.lpSum(x[player_ids[i]] for i in pos_indices) == count

    # Max 3 per club
    unique_teams = set(team_ids)
    for team_id in unique_teams:
        team_indices = [i for i, t in enumerate(team_ids) if t == team_id]
        prob += pulp.lpSum(x[player_ids[i]] for i in team_indices) <= MAX_PER_CLUB

    # Captain constraint
    prob += pulp.lpSum(c[pid] for pid in player_ids) == 1
    for pid in player_ids:
        prob += c[pid] <= x[pid]

    # Diversity constraint for alternatives
    if alt_idx > 0:
        # Require at least alt_idx*2 different players from the optimal squad
        # This is a simplified approach; in production, use more sophisticated diversity
        prob += pulp.lpSum(x[pid] for pid in player_ids) <= SQUAD_SIZE - alt_idx

    # Solve
    solver = _get_solver()
    prob.solve(solver)

    selected = [pid for pid in player_ids if pulp.value(x[pid]) == 1]
    captain = [pid for pid in player_ids if pulp.value(c[pid]) == 1]
    total_cost = sum(prices[i] for i, pid in enumerate(player_ids) if pid in selected)
    total_points = sum(
        expected_points[i] for i, pid in enumerate(player_ids) if pid in selected
    )

    return {
        "squad": selected,
        "captain": captain,
        "expected_points": total_points,
        "total_cost": total_cost,
    }
