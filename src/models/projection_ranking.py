"""Projection and ranking system.

Generates player projections and rankings based on simulation results,
XI probability, ownership data, and identifies captaincy picks and
differential picks.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import numpy as np
import numpy.typing as npt
import polars as pl

logger = logging.getLogger(__name__)


@dataclass
class PlayerProjection:
    """Projection for a single player."""

    player_id: int
    expected_points: float
    xi_probability: float
    combined_score: float
    ownership_pct: float
    is_differential: bool
    is_captain_pick: bool
    rank_overall: int
    rank_by_position: int
    position: str


def generate_projections(
    player_ids: list[int],
    simulation_results: list[dict[str, float]],
    xi_probabilities: npt.NDArray[np.float64],
    ownership_pcts: npt.NDArray[np.float64],
    positions: list[str],
    differential_threshold: float = 10.0,
    n_captain_picks: int = 3,
) -> list[PlayerProjection]:
    """Generate player projections and rankings.

    Args:
        player_ids: List of player IDs.
        simulation_results: List of dicts with 'mean_points' per player.
        xi_probabilities: Probability of starting (0-1) per player.
        ownership_pcts: Ownership percentage per player.
        positions: List of player positions (GK/DEF/MID/FWD).
        differential_threshold: Max ownership % to be considered differential.
        n_captain_picks: Number of top captaincy picks to identify.

    Returns:
        List of PlayerProjection objects sorted by combined_score.
    """
    n_players = len(player_ids)
    projections: list[PlayerProjection] = []

    for i in range(n_players):
        mean_pts = simulation_results[i].get("mean_points", 0.0)
        xi_prob = float(xi_probabilities[i]) if xi_probabilities is not None else 1.0
        ownership = float(ownership_pcts[i]) if ownership_pcts is not None else 0.0

        # Combined score: expected_points * xi_probability
        combined = mean_pts * xi_prob

        is_differential = ownership < differential_threshold

        projections.append(
            PlayerProjection(
                player_id=player_ids[i],
                expected_points=mean_pts,
                xi_probability=xi_prob,
                combined_score=combined,
                ownership_pct=ownership,
                is_differential=is_differential,
                is_captain_pick=False,  # Will be set after sorting
                rank_overall=0,
                rank_by_position=0,
                position=positions[i],
            )
        )

    # Sort by combined score (descending)
    projections.sort(key=lambda p: p.combined_score, reverse=True)

    # Assign overall ranks
    for rank, proj in enumerate(projections, 1):
        proj.rank_overall = rank

    # Assign position ranks
    for pos in set(positions):
        pos_projs = [p for p in projections if p.position == pos]
        for rank, proj in enumerate(pos_projs, 1):
            proj.rank_by_position = rank

    # Identify captaincy picks (top N by combined score)
    for proj in projections[:n_captain_picks]:
        proj.is_captain_pick = True

    logger.info(
        "Generated projections for %d players, %d captaincy picks, %d differentials",
        len(projections),
        n_captain_picks,
        sum(1 for p in projections if p.is_differential),
    )

    return projections


def get_captaincy_recommendations(
    projections: list[PlayerProjection],
    top_n: int = 3,
) -> list[dict[str, Any]]:
    """Generate captaincy recommendations with reasoning.

    Args:
        projections: List of player projections.
        top_n: Number of recommendations to return.

    Returns:
        List of dicts with player_id, expected_points, xi_probability,
        ownership_pct, and reasoning.
    """
    # Filter to players with high XI probability
    viable = [p for p in projections if p.xi_probability >= 0.5]
    viable.sort(key=lambda p: p.combined_score, reverse=True)

    recommendations: list[dict[str, Any]] = []
    for proj in viable[:top_n]:
        reasoning = _generate_captaincy_reasoning(proj)
        recommendations.append(
            {
                "player_id": proj.player_id,
                "expected_points": proj.expected_points,
                "xi_probability": proj.xi_probability,
                "ownership_pct": proj.ownership_pct,
                "combined_score": proj.combined_score,
                "reasoning": reasoning,
            }
        )

    return recommendations


def get_differential_picks(
    projections: list[PlayerProjection],
    top_n: int = 5,
) -> list[dict[str, Any]]:
    """Identify differential picks (low ownership, high potential).

    Args:
        projections: List of player projections.
        top_n: Number of differential picks to return.

    Returns:
        List of dicts with player_id, expected_points, ownership_pct,
        and combined_score.
    """
    differentials = [p for p in projections if p.is_differential]
    differentials.sort(key=lambda p: p.combined_score, reverse=True)

    return [
        {
            "player_id": p.player_id,
            "expected_points": p.expected_points,
            "xi_probability": p.xi_probability,
            "ownership_pct": p.ownership_pct,
            "combined_score": p.combined_score,
            "position": p.position,
        }
        for p in differentials[:top_n]
    ]


def projections_to_dataframe(
    projections: list[PlayerProjection],
) -> pl.DataFrame:
    """Convert projections to a Polars DataFrame.

    Args:
        projections: List of PlayerProjection objects.

    Returns:
        Polars DataFrame with all projection data.
    """
    return pl.DataFrame(
        {
            "player_id": [p.player_id for p in projections],
            "expected_points": [p.expected_points for p in projections],
            "xi_probability": [p.xi_probability for p in projections],
            "combined_score": [p.combined_score for p in projections],
            "ownership_pct": [p.ownership_pct for p in projections],
            "is_differential": [p.is_differential for p in projections],
            "is_captain_pick": [p.is_captain_pick for p in projections],
            "rank_overall": [p.rank_overall for p in projections],
            "rank_by_position": [p.rank_by_position for p in projections],
            "position": [p.position for p in projections],
        }
    )


def _generate_captaincy_reasoning(proj: PlayerProjection) -> str:
    """Generate reasoning for captaincy recommendation.

    Args:
        proj: Player projection.

    Returns:
        Human-readable reasoning string.
    """
    parts = []
    parts.append(f"Expected {proj.expected_points:.1f} points")
    parts.append(f"{proj.xi_probability:.0%} chance to start")

    if proj.ownership_pct < 10:
        parts.append(f"Differential pick ({proj.ownership_pct:.1f}% ownership)")
    elif proj.ownership_pct > 50:
        parts.append(f"Popular pick ({proj.ownership_pct:.1f}% ownership)")

    if proj.rank_by_position == 1:
        parts.append(f"Top-ranked {proj.position}")

    return ". ".join(parts) + "."
