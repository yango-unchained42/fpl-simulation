"""Player-level event simulator.

Simulates individual player actions (shots, passes, tackles) per match
using Poisson/Bernoulli models based on historical performance and
Understat shot location data.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import numpy as np

from src.utils.fpl_scoring import (
    calculate_bps,
    calculate_fpl_points,
    simulate_bonus_points,
)

logger = logging.getLogger(__name__)


@dataclass
class PlayerEvent:
    """Simulated event for a player in a match."""

    player_id: int
    minutes: int = 0
    goals: int = 0
    assists: int = 0
    shots: int = 0
    shots_on_target: int = 0
    key_passes: int = 0
    tackles: int = 0
    interceptions: int = 0
    clearances: int = 0
    saves: int = 0
    yellow_cards: int = 0
    red_cards: int = 0
    penalty_save: bool = False
    penalty_miss: bool = False
    clean_sheet: bool = False
    goals_conceded: int = 0
    bonus: int = 0
    fpl_points: int = 0
    bps: int = 0


@dataclass
class PlayerRates:
    """Historical rates for player event simulation."""

    player_id: int
    position: str
    shots_per_90: float = 0.0
    goals_per_shot: float = 0.0
    assists_per_90: float = 0.0
    key_passes_per_90: float = 0.0
    tackles_per_90: float = 0.0
    interceptions_per_90: float = 0.0
    clearances_per_90: float = 0.0
    saves_per_90: float = 0.0
    yellow_cards_per_90: float = 0.0
    red_cards_per_90: float = 0.0
    penalty_taker: bool = False
    set_piece_taker: bool = False
    start_probability: float = 1.0
    avg_minutes: float = 90.0


def simulate_player_events(
    player_rates: list[PlayerRates],
    opponent_goals_conceded: float = 1.5,
    team_goals_scored: float = 1.5,
    random_seed: int | None = None,
) -> list[PlayerEvent]:
    """Simulate player-level events for a single match.

    Uses Poisson/Bernoulli models based on historical rates.

    Args:
        player_rates: List of PlayerRates for each player.
        opponent_goals_conceded: Expected goals conceded by opponent.
        team_goals_scored: Expected goals scored by player's team.
        random_seed: Random seed for reproducibility.

    Returns:
        List of PlayerEvent objects with simulated events and FPL points.
    """
    if random_seed is not None:
        np.random.seed(random_seed)

    events: list[PlayerEvent] = []

    for rates in player_rates:
        event = PlayerEvent(player_id=rates.player_id)

        # Determine if player starts
        starts = np.random.random() < rates.start_probability
        if not starts:
            # Sub appearance: 10-30 minutes
            event.minutes = int(np.random.uniform(10, 30))
        else:
            # Starter: 60-90 minutes
            event.minutes = int(np.random.uniform(60, 90))

        minutes_factor = event.minutes / 90.0

        # Shots (Poisson)
        expected_shots = rates.shots_per_90 * minutes_factor
        event.shots = int(np.random.poisson(expected_shots))

        # Goals (Bernoulli per shot)
        if event.shots > 0 and rates.goals_per_shot > 0:
            event.goals = int(np.random.binomial(event.shots, rates.goals_per_shot))

        # Assists (Poisson)
        expected_assists = rates.assists_per_90 * minutes_factor
        event.assists = int(np.random.poisson(expected_assists))

        # Key passes (Poisson)
        expected_key_passes = rates.key_passes_per_90 * minutes_factor
        event.key_passes = int(np.random.poisson(expected_key_passes))

        # Defensive actions (for DEF/MID)
        if rates.position in ("DEF", "MID"):
            expected_tackles = rates.tackles_per_90 * minutes_factor
            event.tackles = int(np.random.poisson(expected_tackles))

            expected_interceptions = rates.interceptions_per_90 * minutes_factor
            event.interceptions = int(np.random.poisson(expected_interceptions))

            expected_clearances = rates.clearances_per_90 * minutes_factor
            event.clearances = int(np.random.poisson(expected_clearances))

        # GK saves
        if rates.position == "GK":
            expected_saves = rates.saves_per_90 * minutes_factor
            event.saves = int(np.random.poisson(expected_saves))

        # Cards
        expected_yellow = rates.yellow_cards_per_90 * minutes_factor
        event.yellow_cards = int(np.random.poisson(expected_yellow))

        if np.random.random() < rates.red_cards_per_90 * minutes_factor:
            event.red_cards = 1

        # Clean sheet (based on opponent expected goals)
        if rates.position in ("GK", "DEF") and event.minutes >= 60:
            clean_sheet_prob = np.exp(-opponent_goals_conceded)
            event.clean_sheet = bool(np.random.random() < clean_sheet_prob)
            if not event.clean_sheet:
                event.goals_conceded = max(
                    1, int(np.random.poisson(opponent_goals_conceded))
                )

        # Calculate BPS and bonus
        event.bps = calculate_bps(
            goals=event.goals,
            assists=event.assists,
            clean_sheet=event.clean_sheet,
            goals_conceded=event.goals_conceded,
            saves=event.saves,
            tackles=event.tackles,
            interceptions=event.interceptions,
            clearances=event.clearances,
            key_passes=event.key_passes,
            yellow_cards=event.yellow_cards,
            red_cards=event.red_cards,
            penalty_save=event.penalty_save,
            penalty_miss=event.penalty_miss,
        )

        # Calculate FPL points (bonus will be set after all players)
        event.fpl_points = calculate_fpl_points(
            position=rates.position,
            minutes=event.minutes,
            goals=event.goals,
            assists=event.assists,
            clean_sheet=event.clean_sheet,
            goals_conceded=event.goals_conceded,
            saves=event.saves,
            yellow_cards=event.yellow_cards,
            red_cards=event.red_cards,
            penalty_save=event.penalty_save,
            penalty_miss=event.penalty_miss,
        )

        events.append(event)

    # Simulate bonus points across all players
    bps_scores = {e.player_id: e.bps for e in events}
    bonus_allocation = simulate_bonus_points(bps_scores)
    for event in events:
        event.bonus = bonus_allocation.get(event.player_id, 0)
        event.fpl_points += event.bonus

    return events


def simulate_what_if_scenario(
    player_rates: list[PlayerRates],
    modifications: dict[int, dict[str, Any]],
    opponent_goals_conceded: float = 1.5,
    team_goals_scored: float = 1.5,
    random_seed: int | None = None,
) -> list[PlayerEvent]:
    """Simulate a "what-if" scenario with modified player rates.

    Args:
        player_rates: Base player rates.
        modifications: Dict mapping player_id to dict of rate modifications.
            e.g., {1: {"penalty_taker": True, "shots_per_90": 4.0}}
        opponent_goals_conceded: Expected goals conceded by opponent.
        team_goals_scored: Expected goals scored by player's team.
        random_seed: Random seed for reproducibility.

    Returns:
        List of PlayerEvent objects with simulated events.
    """
    # Apply modifications
    modified_rates = []
    for rates in player_rates:
        if rates.player_id in modifications:
            mods = modifications[rates.player_id]
            # Create modified copy
            rates_dict: dict[str, Any] = {
                "player_id": rates.player_id,
                "position": rates.position,
                "shots_per_90": rates.shots_per_90,
                "goals_per_shot": rates.goals_per_shot,
                "assists_per_90": rates.assists_per_90,
                "key_passes_per_90": rates.key_passes_per_90,
                "tackles_per_90": rates.tackles_per_90,
                "interceptions_per_90": rates.interceptions_per_90,
                "clearances_per_90": rates.clearances_per_90,
                "saves_per_90": rates.saves_per_90,
                "yellow_cards_per_90": rates.yellow_cards_per_90,
                "red_cards_per_90": rates.red_cards_per_90,
                "penalty_taker": rates.penalty_taker,
                "set_piece_taker": rates.set_piece_taker,
                "start_probability": rates.start_probability,
                "avg_minutes": rates.avg_minutes,
            }
            rates_dict.update(mods)
            modified_rates.append(PlayerRates(**rates_dict))
        else:
            modified_rates.append(rates)

    return simulate_player_events(
        modified_rates,
        opponent_goals_conceded,
        team_goals_scored,
        random_seed,
    )
