"""FPL scoring rules engine.

Calculates FPL points from raw match events (goals, assists, clean sheets,
saves, cards, etc.) with position-specific scoring rules and BPS/bonus
point calculation.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Position constants
POS_GK = "GK"
POS_DEF = "DEF"
POS_MID = "MID"
POS_FWD = "FWD"

# Clean sheet points by position
CLEAN_SHEET_POINTS = {POS_GK: 4, POS_DEF: 4, POS_MID: 1, POS_FWD: 0}

# Goals scored points by position
GOAL_POINTS = {POS_GK: 6, POS_DEF: 6, POS_MID: 5, POS_FWD: 4}

# Assist points (same for all positions)
ASSIST_POINTS = 3

# Save points (GK only, 1 pt per 3 saves)
SAVE_POINTS_PER_3 = 1

# Card points
YELLOW_CARD_POINTS = -1
RED_CARD_POINTS = -3

# Penalty save/save points
PENALTY_SAVE_POINTS = 5
PENALTY_MISS_POINTS = -2

# Bonus points (top 3 BPS scorers get 3, 2, 1 bonus points)
BONUS_POINTS = [3, 2, 1]


def calculate_bps(
    goals: int = 0,
    assists: int = 0,
    clean_sheet: bool = False,
    goals_conceded: int = 0,
    saves: int = 0,
    bonus: int = 0,
    tackles: int = 0,
    interceptions: int = 0,
    clearances: int = 0,
    key_passes: int = 0,
    yellow_cards: int = 0,
    red_cards: int = 0,
    penalty_save: bool = False,
    penalty_miss: bool = False,
) -> int:
    """Calculate Bonus Points System (BPS) score.

    Simplified BPS calculation based on key events.

    Args:
        goals: Goals scored.
        assists: Assists.
        clean_sheet: Clean sheet achieved.
        goals_conceded: Goals conceded.
        saves: Saves made.
        bonus: Bonus points awarded (from official data).
        tackles: Tackles made.
        interceptions: Interceptions.
        clearances: Clearances.
        key_passes: Key passes.
        yellow_cards: Yellow cards.
        red_cards: Red cards.
        penalty_save: Penalty saved.
        penalty_miss: Penalty missed.

    Returns:
        BPS score.
    """
    bps = 0
    bps += goals * 24
    bps += assists * 12
    bps += clean_sheet * 12
    bps -= goals_conceded * 2
    bps += saves // 3
    bps += tackles * 2
    bps += interceptions * 4
    bps += clearances * 2
    bps += key_passes * 3
    bps += yellow_cards * -3
    bps += red_cards * -9
    bps += penalty_save * 15
    bps += penalty_miss * -6

    return max(0, bps)


def calculate_fpl_points(
    position: str,
    minutes: int = 0,
    goals: int = 0,
    assists: int = 0,
    clean_sheet: bool = False,
    goals_conceded: int = 0,
    saves: int = 0,
    yellow_cards: int = 0,
    red_cards: int = 0,
    penalty_save: bool = False,
    penalty_miss: bool = False,
    bonus: int = 0,
) -> int:
    """Calculate FPL points from raw match events.

    Args:
        position: Player position (GK/DEF/MID/FWD).
        minutes: Minutes played.
        goals: Goals scored.
        assists: Assists.
        clean_sheet: Clean sheet achieved.
        goals_conceded: Goals conceded.
        saves: Saves made.
        yellow_cards: Yellow cards.
        red_cards: Red cards.
        penalty_save: Penalty saved.
        penalty_miss: Penalty missed.
        bonus: Bonus points awarded.

    Returns:
        Total FPL points.
    """
    points = 0

    # Minutes played
    if minutes >= 60:
        points += 2
    elif minutes > 0:
        points += 1

    # Goals
    points += goals * GOAL_POINTS.get(position, 4)

    # Assists
    points += assists * ASSIST_POINTS

    # Clean sheet
    if clean_sheet and minutes >= 60:
        points += CLEAN_SHEET_POINTS.get(position, 0)

    # Goals conceded (GK/DEF only)
    if position in (POS_GK, POS_DEF):
        points -= goals_conceded

    # Saves (GK only)
    if position == POS_GK:
        points += (saves // 3) * SAVE_POINTS_PER_3

    # Cards
    points += yellow_cards * YELLOW_CARD_POINTS
    points += red_cards * RED_CARD_POINTS

    # Penalty events
    if penalty_save:
        points += PENALTY_SAVE_POINTS
    if penalty_miss:
        points += PENALTY_MISS_POINTS

    # Bonus points
    points += bonus

    return max(0, points)


def simulate_bonus_points(
    player_bps_scores: dict[int, int],
) -> dict[int, int]:
    """Simulate bonus point allocation based on BPS scores.

    Args:
        player_bps_scores: Dict mapping player_id to BPS score.

    Returns:
        Dict mapping player_id to bonus points (0-3).
    """
    # Sort by BPS descending
    sorted_players = sorted(player_bps_scores.items(), key=lambda x: x[1], reverse=True)

    bonus_allocation: dict[int, int] = {pid: 0 for pid in player_bps_scores}

    # Award bonus points to top 3
    for i, (pid, _) in enumerate(sorted_players[:3]):
        bonus_allocation[pid] = BONUS_POINTS[i]

    return bonus_allocation
