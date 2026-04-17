"""Feature engineering module.

Combines rolling features, H2H metrics, form index, fixture difficulty,
and context features into a unified feature set for model training.
"""

from __future__ import annotations

import logging

import polars as pl

from src.features.h2h_metrics import compute_player_vs_team, compute_team_h2h
from src.features.rolling_features import compute_rolling_features

logger = logging.getLogger(__name__)


def engineer_features(
    player_stats: pl.DataFrame,
    matches: pl.DataFrame,
    fixtures: pl.DataFrame,
) -> pl.DataFrame:
    """Engineer the full feature set for model training.

    Args:
        player_stats: Player gameweek statistics.
        matches: Match results for H2H computation.
        fixtures: Upcoming fixture schedule.

    Returns:
        DataFrame with all engineered features.
    """
    logger.info("Engineering features for %d player records", len(player_stats))

    # Rolling features
    df = compute_rolling_features(player_stats)

    # H2H features
    team_h2h = compute_team_h2h(matches)
    player_vs_team = compute_player_vs_team(player_stats)

    # Merge H2H features (only if columns exist)
    if "home_team_id" in df.columns and "away_team_id" in df.columns:
        df = df.join(team_h2h, on=["home_team_id", "away_team_id"], how="left")
    if "opponent_team_id" in df.columns:
        df = df.join(player_vs_team, on=["player_id", "opponent_team_id"], how="left")

    # TODO: Add form index, fixture difficulty, context features

    return df
