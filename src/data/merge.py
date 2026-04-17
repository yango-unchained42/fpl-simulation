"""Data merging module.

Merges data from multiple sources (FPL API, vaastav, Understat)
using name standardization and player ID crosswalks.
"""

from __future__ import annotations

import logging

import polars as pl

logger = logging.getLogger(__name__)


def merge_player_data(
    fpl_players: pl.DataFrame,
    vaastav_stats: pl.DataFrame,
    understat_stats: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Merge player data from all sources.

    Args:
        fpl_players: Player data from FPL API.
        vaastav_stats: Historical stats from vaastav.
        understat_stats: Optional Understat data.

    Returns:
        Merged Polars DataFrame.
    """
    logger.info(
        "Merging player data from %d sources",
        2 + (understat_stats is not None),
    )

    df = fpl_players.join(
        vaastav_stats,
        on="player_id",
        how="left",
    )

    if understat_stats is not None:
        df = df.join(understat_stats, on="player_id", how="left")

    return df


def merge_fixture_data(
    fixtures: pl.DataFrame,
    team_h2h: pl.DataFrame,
) -> pl.DataFrame:
    """Merge fixture data with H2H metrics.

    Args:
        fixtures: Fixture schedule DataFrame.
        team_h2h: Team head-to-head metrics DataFrame.

    Returns:
        Merged DataFrame with fixture and H2H data.
    """
    df = fixtures.join(
        team_h2h,
        left_on=["home_team_id", "away_team_id"],
        right_on=["home_team_id", "away_team_id"],
        how="left",
    )
    return df
