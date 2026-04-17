"""Unified data merger.

Combines FPL, Vaastav, and Understat data sources into a single
feature-ready dataset using player ID crosswalks.
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

UNIFIED_DIR = Path("data/processed")
UNIFIED_FILE = UNIFIED_DIR / "unified_player_gw.parquet"


def create_unified_player_gw(
    fpl_history: pl.DataFrame,
    vaastav_gw: pl.DataFrame,
    understat_pms: pl.DataFrame,
    fpl_players: pl.DataFrame,
    crosswalk: pl.DataFrame,
    use_cache: bool = True,
) -> pl.DataFrame:
    """Create a unified player-gameweek feature table.

    Merges FPL player history, Vaastav GW data, and Understat player match
    stats into a single table keyed by (player_id, gameweek).

    Args:
        fpl_history: FPL player history (GW-by-GW match data).
        vaastav_gw: Vaastav gameweek data (uses FPL element IDs).
        understat_pms: Understat player match stats.
        fpl_players: FPL player info (id, web_name, team, position, etc.).
        crosswalk: Understat→FPL player ID mapping.
        use_cache: Whether to use cached unified table if available.

    Returns:
        Unified DataFrame with all metrics per player per gameweek.
    """
    if use_cache and UNIFIED_FILE.exists():
        logger.info("Using cached unified player GW table")
        return pl.read_parquet(UNIFIED_FILE)

    # Standardize column names for merging
    # FPL history uses 'element' for player_id, 'round' for gameweek
    fpl_clean = _standardize_fpl_history(fpl_history)

    # Vaastav uses 'element' for player_id, 'GW' for gameweek
    vaastav_clean = _standardize_vaastav_gw(vaastav_gw)

    # Understat uses 'player_id' (Understat's ID), needs crosswalk
    understat_clean = _standardize_understat_pms(understat_pms, crosswalk)

    # Merge: start with Vaastav (most complete historical data)
    unified = vaastav_clean.join(
        fpl_clean,
        on=["player_id", "gameweek"],
        how="outer",
        suffix="_fpl",
    )

    # Merge Understat data
    if not understat_clean.is_empty():
        unified = unified.join(
            understat_clean,
            on=["player_id", "gameweek"],
            how="outer",
            suffix="_us",
        )

    # Add FPL player info
    player_info = fpl_players.select(
        ["id", "web_name", "team", "element_type", "now_cost", "status"]
    ).rename({"id": "player_id"})

    unified = unified.join(player_info, on="player_id", how="left")

    # Save cache
    UNIFIED_DIR.mkdir(parents=True, exist_ok=True)
    unified.write_parquet(UNIFIED_FILE)
    logger.info(
        "Saved unified player GW table to %s (%d rows, %d cols)",
        UNIFIED_FILE,
        unified.shape[0],
        unified.shape[1],
    )

    return unified


def _standardize_fpl_history(fpl_history: pl.DataFrame) -> pl.DataFrame:
    """Standardize FPL player history column names."""
    rename_map = {}
    if "element" in fpl_history.columns:
        rename_map["element"] = "player_id"
    if "round" in fpl_history.columns:
        rename_map["round"] = "gameweek"
    if "fixture" in fpl_history.columns:
        rename_map["fixture"] = "fixture_id"
    if "opponent_team" in fpl_history.columns:
        rename_map["opponent_team"] = "opponent_team_id"

    if rename_map:
        fpl_history = fpl_history.rename(rename_map)

    # Ensure player_id is integer
    if "player_id" in fpl_history.columns:
        fpl_history = fpl_history.with_columns(pl.col("player_id").cast(pl.Int64))

    return fpl_history


def _standardize_vaastav_gw(vaastav_gw: pl.DataFrame) -> pl.DataFrame:
    """Standardize Vaastav GW column names."""
    rename_map = {}
    if "element" in vaastav_gw.columns:
        rename_map["element"] = "player_id"
    if "GW" in vaastav_gw.columns:
        rename_map["GW"] = "gameweek"
    if "opponent_team" in vaastav_gw.columns:
        rename_map["opponent_team"] = "opponent_team_id"

    if rename_map:
        vaastav_gw = vaastav_gw.rename(rename_map)

    if "player_id" in vaastav_gw.columns:
        vaastav_gw = vaastav_gw.with_columns(pl.col("player_id").cast(pl.Int64))

    return vaastav_gw


def _standardize_understat_pms(
    understat_pms: pl.DataFrame,
    crosswalk: pl.DataFrame,
) -> pl.DataFrame:
    """Standardize Understat player match stats and map to FPL IDs."""
    if understat_pms.is_empty() or crosswalk.is_empty():
        return pl.DataFrame()

    # Map Understat player_id to FPL player_id via crosswalk
    mapped = understat_pms.join(
        crosswalk.select(["understat_player_id", "fpl_player_id"]),
        left_on="player_id",
        right_on="understat_player_id",
        how="left",
    )

    # Keep only matched players
    mapped = mapped.filter(pl.col("fpl_player_id").is_not_null())

    # Rename to standard names
    rename_map = {}
    if "game_id" in mapped.columns:
        rename_map["game_id"] = "fixture_id"

    if rename_map:
        mapped = mapped.rename(rename_map)

    # Use FPL player_id as the primary key
    mapped = mapped.with_columns(
        pl.col("fpl_player_id").cast(pl.Int64).alias("player_id")
    )

    # Understat doesn't have gameweek directly — we'll need to infer it
    # from the fixture mapping. For now, set to null as Int64.
    if "gameweek" not in mapped.columns:
        mapped = mapped.with_columns(pl.lit(None, dtype=pl.Int64).alias("gameweek"))

    return mapped
