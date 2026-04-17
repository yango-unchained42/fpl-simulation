"""Streamlit data loaders with caching for Supabase data."""

from __future__ import annotations

import polars as pl
import streamlit as st


@st.cache_data(ttl=3600)
def load_players() -> pl.DataFrame:
    """Load player data from Supabase.

    Returns:
        Polars DataFrame with player information.
    """
    from src.data.database import get_supabase_client, read_from_supabase

    client = get_supabase_client()
    if client is None:
        st.warning("Supabase client not available")
        return _get_fallback_players()

    try:
        df = read_from_supabase(
            table="players",
            columns=[
                "id",
                "web_name",
                "team",
                "position",
                "price",
                "form",
                "total_points",
            ],
            client=client,
        )
        return df
    except Exception as e:
        st.error(f"Error loading players: {e}")
        return _get_fallback_players()


@st.cache_data(ttl=3600)
def load_predictions(gameweek: int) -> pl.DataFrame:
    """Load predictions for a specific gameweek.

    Args:
        gameweek: Gameweek number.

    Returns:
        Polars DataFrame with player predictions.
    """
    from src.data.database import get_supabase_client, read_from_supabase

    client = get_supabase_client()
    if client is None:
        st.warning("Supabase client not available")
        return _get_fallback_predictions(gameweek)

    try:
        df = read_from_supabase(
            table="player_predictions",
            columns=[
                "player_id",
                "gameweek",
                "expected_points",
                "goal_prob",
                "assist_prob",
                "clean_sheet_prob",
                "minutes_prob",
            ],
            filters=[("gameweek", "eq", gameweek)],
            client=client,
        )
        return df
    except Exception as e:
        st.error(f"Error loading predictions: {e}")
        return _get_fallback_predictions(gameweek)


@st.cache_data(ttl=3600)
def load_fixtures(gameweek: int | None = None) -> pl.DataFrame:
    """Load fixture data from Supabase.

    Args:
        gameweek: Optional gameweek filter.

    Returns:
        Polars DataFrame with fixtures.
    """
    from src.data.database import get_supabase_client, read_from_supabase

    client = get_supabase_client()
    if client is None:
        st.warning("Supabase client not available")
        return _get_fallback_fixtures()

    try:
        filters = [("gameweek", "eq", gameweek)] if gameweek else None
        df = read_from_supabase(
            table="fixtures",
            columns=["id", "gameweek", "home_team", "away_team", "kickoff_time"],
            filters=filters,
            client=client,
        )
        return df
    except Exception as e:
        st.error(f"Error loading fixtures: {e}")
        return _get_fallback_fixtures()


@st.cache_data(ttl=3600)
def load_teams() -> pl.DataFrame:
    """Load team data from Supabase.

    Returns:
        Polars DataFrame with team information.
    """
    from src.data.database import get_supabase_client, read_from_supabase

    client = get_supabase_client()
    if client is None:
        st.warning("Supabase client not available")
        return _get_fallback_teams()

    try:
        df = read_from_supabase(
            table="teams",
            columns=["id", "name", "strength", "attack_strength", "defence_strength"],
            client=client,
        )
        return df
    except Exception as e:
        st.error(f"Error loading teams: {e}")
        return _get_fallback_teams()


def _get_fallback_players() -> pl.DataFrame:
    """Return fallback player data for when Supabase is unavailable."""
    return pl.DataFrame(
        {
            "id": [],
            "web_name": [],
            "team": [],
            "position": [],
            "price": [],
            "form": [],
            "total_points": [],
        }
    )


def _get_fallback_predictions(gameweek: int) -> pl.DataFrame:
    """Return fallback predictions for when Supabase is unavailable."""
    return pl.DataFrame(
        {
            "player_id": [],
            "gameweek": [],
            "expected_points": [],
            "goal_prob": [],
            "assist_prob": [],
            "clean_sheet_prob": [],
            "minutes_prob": [],
        }
    )


def _get_fallback_fixtures() -> pl.DataFrame:
    """Return fallback fixtures for when Supabase is unavailable."""
    return pl.DataFrame(
        {
            "id": [],
            "gameweek": [],
            "home_team": [],
            "away_team": [],
            "kickoff_time": [],
        }
    )


def _get_fallback_teams() -> pl.DataFrame:
    """Return fallback teams for when Supabase is unavailable."""
    return pl.DataFrame(
        {
            "id": [],
            "name": [],
            "strength": [],
            "attack_strength": [],
            "defence_strength": [],
        }
    )


def filter_players(
    df: pl.DataFrame,
    position: str | None = None,
    team: str | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
) -> pl.DataFrame:
    """Filter players by position, team, and price range.

    Args:
        df: Input DataFrame.
        position: Position filter.
        team: Team filter.
        min_price: Minimum price.
        max_price: Maximum price.

    Returns:
        Filtered DataFrame.
    """
    result = df

    if position is not None and position != "All":
        result = result.filter(pl.col("position") == position)

    if team is not None and team != "All":
        result = result.filter(pl.col("team") == team)

    if min_price is not None:
        result = result.filter(pl.col("price") >= min_price)

    if max_price is not None:
        result = result.filter(pl.col("price") <= max_price)

    return result
