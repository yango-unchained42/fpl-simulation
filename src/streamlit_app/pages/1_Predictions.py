"""Predictions page — display player expected points and probabilities."""

from __future__ import annotations

import polars as pl
import streamlit as st

from src.streamlit_app.loaders import filter_players, load_players, load_predictions

st.set_page_config(page_title="Predictions", page_icon="📊", layout="wide")
st.title("📊 Player Predictions")

# Sidebar filters
with st.sidebar:
    st.header("Filters")
    position_filter = st.selectbox(
        "Position", ["All", "GKP", "DEF", "MID", "FWD"], index=0
    )
    team_filter = st.selectbox("Team", ["All"] + list(range(1, 21)))
    min_price = st.slider("Min Price (£m)", 4.0, 15.0, 4.0, step=0.1)
    max_price = st.slider("Max Price (£m)", 4.0, 15.0, 15.0, step=0.1)

# Load data
players_df = load_players()
predictions_df = load_predictions(1)

# Gameweek selector
gameweek = st.slider("Gameweek", min_value=1, max_value=38, value=1)

# Reload predictions for selected gameweek
predictions_df = load_predictions(gameweek)

if players_df.is_empty():
    st.warning("No player data available. Please run the pipeline first.")
    st.info("Run `python main.py` to generate predictions.")
else:
    # Merge players with predictions
    if not predictions_df.is_empty():
        merged = players_df.join(
            predictions_df, left_on="id", right_on="player_id", how="left"
        )
    else:
        merged = players_df.with_columns(
            pl.lit(None).alias("expected_points"),
            pl.lit(None).alias("goal_prob"),
            pl.lit(None).alias("assist_prob"),
            pl.lit(None).alias("clean_sheet_prob"),
            pl.lit(None).alias("minutes_prob"),
        )

    # Apply filters
    filtered = filter_players(
        merged,
        position=position_filter,
        team=team_filter,
        min_price=min_price,
        max_price=max_price,
    )

    # Sort by expected points
    sorted_df = filtered.sort("expected_points", descending=True).head(50)

    # Display metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Players Shown", len(sorted_df))
    with col2:
        st.metric(
            "Avg Expected Points",
            (
                f"{sorted_df['expected_points'].mean():.2f}"
                if "expected_points" in sorted_df.columns
                else "N/A"
            ),
        )
    with col3:
        st.metric("Gameweek", gameweek)

    # Display player table
    st.subheader(f"Top Players - GW{gameweek}")

    if "expected_points" in sorted_df.columns:
        display_cols = [
            "web_name",
            "team",
            "position",
            "price",
            "form",
            "total_points",
            "expected_points",
            "goal_prob",
            "assist_prob",
        ]
    else:
        display_cols = ["web_name", "team", "position", "price", "form", "total_points"]

    available_cols = [c for c in display_cols if c in sorted_df.columns]
    st.dataframe(sorted_df.select(available_cols), use_container_width=True)

    # Top players highlight
    st.subheader("Top 5 Expected Points")
    if (
        "expected_points" in sorted_df.columns
        and not sorted_df["expected_points"].is_empty()
    ):
        top5 = sorted_df.head(5)
        cols = st.columns(5)
        for i, row in enumerate(top5.iter_rows(named=True)):
            with cols[i]:
                st.metric(
                    f"{row.get('web_name', 'N/A')}",
                    f"{row.get('expected_points', 0):.1f} pts",
                    f"£{row.get('price', 0)}m",
                )
    else:
        st.info("No prediction data available yet.")
