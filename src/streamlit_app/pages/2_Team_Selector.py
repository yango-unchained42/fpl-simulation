"""Team Selector page — ILP-optimized squad display."""

from __future__ import annotations

import numpy as np
import polars as pl
import streamlit as st

from src.models.team_optimizer import optimize_squad
from src.streamlit_app.loaders import load_players, load_predictions

st.set_page_config(page_title="Team Selector", page_icon="⚽", layout="wide")
st.title("⚽ Team Selector")

st.markdown("Build your optimal 15-player squad using Integer Linear Programming.")

# Sidebar configuration
with st.sidebar:
    st.header("Optimizer Settings")
    budget = st.slider(
        "Budget (£m)", min_value=80.0, max_value=100.0, value=100.0, step=0.5
    )
    min_xi_prob = st.slider("Min XI Probability", 0.0, 1.0, 0.5, step=0.1)
    generate_alternatives = st.checkbox("Generate Alternative Squads", value=False)
    n_alternatives = 3 if generate_alternatives else 0

# Load data
players_df = load_players()
gameweek = st.slider("Select Gameweek", min_value=1, max_value=38, value=1)
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
        # Use historical total_points as fallback
        merged = players_df.with_columns(
            pl.col("total_points").alias("expected_points")
        )

    # Fill NaN expected_points with a baseline
    merged = merged.with_columns(
        pl.col("expected_points")
        .fill_null(pl.col("total_points") / 38)
        .alias("expected_points")
    )

    # Prepare data for optimizer
    player_ids = merged["id"].to_list()
    prices = merged["price"].to_numpy().astype(float)
    expected_points = merged["expected_points"].to_numpy().astype(float)
    positions = merged["position"].to_list()
    team_ids = merged["team"].to_list()
    web_names = merged["web_name"].to_list()

    # Run optimization button
    if st.button("🔍 Optimize Squad", type="primary"):
        with st.spinner("Running ILP optimization..."):
            try:
                result = optimize_squad(
                    player_ids=player_ids,
                    prices=prices,
                    expected_points=expected_points,
                    positions=positions,
                    team_ids=team_ids,
                    xi_probabilities=np.ones(len(player_ids)),  # Assume all available
                    min_xi_prob=min_xi_prob,
                    n_alternatives=n_alternatives,
                )

                # Display results
                st.success("Optimization complete!")

                # Main squad
                st.subheader("🏆 Optimized Squad")
                squad_df = pl.DataFrame(
                    {
                        "Player": [
                            web_names[player_ids.index(pid)] for pid in result["squad"]
                        ],
                        "Position": [
                            positions[player_ids.index(pid)] for pid in result["squad"]
                        ],
                        "Price": [
                            prices[player_ids.index(pid)] for pid in result["squad"]
                        ],
                        "Expected Points": [
                            expected_points[player_ids.index(pid)]
                            for pid in result["squad"]
                        ],
                    }
                )
                st.dataframe(squad_df, use_container_width=True)

                # Summary metrics
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Cost", f"£{result['total_cost']:.1f}m")
                with col2:
                    st.metric(
                        "Expected Points (GW)", f"{result['expected_points']:.1f}"
                    )
                with col3:
                    st.metric("Captain", web_names[player_ids.index(result["captain"])])

                # Show alternatives if requested
                if "alternatives" in result and result["alternatives"]:
                    st.subheader("🔄 Alternative Squads")
                    for i, alt in enumerate(result["alternatives"], 1):
                        with st.expander(f"Alternative {i}"):
                            alt_df = pl.DataFrame(
                                {
                                    "Player": [
                                        web_names[player_ids.index(pid)]
                                        for pid in alt["squad"]
                                    ],
                                    "Position": [
                                        positions[player_ids.index(pid)]
                                        for pid in alt["squad"]
                                    ],
                                    "Price": [
                                        prices[player_ids.index(pid)]
                                        for pid in alt["squad"]
                                    ],
                                    "Expected Points": [
                                        expected_points[player_ids.index(pid)]
                                        for pid in alt["squad"]
                                    ],
                                }
                            )
                            st.dataframe(alt_df, use_container_width=True)
                            st.metric(
                                "Expected Points", f"{alt['expected_points']:.1f}"
                            )

            except Exception as e:
                st.error(f"Optimization failed: {e}")
                st.info("Try adjusting the budget or minimum XI probability.")
    else:
        st.info("Click 'Optimize Squad' to generate your optimal team.")
