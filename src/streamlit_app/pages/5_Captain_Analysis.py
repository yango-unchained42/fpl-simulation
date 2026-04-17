"""Captain Analysis page — captain leaderboard, EP ceiling/floor."""

from __future__ import annotations

import streamlit as st

st.set_page_config(page_title="Captain Analysis", page_icon="🎯", layout="wide")
st.title("🎯 Captain Analysis")

st.info("Captain recommendations will appear here once the pipeline has run.")

# TODO: Load captain analysis from Supabase
# @st.cache_data(ttl=3600)
# def load_captain_analysis(gameweek: int) -> pl.DataFrame:
#     ...

st.write("Select a gameweek to analyze captain options.")
gameweek = st.slider("Gameweek", min_value=1, max_value=38, value=1)
st.write(f"No captain data available for GW {gameweek} yet.")
