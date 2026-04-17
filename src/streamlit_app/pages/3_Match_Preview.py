"""Match Preview page — score distribution, win/draw/loss gauges, H2H stats."""

from __future__ import annotations

import streamlit as st

st.set_page_config(page_title="Match Preview", page_icon="📈", layout="wide")
st.title("📈 Match Preview")

st.info("Match simulations will appear here once the pipeline has run.")

# TODO: Load match simulations from Supabase
# @st.cache_data(ttl=3600)
# def load_match_simulations(gameweek: int) -> pl.DataFrame:
#     ...

st.write("Select a fixture to preview.")
st.write("No simulation data available yet.")
