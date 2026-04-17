"""Transfer Suggestions page — ranked transfer recommendations."""

from __future__ import annotations

import streamlit as st

st.set_page_config(page_title="Transfer Suggestions", page_icon="🔄", layout="wide")
st.title("🔄 Transfer Suggestions")

st.info("Transfer recommendations will appear here once the pipeline has run.")

# TODO: Load current squad and transfer suggestions
# @st.cache_data(ttl=3600)
# def load_transfer_suggestions() -> pl.DataFrame:
#     ...

transfers = st.radio("Free transfers remaining", [0, 1, 2])
st.write(f"{transfers} free transfer(s) — no recommendations available yet.")
