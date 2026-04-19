"""FPL Simulation Streamlit Application.

Main entry point for the Streamlit dashboard using native multipage.
"""

from __future__ import annotations

import streamlit as st

# Configure page settings
st.set_page_config(
    page_title="FPL Simulation",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded",
)

# App title and description
st.title("⚽ FPL Simulation Dashboard")
st.markdown(
    """
Welcome to the FPL Simulation Dashboard. Use the sidebar to navigate between pages:

- **Predictions**: Player expected points and probabilities
- **Team Selector**: Build your optimal 15-player squad
- **Match Preview**: Head-to-head analysis
- **Transfer Suggestions**: Smart transfer recommendations
- **Captain Analysis**: Captain picks analysis
"""
)

# Page navigation using native Streamlit
# Each page file in pages/ directory will appear in the sidebar automatically
st.markdown("---")
st.info("Navigate to other pages using the sidebar on the left.")
