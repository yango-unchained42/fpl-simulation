# Ticket: SPRINT9-008 - Shot Map Visualization

## Description
Implement interactive shot map visualizations for players and teams using Understat shot location data. This allows users to see where shots are taken, their quality (xG), and outcomes.

## Technical Requirements
- **Data Preparation:**
  - Load `shots.parquet` from Understat ingestion.
  - Standardize coordinates: Mirror away team shots so all shots are viewed from the attacking direction (standard football analytics view).
  - Handle coordinate systems (Understat uses 0-100 normalized pitch).
- **Visualization Components:**
  - Create a reusable Streamlit component for plotting shot maps.
  - Use Plotly or Altair for interactive plots (hover for xG, outcome, minute).
  - Color code shots by xG value or outcome (Goal, Saved, Missed, Blocked).
  - Size shots by xG magnitude.
- **Filters:**
  - Player selector (show all shots for a specific player).
  - Team selector (show all shots for a team).
  - Season/Gameweek filters.
  - xG threshold filter (e.g., show only shots > 0.1 xG).
- **Performance:**
  - Cache processed shot data to avoid re-mirroring on every load.
  - Use data sampling for large datasets if needed (e.g., >10k shots).

## Acceptance Criteria
- [ ] Shot map component implemented in Streamlit
- [ ] Coordinates standardized (away shots mirrored)
- [ ] Interactive filters (Player, Team, Season, xG threshold)
- [ ] Color/Size encoding for xG and outcome
- [ ] Hover tooltips with shot details
- [ ] Performance optimized for large datasets

## Definition of Done
- [ ] Code implemented and follows project conventions
- [ ] Unit tests for coordinate transformation logic
- [ ] Integration tests for Streamlit component
- [ ] Code reviewed by reviewer
- [ ] Documentation updated

## Agent
build

## Status
📋 Backlog

## Progress Log

## Review Failures
[None yet]

## Comments
- **Note:** Depends on Understat shot data ingestion (SPRINT1-002/SPRINT2-006).
- **Data:** `data/raw/understat/*/shots.parquet` contains `x`, `y`, `xg`, `outcome`, `body_part`.
