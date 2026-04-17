# Ticket: SPRINT9-002 - Player Projections Page

## Description
Create player projections page (1_📊_Predictions.py) with detailed statistics and visualizations. Displays player projections from Supabase with filtering, sorting, and export capabilities.

## Technical Requirements
- Implement player list with key metrics (expected points, XI probability, price, selected_by_percent)
- Create player detail view with full projection distribution (Monte Carlo simulation results)
- Implement form trend visualization (last 3/5/10 gameweeks rolling averages)
- Add position filter (GK, DEF, MID, FWD) and sorting options
- Implement search functionality (player name, team)
- Add export projections to CSV functionality
- Display player status (a=available, d=doubtful, i=injured, s=suspended, u=unavailable)
- Use streamlit-aggrid for interactive tables
- Use Plotly for interactive charts
- Ensure responsive design

## Acceptance Criteria
- [ ] Player list displays key metrics (expected points, XI probability, price, selected_by_percent)
- [ ] Player detail view shows full projection distribution (Monte Carlo results)
- [ ] Form trend visualization (last 3/5/10 GW rolling averages) implemented with Plotly
- [ ] Position filter (GK, DEF, MID, FWD) working correctly
- [ ] Sorting options implemented (by points, probability, price, selected_by_percent)
- [ ] Search functionality working (player name, team)
- [ ] Export projections to CSV functionality implemented
- [ ] Player status badges displayed (a/d/i/s/u)
- [ ] Interactive table using streamlit-aggrid
- [ ] Responsive design implemented
- [ ] Documentation updated

## Definition of Done
- [ ] Code implemented and follows project conventions (fpl_simulation/ structure)
- [ ] Unit tests written and passing (>80% coverage for this component)
- [ ] Type hints added (100% for public APIs)
- [ ] Code reviewed by reviewer
- [ ] Documentation updated
- [ ] Integrated into main application (Streamlit Pages API)

## Agent
Da Vinci (Streamlit)

## Status
📋 Backlog

## Progress Log

## Comments
[Agents can add questions, blockers, or notes here]
