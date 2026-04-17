# Ticket: SPRINT9-004 - Data Visualization Components

## Description
Create reusable visualization components (src/streamlit_app/components/) for projections and analysis. Components used across all 5 Streamlit pages (Predictions, Team Selector, Match Preview, Transfer Suggestions, Captain Analysis).

## Technical Requirements
- Implement point distribution histogram component (Monte Carlo simulation results)
- Create form trend line chart component (last 3/5/10 GW rolling averages)
- Implement XI probability bar chart component (starting XI predictor output)
- Create feature importance chart component (LightGBM feature importance)
- Implement position distribution pie chart (team composition)
- Create team value distribution chart (budget allocation)
- Ensure components are documented and reusable
- Use Plotly for interactive charts
- Use consistent styling and color scheme (primaryColor: #1c39bb)
- Components must work with Supabase data format
- Ensure type hints (100% for public APIs)

## Acceptance Criteria
- [ ] Point distribution histogram component implemented (Plotly)
- [ ] Form trend line chart component implemented (Plotly, 3/5/10 GW options)
- [ ] XI probability bar chart component implemented (Plotly)
- [ ] Feature importance chart component implemented (Plotly, LightGBM output)
- [ ] Position distribution pie chart implemented (Plotly)
- [ ] Team value distribution chart implemented (Plotly)
- [ ] Components documented and reusable
- [ ] All components use type hints (100% for public APIs)
- [ ] Consistent styling applied (primaryColor: #1c39bb)
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
