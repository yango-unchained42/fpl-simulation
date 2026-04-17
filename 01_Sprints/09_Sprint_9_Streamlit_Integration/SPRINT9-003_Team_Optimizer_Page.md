# Ticket: SPRINT9-003 - Team Optimizer Page

## Description
Create team optimizer interface (2_⚽_Team_Selector.py) for building optimal FPL teams using PuLP ILP solver. Enforces 15-player squad constraint with injury filter as hard constraint (pre-solve filtering). Handles DGW (double gameweek) players with summed expected points.

## Technical Requirements
- Implement optimized team display (15-player squad: 2 GK, 3 DEF, 3 MID, 2 FWD + 4 subs)
- Create team budget tracker (current vs. 100.0m limit)
- Implement captaincy selector with recommendations (2x points for captain)
- Add alternative team suggestions (top 3-5 optimal solutions)
- Implement player swap functionality (see impact on total points)
- Add export team to CSV functionality
- Apply injury filter as hard constraint (pre-solve: exclude i=injured, s=suspended, u=unavailable players)
- Handle DGW (double gameweek) players: sum expected points from both fixtures
- Display player status (a=available, d=doubtful, i=injured, s=suspended, u=unavailable)
- Use streamlit-aggrid for interactive team table
- Use Plotly for budget distribution chart
- Ensure responsive design

## Acceptance Criteria
- [ ] Optimized team displays 15 players with correct positions (2 GK, 3 DEF, 3 MID, 2 FWD + 4 subs)
- [ ] Team budget tracker shows current vs. 100.0m limit
- [ ] Captaincy selector with recommendations working (2x points)
- [ ] Alternative team suggestions (top 3-5) implemented
- [ ] Player swap functionality shows impact on total points
- [ ] Export team to CSV functionality implemented
- [ ] Injury filter applied as hard constraint (pre-solve filtering)
- [ ] DGW handling implemented (summed expected points for double gameweek players)
- [ ] Player status badges displayed (a/d/i/s/u)
- [ ] Interactive table using streamlit-aggrid
- [ ] Budget distribution chart using Plotly
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
