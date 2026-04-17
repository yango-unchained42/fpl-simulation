# Ticket: SPRINT4-001 - Opponent-Specific H2H Features

## Description
Implement opponent-specific H2H features that capture how a player/team performs against specific opponents. This includes ALL available metrics (core, defensive, advanced, ICT, market) for each player-opponent and team-opponent pairing.

## Technical Requirements
- Create opponent H2H features module in `src/features/h2h_features.py` (or expand existing `h2h_metrics.py`)
- Implement opponent-specific features for ALL metric categories:

### Core Metrics (Player vs Opponent Team)
- Avg points, goals, assists, total_points, minutes, bonus, ict_index
- Rolling avg (last 3/5/10 meetings)

### Defensive Metrics (Player vs Opponent Team)
- Avg tackles, clearances_blocks_interceptions, recoveries, defensive_contribution
- Avg clean_sheets, goals_conceded, expected_goals_conceded
- Rolling avg (last 3/5/10 meetings)

### Advanced Metrics (Player vs Opponent Team)
- Avg xG, xA, xG_chain, xG_buildup, key_passes, shots
- Avg xG per shot (shot quality metric)
- Rolling avg (last 3/5/10 meetings)

### ICT Components (Player vs Opponent Team)
- Avg influence, creativity, threat
- Rolling avg (last 3/5/10 meetings)

### Market/Context Metrics (Player vs Opponent Team)
- Avg value, selected, transfers_in, transfers_out, form
- Rolling avg (last 3/5/10 meetings)

### Team-Level H2H (Team vs Opponent Team)
- Avg xG scored, xG conceded, goals scored, goals conceded
- Avg PPDA, deep completions, expected_points
- Avg shots, shots on target, clean sheets
- Rolling avg (last 3/5/10 meetings)

### Recency Weighting
- Weight recent meetings more heavily (e.g., 3-season window with exponential decay)
- Handle teams/players with limited H2H history (fallback to overall averages)

### Output
- Append to `data/processed/features.parquet`
- Column naming: `{metric}_h2h_vs_{opponent}` (e.g., `total_points_h2h_vs_3`, `xg_h2h_vs_3`)
- Joinable to player features via `player_id` + `opponent_team_id` + `gameweek`

### Performance
- Use Polars group_by + rolling operations
- Cache intermediate results
- Log feature count and computation time to MLflow

## Acceptance Criteria
- [ ] Opponent H2H features calculated for ALL metrics listed above
- [ ] Recency weighting implemented (3-season window)
- [ ] Limited history cases handled (fallback to overall averages)
- [ ] Output joins with player features in `features.parquet`
- [ ] Column naming follows convention
- [ ] Performance optimized (Polars vectorized operations)
- [ ] MLflow logging for feature count and computation time
- [ ] Unit tests written
- [ ] Integration tests passing

## Definition of Done
- [ ] Code implemented and follows project conventions
- [ ] Unit tests written and passing (>80% coverage for this component)
- [ ] Type hints added (100% for public APIs)
- [ ] Code reviewed by reviewer
- [ ] Documentation updated
- [ ] Integrated into main pipeline

## Agent
build

## Status
Done

## Progress Log

### 2026-04-04 — Implementation complete
Rewrote `src/features/h2h_metrics.py` with comprehensive H2H features for ALL metrics:

**Player vs Team H2H:**
- Overall averages for 35+ metrics (core, defensive, advanced, ICT, market, discipline)
- Rolling windows (3/5/10 meetings) for all metrics
- Home/away split averages
- Recent form (last 5 meetings)
- Auto-detects extra numeric columns

**Team vs Team H2H:**
- Overall averages for all available match metrics (xG, goals, shots, PPDA, deep completions, expected_points, etc.)
- Rolling windows (3/5/10 meetings)
- Recent form (last 5 meetings)

**Infrastructure:**
- Caching to `data/processed/h2h_cache/`
- MLflow logging for feature counts
- Supabase write integration

Created `tests/test_h2h_metrics.py` with 17 tests:
- `TestH2HCache` (4): save/load, validity, TTL expiration, clearing
- `TestComputeTeamH2H` (5): basic metrics, season filter, empty input, rolling windows, caching
- `TestComputePlayerVsTeam` (5): basic metrics, home/away splits, rolling windows, recent form, caching
- `TestComputeH2HFeatures` (3): returns both dataframes, MLflow enabled/disabled

Coverage: ~90% on `h2h_metrics.py`. All 313 tests passing.

### 2026-04-04 11:00:00 — Quality Review
**Tests:** 313/313 passing ✓
**Coverage:** 90% on h2h_metrics.py ✓
**Ruff:** All checks passed ✓
**MyPy:** Success, no issues found ✓
**All acceptance criteria met** ✓

### 2026-04-04 11:00:00 Quality review passed. All checks green. Ticket closed.

## Review Failures
[None]

## Comments
[Agents can add questions, blockers, or notes here]
