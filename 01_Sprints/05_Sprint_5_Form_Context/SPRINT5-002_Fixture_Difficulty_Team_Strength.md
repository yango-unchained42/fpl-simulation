# Ticket: SPRINT5-002 - Fixture Difficulty & Team Strength

## Description
Calculate fixture difficulty ratings and opponent team strength metrics.

## Technical Requirements
- Create fixture difficulty module in `fpl_simulation/src/features/`
- Calculate opponent team strength (based on recent performance)
- Compute fixture difficulty rating (home/away adjusted)
- Calculate strength of schedule metric for each player's team
- Calculate expected points against opponent
- Store features in Supabase
- Optimize for performance

## Acceptance Criteria
- [ ] Opponent team strength calculated (based on recent performance)
- [ ] Fixture difficulty rating computed (home/away adjusted)
- [ ] Strength of schedule metric for each player's team
- [ ] Expected points against opponent calculated
- [ ] Features stored in feature store
- [ ] Performance optimized
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

### 2026-04-04 — Implementation started
Picking up SPRINT5-002. Will implement fixture difficulty ratings, opponent team strength, and strength of schedule metrics.

### 2026-04-04 — Implementation complete
Created `src/features/fixture_difficulty.py` with:
- `compute_fixture_difficulty()` — 1-5 scale difficulty ratings based on opponent strength and home/away adjustment
- `compute_team_strength()` — dynamic strength ratings from match data (attack/defense/xG)
- `compute_strength_of_schedule()` — average difficulty of next N fixtures per team
- Caching, MLflow logging

Created `tests/test_form_fixture.py` with 13 tests covering fixture difficulty, team strength, and strength of schedule.

### 2026-04-04 12:00:00 — Review fixes applied
- Fixed unused `home_strength` variable by restructuring logic to properly use both `home_strength` and `away_strength` in difficulty calculations
- All 22 Sprint 5 tests passing, ruff clean, mypy clean

### 2026-04-04 13:00:00 — Final Re-review
**Tests:** 347/347 passing ✓
**Ruff:** All checks passed ✓
**MyPy:** Success, no issues found ✓
**All acceptance criteria met** ✓

### 2026-04-04 13:00:00 Quality review passed. All checks green. Ticket closed.

## Review Failures

### 2026-04-04 12:00:00 — Quality Review
**1. Unused variable `home_strength`** — ✅ Fixed: restructured logic to properly use both `home_strength` and `away_strength` in difficulty calculations

## Comments
[Agents can add questions, blockers, or notes here]
