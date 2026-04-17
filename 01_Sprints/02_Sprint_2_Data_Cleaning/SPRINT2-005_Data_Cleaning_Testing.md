# Ticket: SPRINT2-005 - Data Cleaning Testing

## Description
Create comprehensive tests for all data cleaning components to ensure data quality and reliability.

## Technical Requirements
- Create unit tests for each cleaning module in `fpl_simulation/tests/cleaning/`
- Create integration tests for the full cleaning pipeline
- Add data quality tests:
  - Name format validation ("First Last" format)
  - Name matching accuracy tests
  - Imputation quality tests
  - Validation rule tests
  - H2H calculation accuracy tests (Option C)
- Implement test fixtures with known data
- Achieve >80% test coverage
- Test polars-based operations
- Test Supabase integration

## Acceptance Criteria
- [ ] Unit tests for all cleaning modules
- [ ] Integration tests for pipeline
- [ ] Data quality tests implemented
- [ ] Test fixtures with known data created
- [ ] >80% test coverage achieved
- [ ] All tests passing

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

### 2026-04-02 — Implementation complete
Created `tests/test_cleaning_integration.py` with 12 integration tests covering name format validation, imputation quality, validation rules, H2H accuracy, and full pipeline. Total Sprint 2: 118 tests (33+20+26+15+12+12 Supabase).

### 2026-04-02 — Review fixes applied
- Updated H2H integration tests to use `write_db=False` to avoid Supabase dependency in tests
- Added `TestSupabaseIntegration` class with 4 tests: insert success, upsert, connection error handling, no client handling
- Fixed `test_features.py` column name assertions to match new H2H API

### 2026-04-02 04:00:00 — Final Re-review
**Tests:** 16/16 passing in test_cleaning_integration.py ✓
**Total project tests:** 200/200 passing ✓
**Coverage:** 85%+ across Sprint 2 modules ✓
**Ruff:** All checks passed ✓
**MyPy:** Success, no issues found ✓
**All acceptance criteria met** ✓

### 2026-04-02 04:00:00 Quality review passed. All checks green. Ticket closed.

## Review Failures

### 2026-04-02 01:00:00 — Quality Review
**1. Integration test depends on broken H2H API** — ✅ Fixed: updated `test_features.py` to use new column names (`avg_goals_scored_home`, `total_goals`, etc.)
**2. Missing Supabase integration tests** — ✅ Fixed: added `TestSupabaseIntegration` class with 4 tests covering insert, upsert, connection error, and no-client scenarios

## Comments
[Agents can add questions, blockers, or notes here]
