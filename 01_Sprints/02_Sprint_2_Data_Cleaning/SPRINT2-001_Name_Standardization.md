# Ticket: SPRINT2-001 - Name Standardization

## Description
Implement name standardization to ensure consistent player and team names across all data sources.

## Technical Requirements
- Create name standardization module in `fpl_simulation/src/cleaning/`
- Standardize all player names to "First Last" format (e.g., "Bukayo Saka")
- Handle name variations and formatting:
  - Convert "Last, First" to "First Last"
  - Handle middle names/initials
  - Standardize team name variations
  - Use Levenshtein distance for fuzzy matching
- Create mapping tables for known variations
- Implement confidence scoring for matches
- Log all standardization decisions to MLflow (local only)

## Acceptance Criteria
- [ ] Name standardization module implemented
- [ ] Fuzzy matching algorithm implemented
- [ ] Mapping tables created
- [ ] Confidence scoring implemented
- [ ] Standardization decisions logged
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

### 2026-04-02 — Implementation complete
Rewrote `src/utils/name_resolver.py` with fuzzy matching, Levenshtein distance, mapping tables, confidence scoring, MLflow logging. 33 tests, 95% coverage.

### 2026-04-02 — Review fixes applied
- Removed unused `type: ignore[no-any-return]` comment on line 98

### 2026-04-02 03:00:00 — Re-review (FAILED)

**Tests:** 33/33 in test_name_resolver.py ✓, but 2 failures in test_utils.py ✗
**Ruff:** All checks passed ✓
**MyPy:** Success, no issues found ✓

**Failures:**

1. **Breaking API change: `build_name_mapping()` return type changed — NOT FIXED**
   - Old API: returns `dict[str, str]` (source → target name)
   - New API: returns `dict[str, tuple[str, float]]` (source → (target, confidence))
   - `tests/test_utils.py::TestBuildNameMapping::test_exact_match` fails — expects `"Bukayo Saka"` but gets `("Bukayo Saka", 1.0)`
   - `tests/test_utils.py::TestBuildNameMapping::test_no_match_returns_empty` fails — expects empty dict but gets fuzzy match with low confidence `{'Unknown Player': ('Unknown Player', 0.21)}`
   - **Fix required:** Update `tests/test_utils.py::TestBuildNameMapping` to use the new return type (tuple with confidence score), or add backward-compatible wrapper

### 2026-04-02 03:00:00 — Re-review fixes applied
- Updated `tests/test_utils.py::TestBuildNameMapping` to use new return type `dict[str, tuple[str, float]]`:
  - `test_exact_match`: assertions now check tuple `(name, confidence)` instead of plain string
  - `test_case_insensitive`: added confidence score assertion
  - `test_no_match_returns_empty` → `test_no_match_returns_original`: now expects `(original_name, low_confidence)` instead of empty dict

### 2026-04-02 04:00:00 — Final Re-review
**Tests:** 200/200 passing ✓
**Ruff:** All checks passed ✓
**MyPy:** Success, no issues found ✓
**All acceptance criteria met** ✓

### 2026-04-02 04:00:00 Quality review passed. All checks green. Ticket closed.

## Review Failures

### 2026-04-02 01:00:00 — Quality Review
**1. MyPy unused type: ignore comment** — ✅ Fixed: removed `# type: ignore[no-any-return]` from line 98

### 2026-04-02 02:00:00 — Quality Review
**1. Breaking API change in `build_name_mapping()`** — ✅ Fixed: updated `tests/test_utils.py::TestBuildNameMapping` to use new return type `dict[str, tuple[str, float]]` with confidence scores

## Comments
[Agents can add questions, blockers, or notes here]
