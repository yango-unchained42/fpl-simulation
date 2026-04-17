# Sprint 5: Form & Context Features

## Overview

**Sprint Goal:** Create form-based features and contextual variables that capture recent performance trends and match-specific conditions.

**Duration:** 1 week

**Focus Areas:**
- 7-day and 14-day form metrics (rolling windows)
- Fixture difficulty adjustments
- Team strength context
- Rest days and fatigue indicators
- Injuries and suspensions impact (hard constraint filter)

---

## Tickets

### SPRINT5-001: Form Metrics Calculation
**Status:** Awaiting Development  
**Priority:** High  
**Labels:** `feature` `form` `metrics`

**Description:**
Calculate rolling form metrics for players and teams over 7-day and 14-day windows.

**Definition of Done:**
- [ ] 7-day form feature implemented (last 3-5 gameweeks) in `fpl_simulation/src/features/`
- [ ] 14-day form feature implemented (last 6-8 gameweeks) in `fpl_simulation/src/features/`
- [ ] Form weighted by recency (more recent games weighted higher)
- [ ] Form calculated for both players (points, xG, xA, etc.) and teams (goals scored/conceded)
- [ ] Features stored in Supabase with proper metadata
- [ ] Handles edge cases (new players, players returning from injury)
- [ ] Documentation updated

**Assignee:** @Developer  
**Status:** Awaiting Development  
**Comments:**

---

### SPRINT5-002: Fixture Difficulty & Team Strength
**Status:** Awaiting Development  
**Priority:** Medium  
**Labels:** `feature` `context` `difficulty`

**Description:**
Calculate fixture difficulty ratings and opponent team strength metrics.

**Definition of Done:**
- [ ] Opponent team strength calculated (based on recent performance) in `fpl_simulation/src/features/`
- [ ] Fixture difficulty rating computed (home/away adjusted)
- [ ] Strength of schedule metric for each player's team
- [ ] Expected points against opponent calculated
- [ ] Features stored in Supabase
- [ ] Documentation updated

**Assignee:** @Developer  
**Status:** Awaiting Development  
**Comments:**

---

### SPRINT5-003: Contextual Features (Rest, Fatigue, Injuries)
**Status:** Awaiting Development  
**Priority:** Medium  
**Labels:** `feature` `context` `injuries`

**Description:**
Add contextual features for rest days, team fatigue, and injury/suspension impact.

**Definition of Done:**
- [ ] Days since last match calculated in `fpl_simulation/src/features/`
- [ ] Days until next match calculated
- [ ] Team fatigue metric (based on matches in short period)
- [ ] Key player absence impact (when star players injured/suspended)
- [ ] International break impact flag
- [ ] Cup match fatigue indicator
- [ ] Features stored in Supabase
- [ ] Documentation updated

**Assignee:** @Developer  
**Status:** Awaiting Development  
**Comments:**

---

### SPRINT5-004: Form & Context Testing
**Status:** Awaiting Development  
**Priority:** Medium  
**Labels:** `testing` `form` `context`

**Description:**
Comprehensive testing for all form and context features.

**Definition of Done:**
- [ ] Unit tests for form calculations (edge cases: new players, returns) in `fpl_simulation/tests/`
- [ ] Unit tests for fixture difficulty calculations
- [ ] Unit tests for contextual features
- [ ] Integration test for complete feature pipeline
- [ ] Data validation tests for feature distributions
- [ ] Test coverage >80%
- [ ] All tests passing

**Assignee:** @TestSpecialist  
**Status:** Awaiting Development  
**Comments:**

---

### SPRINT5-005: Form & Context Review
**Status:** Awaiting Review  
**Priority:** Medium  
**Labels:** `review` `form` `context`

**Description:**
Project Owner review of form and context features implementation.

**Definition of Done:**
- [ ] Code review completed (fpl_simulation/ conventions)
- [ ] Feature quality validated
- [ ] Documentation reviewed
- [ ] Test coverage verified (>80%)
- [ ] Performance benchmarks met
- [ ] Approved for merge

**Assignee:** @ProjectOwner  
**Status:** Awaiting Review  
**Comments:**

---

## Sprint Summary

**Total Tickets:** 5  
**Development Tickets:** 3  
**Testing Tickets:** 1  
**Review Tickets:** 1

**Expected Outcomes:**
- Complete form metrics system (7-day and 14-day)
- Fixture difficulty and team strength calculations
- Contextual features for match conditions
- All features properly tested and documented
