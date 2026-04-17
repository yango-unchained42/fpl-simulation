# Ticket: BACKLOG-001 - Unify Event-Based and Point-Based Simulations

## Description
Combine the two simulation approaches (event_simulator.py and match_simulator.py) into a unified simulation engine that produces more accurate and consistent player point predictions by using ML to predict event rates and deriving FPL points from simulated events.

## Background

Currently we have two separate simulation approaches:

1. **Event-Based Simulation** (`src/models/event_simulator.py`)
   - Simulates individual events (shots, tackles, passes) per match
   - Uses Poisson/Bernoulli models based on **historical rates** (not ML)
   - More detailed and realistic but slower
   - Good for scenario analysis and edge cases

2. **Point-Based Simulation** (`src/models/match_simulator.py`)
   - Uses expected_points directly from ML model
   - Fast statistical approximation
   - Used for bulk ranking and team optimization
   - May not capture all edge cases accurately

## Problem

- Two simulations use different data sources (historical rates vs ML predictions)
- Not actually "competing" predictions - they're fundamentally different approaches
- Event simulation is potentially more accurate but uses wrong inputs (historical data not ML)

## Proposed Solution: Option A (Recommended)

**Retrain ML model to predict EVENT RATES instead of points, then use event simulation to derive FPL points.**

### Architecture Change:
```
┌─────────────────────────────────────────────────────────────────┐
│                     UNIFIED SIMULATION                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. ML Model (RETRAINED)                                       │
│     Input: rolling_features + h2h + form + context              │
│     Output: event_rates (xG, xA, shots, key_passes, tackles,    │
│             interceptions, clearances, saves)                  │
│                                                                  │
│  2. Event Simulator                                             │
│     Input: predicted event_rates                                 │
│     Process: Poisson/Bernoulli simulation of each event         │
│                                                                  │
│  3. FPL Scoring Engine                                          │
│     Input: simulated events                                      │
│     Process: Apply FPL rules to calculate points                │
│     Output: fpl_points                                          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### FPL Scoring Rules (Verified from `src/utils/fpl_scoring.py`):

**By Position:**

| Event | GKP | DEF | MID | FWD |
|-------|-----|-----|-----|-----|
| Goal | 6 | 6 | 5 | 4 |
| Assist | 3 | 3 | 3 | 3 |
| Clean Sheet (60+ min) | 4 | 4 | 1 | 0 |
| Goal Conceded (per 1) | -1 | -1 | 0 | 0 |
| Penalty Save | 5 | - | - | - |
| Penalty Miss | -2 | -2 | -2 | -2 |
| Yellow Card | -1 | -1 | -1 | -1 |
| Red Card | -3 | -3 | -3 | -3 |
| Save (per 3) | **1** | - | - | - |
| Minutes ≥60 | +2 | +2 | +2 | +2 |
| Minutes >0 & <60 | +1 | +1 | +1 | +1 |
| Bonus | 3/2/1 | 3/2/1 | 3/2/1 | 3/2/1 |

> **Note**: Saves are already included in the `target_events` list (GK specific).

### Benefits of This Approach:

1. **More interpretable** - We can see why a player is predicted to score X points
2. **More realistic** - Event simulation adds proper variance
3. **FPL rules embedded** - Points derived directly from game rules
4. **Scales to edge cases** - Can simulate what-ifs (e.g., "what if player gets 3 shots?")

### Technical Requirements

- [ ] Retrain LightGBM model to output event rates instead of points
- [ ] Create new target variables: xG, xA, shots, key_passes, tackles, etc.
- [ ] Modify event_simulator to accept predicted rates as input
- [ ] Ensure FPL scoring rules correctly applied
- [ ] Add calibration layer to tune event rates against actuals
- [ ] Performance optimization (event simulation is slower)
- [ ] Validation: compare to baseline (direct point prediction)

### Event Rates to Predict:

```python
# Target variables for ML model
target_events = [
    "expected_goals",        # xG - shots conversion
    "expected_assists",     # xA - key passes conversion  
    "shots",                # shot frequency
    "key_passes",           # chance creation
    "tackles",              # defensive actions
    "interceptions",        # defensive actions
    "clearances",           # defensive actions (for DEF)
    "saves",                # GK specific
    "minutes",              # playing time probability
]
```

## Acceptance Criteria

- [ ] ML model outputs event rates (xG, xA, shots, etc.) not just points
- [ ] Event simulation uses ML-predicted rates as input
- [ ] FPL points derived from events match official scoring rules
- [ ] Event-based predictions are more accurate than direct point prediction
- [ ] Calibration improves accuracy on validation set
- [ ] Performance acceptable for team optimization (sub 30s for full squad)

## Priority
Medium - Important for long-term accuracy, not critical for MVP

## Dependencies
- SPRINT-6 (Model Development) - completed
- SPRINT-8 (Simulation & Optimizer) - completed
- Need historical validation data in Supabase

## Agent
TBD

## Status
📋 Backlog

## Notes
- This is the recommended approach (Option A), NOT simple output combination
- Requires ML architecture changes, not just combining two predictions
- Should be pursued if direct point prediction shows poor performance
- Start with baseline implementation first, then evaluate if needed
- **Important**: FPL scoring rules have changed over years (e.g., bonus points system)
  - Need to track which rules applied to which season
  - May need historical scoring rules table for accurate model training