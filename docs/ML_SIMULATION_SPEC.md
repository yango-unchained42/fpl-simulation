# FPL + Understat ML & Monte Carlo System Specification

## 1. Purpose

Predict **Fantasy Premier League points under uncertainty** and optimize squad decisions (transfers, captaincy, benching, chips) using FPL and Understat data. Combines **machine learning, Monte Carlo simulation, and integer linear programming** into a single pipeline.

## 2. Design Principles

1. Predict rewarded outcomes (FPL points), not raw football events
2. Separate expectation modeling (ML) from uncertainty modeling (simulation)
3. Simulate at **team level first**, then derive player outcomes
4. Use rolling, role-aware metrics instead of season averages
5. Model availability before performance
6. Every output must support a concrete decision
7. Temporal validation only — never use future data in training

---

## 3. Data Sources

### 3.1 Input Tables

**player_gw_raw** (silver_unified_player_stats — per player per fixture)

| Column | Source | Notes |
|--------|--------|-------|
| player_id | unified_player_id | UUID key |
| team_id | unified_team_id | UUID key |
| opponent_id | unified_team_id | UUID key |
| fixture_id | match_id | UUID key |
| gameweek | gameweek | 1-38 |
| home_away | was_home | boolean |
| minutes | FPL | 0-90+ |
| started | FPL (starts > 0) | boolean |
| goals | FPL | |
| assists | FPL | |
| xG | Understat | preferred over FPL xG |
| xA | Understat | preferred over FPL xA |
| np_xG | Understat | xG minus penalties |
| shots | Understat | |
| shots_in_box | Understat | if available |
| key_passes | Understat | |
| tackles | FPL | 2025/26 defensive stats |
| interceptions | FPL | 2025/26 defensive stats |
| blocks | FPL | 2025/26 defensive stats |
| clearances | FPL | 2025/26 defensive stats |
| recoveries | FPL | 2025/26 |
| defensive_contribution | FPL | 2025/26 aggregate |
| yellow_cards | FPL | |
| red_cards | FPL | |
| bps | FPL | Bonus Points System |
| clean_sheets | FPL | |
| goals_conceded | FPL | |
| saves | FPL | GK only |
| penalties_saved | FPL | GK only |
| penalties_missed | FPL | |
| own_goals | FPL | |
| kickoff_time | FPL | for rest days calc |

**team_gw_raw** (silver_understat_match_stats + silver_fixtures — per team per fixture)

| Column | Source |
|--------|--------|
| team_id | unified_team_id |
| opponent_id | unified_team_id |
| fixture_id | match_id |
| gameweek | gameweek |
| home_away | boolean |
| goals_for | match result |
| xG_for | Understat |
| xG_against | Understat |
| shots_for | Understat |
| shots_against | Understat |

### 3.2 External Context

| Factor | Source | Usage |
|--------|--------|-------|
| Fixture difficulty | FPL team strength ratings | Opponent adjustment |
| Rest days | kickoff_time differences | Fatigue factor |
| Injury status | FPL chance_of_playing_next_round | Availability gate |
| Price | FPL now_cost | Budget constraint |

---

## 4. Defensive Contributions — 2025/26 Handling

### 4.1 The Problem

FPL introduced defensive contribution scoring in 2025/26:
- Every 2 tackles, interceptions, blocks, or clearances = 1 point
- Applies to all positions

Historical seasons (2021-25) have NO defensive contribution points. But Understat provides tackle/interception/block data for all seasons.

### 4.2 Strategy: Hybrid Training

**Step 1: Train defensive ACTIVITY rates from all seasons (2021-26)**
- Understat has tackles, interceptions per player per match across all seasons
- Calculate `defensive_actions_per_90` rolling stats from full history
- This gives us: "Player X averages 4.2 defensive actions per 90"

**Step 2: Train defensive POINTS conversion from 2025/26 only**
- From 2025/26 FPL data, learn: `defensive_points = floor(defensive_actions / 2)`
- This is deterministic — no ML needed, just a formula

**Step 3: For historical training (backfill)**
- Apply 2025/26 defensive scoring rules to historical Understat defensive stats
- This creates a synthetic `defensive_points` column for 2021-25
- Train the full model on augmented historical data

```python
def backfill_defensive_points(player_stats: pl.DataFrame) -> pl.DataFrame:
    """Apply 2025/26 defensive scoring to historical data."""
    # Understat has tackles + interceptions + blocks as separate columns
    # FPL defensive_contribution = tackles + interceptions + blocks + clearances
    if 'defensive_contribution' not in player_stats.columns:
        player_stats['defensive_contribution'] = (
            player_stats.get('tackles', 0) +
            player_stats.get('interceptions', 0) +
            player_stats.get('blocks', 0) +
            player_stats.get('clearances', 0)
        )
    # FPL rule: 1 point per 2 defensive actions
    player_stats['defensive_points'] = (
        player_stats['defensive_contribution'] // 2
    ).clip(upper=5)  # Cap at reasonable max
    return player_stats
```

**Why this works:**
- Defensive activity patterns are stable across seasons (tackle rates don't change much)
- Only the POINT CONVERSION is new
- By backfilling, we train on 5x more data instead of just 2025/26

---

## 5. Rolling Window Calculations

### 5.1 Windows

Use **multiple windows** to capture both form (recent) and quality (stable):
- **W3**: Last 3 fixtures — hot/cold form
- **W5**: Last 5 fixtures — medium-term form
- **W10**: Last 10 fixtures — baseline quality
- **Season**: All fixtures — playing time trends

Feature: `shots_per90_W3 / shots_per90_W10` = form ratio (above 1.0 = in form)

### 5.2 Minutes Reliability

```
start_prob_W = Σ(started_i) / N_fixtures_W

avg_minutes_started_W = Σ(minutes_i | started_i=1) / Σ(started_i)

expected_minutes = start_prob_W × avg_minutes_started_W

minutes_std_W = stddev(minutes_i over W)
```

### 5.3 Per-90 Normalization

For any count metric M (shots, tackles, key passes):

```
M_per_90_W = (Σ M_i / Σ minutes_i) × 90
```

Only use fixtures where `minutes >= 30` to avoid noise from sub appearances.

### 5.4 Player Usage Shares

```
xG_share_W = player_xG_W / team_xG_W
xA_share_W = player_xA_W / team_xA_W
shot_share_W = player_shots_W / team_shots_W
def_share_W = player_def_actions_W / team_def_actions_W
```

### 5.5 Quality Metrics

```
xG_per_shot_W = player_xG_W / player_shots_W
shots_in_box_rate_W = shots_in_box_W / shots_W
xA_per_key_pass_W = xA_W / key_passes_W
```

### 5.6 Team Strength

```
team_xG_for_W = Σ(xG_for_i) / N_matches_W
team_xG_against_W = Σ(xG_against_i) / N_matches_W
team_xG_std_W = stddev(xG_for_i over W)
```

### 5.7 Context Features

```
rest_days = (kickoff_time_i - kickoff_time_{i-1}).days
fatigue_factor = max(0, 1 - (3 - rest_days) × 0.1)  # 1.0 at 3+ days, 0.7 at 0 days
is_home = was_home (boolean)
fixture_difficulty = opponent_strength_rating  # from FPL team data
```

---

## 6. Availability Modeling

```
availability_prob = chance_of_playing_next_round / 100

# If chance_of_playing_next_round is null:
#   status == 'a' → 0.95
#   status == 'd' → 0.50
#   status == 'i' or 's' → 0.05

play ~ Bernoulli(availability_prob)
if play == 0 → minutes = 0 → all points = 0
```

---

## 7. ML Model Architecture

### 7.1 Model 1: Expected Minutes

**Type:** Regression (LightGBM)
**Target:** minutes next GW
**Key features:**
- start_prob_W3, W5, W10
- avg_minutes_started_W5
- availability_prob
- minutes_last_3_gw (trend)
- position (GKP starters play 90, subs play 0)

### 7.2 Model 2: Expected xG

**Type:** Regression (LightGBM)
**Target:** xG next GW (from Understat)
**Key features:**
- xG_per_90_W3, W5, W10
- xG_share_W5
- shots_per_90_W5
- xG_per_shot_W5
- shots_in_box_rate_W
- fixture_difficulty
- is_home
- expected_minutes
- position (FWD/MID have higher base rates)

### 7.3 Model 3: Expected xA

**Type:** Regression (LightGBM)
**Target:** xA next GW
**Key features:**
- xA_per_90_W3, W5, W10
- xA_share_W5
- key_passes_per_90_W5
- xA_per_key_pass_W5
- fixture_difficulty
- is_home
- expected_minutes

### 7.4 Model 4: Clean Sheet Probability

**Type:** Classification (LightGBM)
**Target:** clean_sheet (binary)
**Key features:**
- team_xG_against_W5 (lower = better defense)
- opponent_xG_for_W5 (higher = harder to keep CS)
- is_home (home teams CS more)
- fixture_difficulty
- position (GKP/DEF get CS points, MID/FWD don't)
- team_defensive_strength (FPL ratings)

**Important:** This is a TEAM-level prediction, applied to all players on the team.

### 7.5 Model 5: Defensive Points (2025/26 only)

**Type:** Regression (LightGBM)
**Target:** defensive_points = floor(defensive_contribution / 2)
**Key features:**
- def_actions_per_90_W3, W5, W10
- def_share_W5
- tackles_per_90_W5
- interceptions_per_90_W5
- position (DEF highest, GKP lowest)
- opponent_xG_for (more attacks → more defensive actions)
- expected_minutes

**Training data:** 2025/26 only for points conversion. But use 2021-26 for defensive activity rates (Understat).

### 7.6 Model 6: Bonus Points Expectation

**Type:** Regression (LightGBM) or heuristic
**Target:** bonus_points (0, 1, 2, or 3)
**Key features:**
- bps_per_90_W5
- xG + xA (attacking involvement)
- defensive_actions_per_90 (defenders get BPS for clean sheets)
- position

**Alternative (simpler):** Use historical BPS distribution:
```
expected_bonus = P(bonus=1)×1 + P(bonus=2)×2 + P(bonus=3)×3
where P(bonus=k) = empirical_rate(player_bps_rank_in_match)
```

### 7.7 Temporal Validation

**NEVER use random train/test splits.** Always use temporal cross-validation:

```
Fold 1: Train on GW 1-10,  Test on GW 11
Fold 2: Train on GW 1-15,  Test on GW 16
Fold 3: Train on GW 1-20,  Test on GW 21
...
```

**Metrics:**
- Pinball loss (for regression models — quantile-aware)
- Log-loss (for classification — clean sheet)
- MAE on points prediction
- ROI on team selection (actual FPL points vs cost)

---

## 8. Monte Carlo Simulation

### 8.1 Team-Level Match Simulation (CRITICAL)

Simulate MATCHES first, then derive player outcomes:

```
# For each simulation iteration:
for match in upcoming_fixtures:
    # Team-level xG with home advantage
    home_xG = home_team_xG_for × home_advantage_factor
    away_xG = away_team_xG_for × away_disadvantage_factor
    
    # Dixon-Coles adjusted Poisson
    home_goals ~ Poisson(home_xG)
    away_goals ~ Poisson(away_xG)
    
    # Apply Dixon-Coles correction for low-scoring
    if (home_goals, away_goals) in [(0,0), (1,0), (0,1), (1,1)]:
        apply_dc_correction(home_goals, away_goals, home_xG, away_xG, ρ)
```

**Dixon-Coles correction:**
```
ρ ≈ -0.13 (typical PL value, can be estimated from data)

P(0,0) = τ(0,0,λ,μ,ρ) × P_poisson(0,λ) × P_poisson(0,μ)
P(1,0) = τ(1,0,λ,μ,ρ) × P_poisson(1,λ) × P_poisson(0,μ)
P(0,1) = τ(0,1,λ,μ,ρ) × P_poisson(0,λ) × P_poisson(1,μ)
P(1,1) = τ(1,1,λ,μ,ρ) × P_poisson(1,λ) × P_poisson(1,μ)

where:
τ(i,j,λ,μ,ρ) = 1 - λ×μ×ρ         for (0,0)
τ(i,j,λ,μ,ρ) = 1 + λ×ρ           for (1,0)
τ(i,j,λ,μ,ρ) = 1 + μ×ρ           for (0,1)
τ(i,j,λ,μ,ρ) = 1 - ρ             for (1,1)
```

### 8.2 Derive Player Outcomes FROM Match Outcome

```
# Clean sheet: directly from match result
CS = (opponent_goals == 0)  # NOT independent per player!

# Goals: distribute team goals using xG shares
team_xG_shares = [player.xG_share for player in team_players]
for each goal scored by team:
    scorer = Multinomial(team_xG_shares)

# Assists: distribute using xA shares (independent from goals)
team_xA_shares = [player.xA_share for player in team_players]
for each assist opportunity:
    assister = Multinomial(team_xA_shares)
```

### 8.3 Player-Level Simulation (Post-Match)

For each player, given match outcome:

```
# Availability gate
play ~ Bernoulli(availability_prob)
if not play: minutes = 0, points = 0

# Minutes (from ML model + noise)
minutes ~ Normal(pred_minutes, minutes_std)
minutes = clip(minutes, 0, 90)

# Goals (from ML model, scaled by actual minutes)
λ_goal = pred_xG × (minutes / 90) × opponent_def_factor
goals ~ Poisson(λ_goal)

# Assists
λ_assist = pred_xA × (minutes / 90)
assists ~ Poisson(λ_assist)

# Clean sheet (from match simulation)
CS = (opponent_goals == 0) AND (minutes >= 60)

# Defensive points (2025/26)
λ_def = pred_defensive_actions × (minutes / 90)
def_actions ~ Poisson(λ_def)
defensive_points = floor(def_actions / 2)

# Bonus (from BPS model or heuristic)
bonus ~ Categorical(bonus_distribution)

# Cards
yellow_card_prob = empirical_rate × position_factor × aggression_factor
yellow ~ Bernoulli(yellow_card_prob)
red_card_prob = yellow_card_prob × 0.05  # ~5% of yellows become red
red ~ Bernoulli(red_card_prob)
```

### 8.4 FPL Points Calculation

```
# Position-specific scoring (2025/26 rules)
GOAL_POINTS = {'GKP': 6, 'DEF': 6, 'MID': 5, 'FWD': 4}
CS_POINTS = {'GKP': 4, 'DEF': 4, 'MID': 1, 'FWD': 0}

appearance = 2 if minutes >= 60 else (1 if minutes > 0 else 0)
goal_pts = goals × GOAL_POINTS[position]
assist_pts = assists × 3
cs_pts = (1 if CS else 0) × CS_POINTS[position]
def_pts = defensive_points  # 2025/26 only
bonus_pts = bonus
card_pts = -(yellow × 1 + red × 3)
pen_miss = -(penalties_missed × 2)
og_pts = -(own_goals × 2)
pen_save_pts = penalties_saved × 5  # GK only
saves_pts = floor(saves / 3)  # GK only, 1 pt per 3 saves

total = appearance + goal_pts + assist_pts + cs_pts + def_pts
        + bonus_pts + card_pts + pen_miss + og_pts + pen_save_pts + saves_pts
```

### 8.5 Double Gameweek Handling

```
for player in DGW_players:
    total_points = 0
    for fixture in player.fixtures_this_gw:
        # Simulate each match independently
        points_fixture = simulate_player_in_match(player, fixture)
        total_points += points_fixture
    # DGW simulation uses summed points
```

### 8.6 Distribution Outputs

Over N=10,000 simulations per player:

```
mean_points = mean(points_i)
median_points = median(points_i)
p10 = percentile(points, 10)   # floor
p25 = percentile(points, 25)   # conservative
p75 = percentile(points, 75)   # optimistic
p90 = percentile(points, 90)   # ceiling
std = stddev(points_i)
boom_rate = P(points >= threshold)  # e.g., P(>=10 points)
```

---

## 9. Squad Optimization (ILP via PuLP)

### 9.1 Constraints

```
Budget:    Σ(price_i × selected_i) ≤ 100.0
Squad:     Σ(selected_i) = 15
Position:  Σ(selected_i | pos=GKP) = 2
           Σ(selected_i | pos=DEF) = 5
           Σ(selected_i | pos=MID) = 5
           Σ(selected_i | pos=FWD) = 3
Club:      Σ(selected_i | team=j) ≤ 3  ∀j
Captain:   Σ(captain_i) = 1
           captain_i ≤ selected_i  ∀i
```

### 9.2 Starting XI Selection

From the 15-player squad, select 11 starters:
```
Start:     Σ(starting_i) = 11
Formation: valid_formation(starting)
           Σ(starting_i | pos=GKP) = 1
           Σ(starting_i | pos=DEF) ≥ 3
           Σ(starting_i | pos=MID) ≥ 2
           Σ(starting_i | pos=FWD) ≥ 1
```

### 9.3 Objective Functions

**Squad selection (maximize expected value):**
```
maximize: Σ(mean_points_i × selected_i) + mean_points_captain
```

**Risk-adjusted squad (for transfers):**
```
maximize: Σ(λ × p25_i + (1-λ) × mean_points_i) × selected_i
where λ = 0.3 (conservative) to 0.0 (aggressive)
```

**Captain selection (maximize ceiling):**
```
maximize: p90_points  # Want highest upside for doubled score
```

### 9.4 Transfer Optimization

```
# Single transfer: find best swap
maximize: mean_points_in - mean_points_out
subject to: price_in ≤ price_out + bank

# Double transfer: find best pair
maximize: mean_points_in1 + mean_points_in2 - mean_points_out1 - mean_points_out2
subject to: price_in1 + price_in2 ≤ price_out1 + price_out2 + bank
```

### 9.5 Chip Strategy

| Chip | Optimization Change |
|------|---------------------|
| Triple Captain | `captain_points = points × 3` instead of 2 |
| Bench Boost | `total = Σ(all_15_points)` instead of Σ(starting_11) |
| Free Hit | Re-optimize full squad for one GW, ignoring long-term value |
| Wildcard | Multi-week optimization, minimize transfers needed |

---

## 10. Model Evaluation

### 10.1 Offline Metrics

| Model | Metric | Target |
|-------|--------|--------|
| Minutes | MAE | < 15 min |
| xG | Pinball loss (τ=0.5) | Beat naive baseline |
| xA | Pinball loss (τ=0.5) | Beat naive baseline |
| CS | Log-loss | < 0.55 |
| Defensive pts | MAE | < 1.5 pts |
| Overall points | MAE | < 2.5 pts vs actual |
| Team selection | ROI | > FPL average |

### 10.2 Baselines to Beat

1. **FPL's own ep_next** — if you can't beat FPL's expected points, the model adds no value
2. **Season average per-90** — naive rolling mean, no ML
3. **Bookmaker odds** — implied probabilities from betting markets

### 10.3 Temporal Validation Protocol

```
For each GW g in [15, 20, 25, 30, 35]:
    Train on data up to GW g-1
    Predict GW g
    Evaluate against actual GW g results
    Record per-model and overall metrics
```

Average across all validation GWs for final score.

---

## 11. Pipeline Order

```
1. Data ingestion (Bronze → Silver)
2. Feature engineering (rolling windows, per-90, shares)
3. Model training (temporal CV)
4. Model evaluation (beat baselines?)
5. Monte Carlo simulation (10k iterations)
6. Squad optimization (ILP)
7. Output: predictions, team, captain, transfers
8. Streamlit dashboard
```

Steps 2-6 run weekly (before each GW deadline).
Step 1 runs daily.
Step 8 is always-on.

---

## 12. Storage

### ML Spec (this document)
→ GitHub (`docs/ML_SIMULATION_SPEC.md`) — versioned with code

### Model artifacts
→ `data/models/` (committed — NOT gitignored; required for Streamlit Cloud deployment)

### Experiment tracking
→ MLflow local (`mlruns/`) — logs params, metrics, artifacts per training run

### Predictions
→ Supabase `gold_predictions` table — per player per GW, queryable by Streamlit

### Training data
→ Supabase silver tables (input) → Polars DataFrames (in-memory) → model
