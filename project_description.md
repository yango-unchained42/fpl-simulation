# FPL Prediction Pipeline - Project Specification

## 🎯 **Project Overview**

A comprehensive Football (Soccer) Prediction Pipeline for Fantasy Premier League (FPL) using modern data engineering, machine learning, and Monte Carlo simulations. The system ingests data from multiple sources, engineers features, trains predictive models, and provides an interactive Streamlit dashboard for FPL managers.

---

## 📋 **Key Decisions & Requirements**

### **Confirmed Decisions**

| Decision | Value |
|----------|-------|
| **Name Format** | "First Last" (e.g., "Bukayo Saka") |
| **H2H Window** | Full 3 seasons (2021/22-2023/24) |
| **Player H2H Scope** | Option C: Player vs team defense only |
| **Model Retraining** | Manual trigger on major changes |
| **Output Format** | Supabase (hosted Postgres, free tier) |
| **Data Sources** | FPL API, Understat (via `soccerdata` package) |
| **Deployment** | Streamlit Cloud (free tier) |
| **Priority** | Player predictions accuracy (highest) |

---

## 🏗️ **Architecture Overview**

### **Tech Stack**

#### **Data Engineering**
- **Processing**: Polars (fast, memory-efficient) + DuckDB (SQL queries)
- **Ingestion**: `soccerdata` package (Understat) + FPL API + vaastav GitHub
- **Validation**: Pandera (schema validation)
- **Database**: Supabase (hosted Postgres, free tier — persistent across Streamlit Cloud deployments)

#### **ML & Statistics**
- **ML Framework**: LightGBM (primary), Scikit-learn, XGBoost
- **Optimization**: PuLP (ILP solver for team selection)
- **Simulation**: NumPy (Monte Carlo, Poisson regression)
- **Tracking**: MLflow (local experiment tracking)

#### **Frontend**
- **UI Framework**: Streamlit 1.30+ with Pages API
- **Data Tables**: streamlit-aggrid (interactive tables)
- **Charts**: Plotly (interactive) + Matplotlib (static)

#### **Code Quality & DevOps**
- **Linting**: Ruff (fastest Python linter)
- **Formatting**: Black + isort
- **Type Checking**: MyPy (strict type hints)
- **Pre-commit**: pre-commit hooks
- **CI/CD**: GitHub Actions
- **Testing**: pytest + pytest-asyncio

---

## 📁 **Project Structure**

```
fpl_simulation/
├── pyproject.toml              # Modern Python project config
├── .pre-commit-config.yaml     # Pre-commit hooks
├── README.md                   # Project overview
├── PROJECT.md                  # This file
├── main.py                     # Pipeline entry point
├── .github/
│   └── workflows/
│       ├── ci.yml              # CI/CD pipeline
│       └── pipeline.yml        # Scheduled data pipeline
├── src/
│   ├── __init__.py
│   ├── data/
│   │   ├── __init__.py
│   │   ├── ingest_fpl.py       # FPL API collector (daily)
│   │   ├── ingest_vaastav.py   # vaastav historical GW stats loader
│   │   ├── ingest_understat.py # Understat scraper (soccerdata, historical)
│   │   ├── clean.py            # Data cleaning & validation
│   │   ├── merge.py            # Data merging strategy
│   │   └── database.py         # Supabase (Postgres) client & operations
│   ├── features/
│   │   ├── __init__.py
│   │   ├── h2h_metrics.py      # Head-to-head metrics
│   │   ├── rolling_features.py # Rolling averages
│   │   └── engineer.py         # Feature engineering
│   ├── models/
│   │   ├── __init__.py
│   │   ├── player_predictor.py # LightGBM player model
│   │   ├── starting_xi.py      # Starting XI predictor
│   │   ├── match_simulator.py  # Monte Carlo simulator
│   │   └── team_optimizer.py   # ILP team optimizer
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── name_resolver.py    # Name standardization
│   │   ├── validators.py       # Data validators
│   │   └── mlflow_client.py    # MLflow integration
│   └── streamlit_app/
│       ├── __init__.py
│       ├── pages/
│       │   ├── 1_📊_Predictions.py
│       │   ├── 2_⚽_Team_Selector.py
│       │   ├── 3_📈_Match_Preview.py
│       │   ├── 4_🔄_Transfer_Suggestions.py
│       │   └── 5_🎯_Captain_Analysis.py
│       └── components/
├── tests/
│   ├── __init__.py
│   ├── test_data.py
│   ├── test_features.py
│   ├── test_models.py
│   └── test_integration.py
├── data/
│   ├── raw/
│   │   ├── fpl_api/
│   │   ├── understat/
│   ├── processed/
│   └── models/
├── mlruns/                     # MLflow local tracking (gitignored)
└── outputs/
    └── predictions_export/     # Local prediction exports (optional backup)
```

---

## 📊 **Data Layer**

### **Data Sources**

| Source | Purpose | Frequency | Access |
|--------|---------|-----------|--------|
| **FPL API** | Player stats, prices, team info, injury status | Daily | Public API |
| **vaastav/FPL** | Historical per-GW FPL stats (2016/17–present) — primary training dataset | One-time load + annual refresh | GitHub CSV download |
| **Understat** | xG, xA, match-level data | One-time historical load + seasonal refresh | `soccerdata` |

### **Data Flow**

```
Data Sources → Ingestion → Cleaning → Merging → Feature Engineering → Supabase
     │              │             │           │              │           │
     ▼              ▼             ▼           ▼              ▼           ▼
  Raw Data     Processed Data  Cleaned     Merged         Engineered  Final
              (3 seasons)      + Validated  + H2H         Features     DB
```

### **Database Schema (Supabase / Postgres)**

```sql
-- Players
CREATE TABLE players (
    player_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,              -- "Bukayo Saka" format
    team_id INTEGER,
    position TEXT,                   -- GK, DEF, MID, FWD
    price DECIMAL(5,2),
    selected_by_percent DECIMAL(5,2),
    status TEXT DEFAULT 'a'          -- a=available, d=doubtful, i=injured, s=suspended, u=unavailable
);

-- Teams
CREATE TABLE teams (
    team_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    strength_attack DECIMAL(5,2),
    strength_defense DECIMAL(5,2),
    strength_midfield DECIMAL(5,2)
);

-- Fixtures
CREATE TABLE fixtures (
    fixture_id INTEGER PRIMARY KEY,
    home_team_id INTEGER,
    away_team_id INTEGER,
    gameweek INTEGER,
    date TIMESTAMP,
    is_home_advantage BOOLEAN,
    is_double_gw BOOLEAN DEFAULT FALSE,  -- team plays twice this GW
    is_blank_gw BOOLEAN DEFAULT FALSE    -- team has no fixture this GW
);

-- Player Stats (per gameweek)
CREATE TABLE player_stats (
    player_id INTEGER,
    fixture_id INTEGER,
    gameweek INTEGER,
    minutes INTEGER,
    goals INTEGER,
    assists INTEGER,
    clean_sheets INTEGER,
    yellow_cards INTEGER,
    red_cards INTEGER,
    saves INTEGER,                    -- GK saves (bonus: +1 per 3)
    points INTEGER,
    bps INTEGER,                      -- Bonus Points System
    xg DECIMAL(10,4),                 -- Expected Goals
    xa DECIMAL(10,4),                 -- Expected Assists
    xgb DECIMAL(10,4),                -- Expected Goal Buildup
    PRIMARY KEY (player_id, fixture_id)
);

-- Team H2H Metrics (last 3 seasons)
CREATE TABLE team_h2h (
    home_team_id INTEGER,
    away_team_id INTEGER,
    avg_goals_scored DECIMAL(5,2),
    avg_goals_conceded DECIMAL(5,2),
    clean_sheet_rate DECIMAL(5,2),
    last_5_meetings JSONB,            -- JSON array
    appearances INTEGER,
    PRIMARY KEY (home_team_id, away_team_id)
);

-- Player vs Team Defense (Option C: Team H2H)
CREATE TABLE player_vs_team (
    player_id INTEGER,
    opponent_team_id INTEGER,
    avg_points DECIMAL(5,2),
    avg_xg DECIMAL(10,4),
    goals INTEGER,
    appearances INTEGER,
    PRIMARY KEY (player_id, opponent_team_id)
);

-- Predictions
CREATE TABLE predictions (
    id SERIAL PRIMARY KEY,
    gameweek INTEGER NOT NULL,
    player_id INTEGER,
    start_probability DECIMAL(5,2),   -- P(starting)
    expected_points DECIMAL(5,2),     -- EP
    expected_goals DECIMAL(5,2),
    expected_assists DECIMAL(5,2),
    clean_sheet_probability DECIMAL(5,2),
    predicted_points INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Match Simulations (aggregated — raw runs stay in memory)
CREATE TABLE match_simulations (
    fixture_id INTEGER PRIMARY KEY,
    home_win_pct DECIMAL(5,2),
    draw_pct DECIMAL(5,2),
    away_win_pct DECIMAL(5,2),
    home_cs_pct DECIMAL(5,2),         -- home clean sheet probability
    away_cs_pct DECIMAL(5,2),         -- away clean sheet probability
    score_distribution JSONB,         -- e.g. {"1-0": 0.12, "2-1": 0.09, ...}
    expected_home_goals DECIMAL(5,2),
    expected_away_goals DECIMAL(5,2),
    p10_home_goals INTEGER,           -- 10th percentile
    p90_home_goals INTEGER            -- 90th percentile
);
```

---

## 🔧 **Pipeline Architecture**

### **1. Data Ingestion Layer**

#### **vaastav Historical Loader**
- **Source**: `https://github.com/vaastav/Fantasy-Premier-League`
- **Data Collected**:
  - Per-gameweek FPL stats for all players (2016/17–present)
  - Season summary stats
  - Historical prices, ownership, BPS
- **Frequency**: One-time historical load + annual refresh at season start
- **Note**: Uses FPL player IDs natively — no ID crosswalk required for FPL joins

#### **FPL API Collector**
- **Endpoint**: `https://fantasy.premierleague.com/api/`
- **Data Collected**:
  - Player info (id, name, team, position, price)
  - Gameweek stats (minutes, goals, assists, points)
  - Team info (strength ratings)
  - Fixtures schedule
- **Frequency**: Daily (GitHub Actions)

#### **Understat Scraper** (via `soccerdata`)
- **Data Collected**:
  - Match-level xG, xA
  - Player shot data
  - Season stats (2021/22-2023/24)
- **Frequency**: One-time historical load + seasonal refresh (not a live feed)

#### **Understat Scraper** (via `soccerdata`)
- **Data Collected**:
  - xG, xA per shot
  - Match-level xG statistics
  - Player match-level stats (xG, xA, shots, key passes)
  - Player season-aggregated stats
- **Frequency**: One-time historical load + seasonal refresh (not a live feed)

---

### **2. Data Cleaning & Merging**

#### **Step 1: Name Standardization**
```python
# All names in "First Last" format
name_mapping = {
    "Saka": "Bukayo Saka",
    "Salah": "Mohamed Salah",
    "Haaland": "Erling Haaland"
}
```

#### **Step 2: Missing Data Imputation**
- **Player minutes**: Understat probability-weighted estimates
- **Missing stats**: Rolling averages + position-based defaults
- **Outliers**: Winsorize at 1% and 99%

#### **Step 3: Data Validation**
```python
# Pandera schema validation
schema = pa.DataFrameSchema({
    "player_id": pa.Column(int),
    "name": pa.Column(str),
    "position": pa.Column(str, pa.Check(lambda x: x in ["GK", "DEF", "MID", "FWD"]))
})
```

#### **Step 4: H2H Feature Calculation**
- **Team H2H**: Last 3 seasons of head-to-head matches
- **Player vs Team**: Player performance against specific team defenses
- **Window**: Full 3 seasons (2021/22-2023/24)

---

### **3. Feature Engineering Layer**

#### **Player Features**
```python
features = {
    # Rolling averages
    "points_last_3": rolling_avg(points, window=3),
    "points_last_5": rolling_avg(points, window=5),
    "points_last_10": rolling_avg(points, window=10),
    
    # Expected metrics
    "xg_per_90": xg / minutes * 90,
    "xa_per_90": xa / minutes * 90,
    "goals_over_xg": goals_scored - xg,       # finishing over/underperformance
    
    # Fixture difficulty (from FPL API + Dixon-Coles parameters)
    "fdr_next_1": fixture_difficulty_rating(gw + 1),
    "fdr_next_3": avg_fixture_difficulty_rating(gw + 1, gw + 3),
    "fdr_next_5": avg_fixture_difficulty_rating(gw + 1, gw + 5),
    
    # H2H metrics (Option C)
    "avg_points_vs_opponent": player_vs_team.avg_points,
    "avg_xg_vs_opponent": player_vs_team.avg_xg,
    "goals_vs_opponent": player_vs_team.goals,
    
    # Form metrics
    "form_index": weighted_average(last_5_gw),
    "consistency_score": std_dev(last_10_gw),
    
    # Context features
    "opponent_defense_strength": opponent.strong_defense,
    "home_advantage": is_home * 1.1,
    "rest_days": days_since_last_match,
    
    # Availability (hard filter — zero out unavailable players before optimizer)
    "is_available": status == 'a'
}
```

#### **Team Features**
```python
team_features = {
    "attack_strength": attack_rating,
    "defense_strength": defense_rating,
    "form_last_5": last_5_matches,
    "goals_per_game": avg_goals_scored,
    "goals_conceded_per_game": avg_goals_conceded,
    "clean_sheet_rate": clean_sheets / games
}
```

---

### **4. Modeling Layer**

#### **A. Player Performance Model** (LightGBM) ⭐ **PRIORITY**

**Objective**: Predict FPL points, goals, assists, clean sheets

**Input Features**:
- Player rolling stats (3/5/10 GW)
- Team strength (attack/defense)
- Opponent strength (defense)
- H2H metrics (player vs team defense)
- Context (home/away, rest days)

**Targets**:
- `points` (regression)
- `goals` (classification)
- `assists` (classification)
- `clean_sheet` (classification, DEF/GK only)
- `bps` (regression)

**Model**: LightGBM Regressor/Classifier
```python
model = lgb.LGBMRegressor(
    n_estimators=500,
    max_depth=10,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42
)
```

**Evaluation Metrics**:
- MAE (Mean Absolute Error) for points
- RMSE for goals/assists
- Accuracy/F1 for clean sheets
- Cross-validation (5-fold)

---

#### **B. Starting XI Predictor** (LightGBM)

**Objective**: Probability of starting

**Input Features**:
- Player form (last 5 GW)
- Minutes played (last 3 GW)
- Fitness (injury status from FPL API `status` field)
- Manager's lineup history vs opponent
- Position-specific features

**Output**: `start_probability` (0-1)

**Model**: LightGBM Classifier (consistent with player performance model; better-calibrated probabilities via `predict_proba`)
```python
model = lgb.LGBMClassifier(
    n_estimators=200,
    max_depth=8,
    learning_rate=0.05,
    class_weight='balanced',
    random_state=42
)
```

---

#### **C. Match Outcome Simulator** (Monte Carlo)

**Objective**: Goal distribution, win/draw/loss probabilities

**Method**: Poisson regression + Monte Carlo (10k simulations)

**Algorithm**:
```python
def simulate_match(home_strength, away_strength, h2h_adjustment):
    # Base rate from team strength
    home_lambda = home_strength * home_advantage * 1.0
    away_lambda = away_strength * 0.9  # Away penalty
    
    # H2H adjustment (last 3 seasons)
    home_lambda += h2h_adjustment.home_bonus
    away_lambda += h2h_adjustment.away_bonus
    
    # Monte Carlo simulation
    simulations = []
    for i in range(10000):
        home_goals = np.random.poisson(home_lambda)
        away_goals = np.random.poisson(away_lambda)
        simulations.append((home_goals, away_goals))
    
    return analyze_simulations(simulations)
```

**Output**:
- Goal distribution
- Win/draw/loss probabilities
- Clean sheet probabilities
- Player expected points (from match outcome)

---

#### **D. Team Optimizer** (Integer Linear Programming)

**Objective**: Optimal 15-player squad + starting XI within budget constraints

**Constraints**:
- Budget: £100.0m total (15-player squad)
- Positions: 2 GK, 5 DEF, 5 MID, 3 FWD (full squad)
- Starting XI: valid formation subset of squad (min 3 DEF, 2 MID, 1 FWD)
- Team limit: max 3 players per club
- Captain: 1 player (2x points), vice-captain: 1 player (fallback 2x)
- Injury filter: force `x[p] == 0` for any player where `status != 'a'` (pre-solve hard constraint)
- DGW handling: players with `is_double_gw = True` use summed expected points from two simulations

**Solver**: PuLP (ILP)
```python
from pulp import LpProblem, LpVariable, lpSum, LpMaximize

prob = LpProblem("FPL_Optimization", LpMaximize)

# Decision variables
x = LpVariable.dicts("select", player_ids, cat='Binary')
c = LpVariable.dicts("captain", player_ids, cat='Binary')

# Pre-solve: zero out unavailable players
for p in player_ids:
    if player_status[p] != 'a':
        prob += x[p] == 0

# Objective: Maximize expected points
prob += lpSum([x[p] * expected_points[p] for p in player_ids])

# Constraints
prob += lpSum([x[p] * price[p] for p in player_ids]) <= 100.0
prob += lpSum([x[p] for p in gk_players]) == 2
prob += lpSum([x[p] for p in def_players]) == 5
prob += lpSum([x[p] for p in mid_players]) == 5
prob += lpSum([x[p] for p in fwd_players]) == 3
prob += lpSum([c[p] for p in player_ids]) == 1
```

**Transfer Advisor** (separate ILP problem):
- Input: current squad + free transfers remaining
- Models 1-transfer and 2-transfer scenarios
- Applies -4 pt hit per transfer beyond free transfer allowance
- Output: ranked swap recommendations by net expected point gain
- Implemented as a delta-ILP: fix existing squad players as constraints, optimise over replacement candidates

---

### **5. Simulation Layer**

#### **Weekly Match Simulation**
- Run 10,000 Monte Carlo simulations per match
- Output: Goal distribution, win/draw/loss probabilities
- Feed into player expected points calculation

#### **Player Expected Points**
```python
# Appearance points: 1 pt if minutes < 60, 2 pts if minutes >= 60
appearance_pts = P(start) * (P(60_plus_mins) * 2 + P(sub_appearance) * 1)

# Goal/assist contribution
goal_pts     = P(score) * position_goal_points   # GK/DEF=6, MID=5, FWD=4
assist_pts   = P(assist) * 3

# Defensive contributions (GK and DEF only)
cs_pts       = P(clean_sheet) * cs_points        # GK/DEF=4 (full match), MID=1
save_pts     = P(GK) * expected_saves / 3        # +1 per 3 saves (GK only)

# Penalties
yellow_pts   = -1 * P(yellow_card)
red_pts      = -3 * P(red_card)

# Bonus
bonus_pts    = expected_bonus                    # average BPS-based bonus

EP = appearance_pts + goal_pts + assist_pts + cs_pts + save_pts + yellow_pts + red_pts + bonus_pts
```

#### **Captain Selection**
- Simulate captaincy scenarios (highest EP player vs safe option)
- Optimize for variance vs ceiling
- Output: Captain recommendation with confidence score

---

## 🔄 **Pipeline Workflow**

### **Daily Check (GitHub Actions)**

```
1. Check if next GW is within 24 hours
   ├─ YES: Run full pipeline
   └─ NO: Skip (exit gracefully)
```

### **Full Pipeline Execution**

```
┌─────────────────────────────────────────────────────────────┐
│                    DAILY PIPELINE                           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌────────────────────────────────────────┐
        │ 1. Data Ingestion                      │
        │    - Fetch FPL API (current season)    │
        │    - Load Understat (3 seasons)        │
        └────────────────────────────────────────┘
                              │
                              ▼
        ┌────────────────────────────────────────┐
        │ 2. Data Cleaning & Validation          │
        │    - Standardize names                 │
        │    - Impute missing values             │
        │    - Validate with Pandera             │
        └────────────────────────────────────────┘
                              │
                              ▼
        ┌────────────────────────────────────────┐
        │ 3. Feature Engineering                 │
        │    - Calculate H2H metrics             │
        │    - Rolling averages (3/5/10 GW)      │
        │    - Opponent-adjusted stats           │
        └────────────────────────────────────────┘
                              │
                              ▼
        ┌────────────────────────────────────────┐
        │ 4. Smart Retraining Check              │
        │    - Manager changes?                  │
        │    - Major transfers?                  │
        │    - Key injuries?                     │
        │    ├─ YES: Retrain models              │
        │    └─ NO: Load existing models         │
        └────────────────────────────────────────┘
                              │
                              ▼
        ┌────────────────────────────────────────┐
        │ 5. Model Predictions                   │
        │    - Player performance (LightGBM)     │
        │    - Starting XI probability           │
        │    - Match simulations (Monte Carlo)   │
        └────────────────────────────────────────┘
                              │
                              ▼
        ┌────────────────────────────────────────┐
        │ 6. Team Optimization                   │
        │    - Optimal XI (PuLP ILP)             │
        │    - Captain selection                 │
        └────────────────────────────────────────┘
                              │
                              ▼
        ┌────────────────────────────────────────┐
        │ 7. Output to Supabase                  │
        │    - Update predictions table          │
        │    - Store match simulations           │
        │    - Streamlit auto-refreshes          │
        └────────────────────────────────────────┘
```

---

## 🎯 **Key Implementation Details**

### **H2H Scope (Option C: Team Defense Only)**

**Rationale**: Best balance of data availability and specificity for 3-season window

**Implementation**:
```python
# Player vs team defense metrics
player_vs_team = df.groupby(['player_id', 'opponent_team_id']).agg({
    'points': 'mean',
    'xg': 'mean',
    'goals': 'sum',
    'minutes': 'sum'
}).reset_index()

# Features for prediction
features['avg_points_vs_opponent'] = player_vs_team.merge(
    current_fixture, on='opponent_team_id'
)['points_mean']
```

**Data Sources**:
- Understat: Match-level player stats vs specific teams
- Window: Full 3 seasons (2021/22-2023/24)

---

### **Smart Retraining (Manual Approach)**

**Trigger Events** (Manual):
1. Manager change (detected via news/API)
2. Major transfers (top 20 players)
3. Key injuries (injury list updates)

**Implementation**:
```python
# Manual trigger function
def trigger_retrain(reason: str):
    """
    Manually trigger model retraining
    
    Args:
        reason: str - "manager_change", "major_transfer", "injury"
    """
    logger.info(f"Manual retrain triggered: {reason}")
    
    # Retrain all models
    retrain_player_model()
    retrain_starting_xi_model()
    
    # Log to MLflow
    mlflow.log_param("retrain_reason", reason)
    mlflow.log_param("retrain_date", datetime.now())
```

**Fallback**: Monthly retrain if no triggers

---

### **Supabase Database Design**

**Advantages**:
- Hosted Postgres — persistent across Streamlit Cloud deployments
- Free tier: 500MB storage, no dyno resets
- ACID compliant with full Postgres feature set (JSONB, indexed columns)
- REST API + `supabase-py` client for easy Streamlit integration
- Row-level security available if needed

**Indexes**:
```sql
CREATE INDEX idx_player_stats_gameweek ON player_stats(gameweek);
CREATE INDEX idx_predictions_gameweek ON predictions(gameweek);
CREATE INDEX idx_player_h2h ON player_vs_team(player_id);
CREATE INDEX idx_fixture_gameweek ON fixtures(gameweek);
```

**Query Example**:
```python
# Get player predictions for next gameweek
query = """
SELECT 
    p.name,
    p.position,
    p.price,
    pr.expected_points,
    pr.start_probability,
    pr.predicted_points
FROM predictions pr
JOIN players p ON pr.player_id = p.player_id
WHERE pr.gameweek = ?
ORDER BY pr.predicted_points DESC
"""
```

---

## 📊 **Output Structure**

### **Supabase Database** (hosted Postgres)

```sql
-- Main prediction table
predictions (
    gameweek, player_id,
    start_probability, expected_points,
    expected_goals, expected_assists,
    clean_sheet_probability, predicted_points
)

-- Match simulations (aggregated — raw Monte Carlo runs stay in memory)
match_simulations (
    fixture_id,
    home_win_pct, draw_pct, away_win_pct,
    home_cs_pct, away_cs_pct,
    score_distribution,          -- JSONB: {"1-0": 0.12, "2-1": 0.09, ...}
    expected_home_goals, expected_away_goals
)

-- H2H metrics
team_h2h (
    home_team_id, away_team_id,
    avg_goals_scored, clean_sheet_rate
)

player_vs_team (
    player_id, opponent_team_id,
    avg_points, avg_xg, goals
)
```

### **Model Artefacts** (`data/models/`)

```
data/models/
├── player_predictor.joblib     # LightGBM points regression model
├── starting_xi.joblib          # LightGBM XI classifier
└── dixon_coles_params.joblib   # Fitted attack/defence parameters
```
These are committed to the repo and loaded at runtime in the deployed environment (MLflow is not available on Streamlit Cloud).

### **MLflow Tracking** (`mlruns/`) — local development only

> **Note**: MLflow writes to the local filesystem and is incompatible with Streamlit Cloud's ephemeral environment. Use MLflow only during local development and experimentation. Before deployment, serialise final models to `data/models/*.joblib` and commit them to the repo.

```
mlruns/
├── 0/                              # Experiment 0: Player Predictor
│   ├── runs/
│   │   ├── <run-id-1>/metrics/
│   │   │   └── mae_points
│   │   │   └── rmse_goals
│   │   └── <run-id-2>/...
│   └── models/
├── 1/                              # Experiment 1: Starting XI
└── 2/                              # Experiment 2: Match Simulator
```

---

## 🧪 **Testing Strategy**

### **Test Coverage Target**: >80%

#### **Unit Tests**
```python
# test_data.py
def test_fpl_ingestion():
    """Test FPL API data ingestion"""
    data = ingest_fpl_data()
    assert len(data) > 0
    assert "player_id" in data.columns

def test_name_standardization():
    """Test name format conversion"""
    assert standardize_name("Saka") == "Bukayo Saka"

# test_features.py
def test_h2h_calculation():
    """Test H2H metrics calculation"""
    h2h = calculate_team_h2h(matches_df)
    assert "avg_goals_scored" in h2h.columns

# test_models.py
def test_player_predictor():
    """Test player performance model"""
    model = train_player_model(train_data)
    predictions = model.predict(test_data)
    assert len(predictions) == len(test_data)
```

#### **Integration Tests**
```python
# test_integration.py
def test_full_pipeline():
    """Test end-to-end pipeline"""
    # Ingest data
    data = ingest_all_sources()
    
    # Clean and merge
    cleaned = clean_and_merge(data)
    
    # Engineer features
    features = engineer_features(cleaned)
    
    # Train model
    model = train_player_model(features)
    
    # Predict
    predictions = model.predict(test_features)
    
    assert len(predictions) > 0
```

#### **Data Validation Tests**
```python
def test_schema_validation():
    """Test Pandera schema validation"""
    schema.validate(player_df)  # Should not raise

def test_h2h_data_completeness():
    """Test H2H data has sufficient samples"""
    assert player_vs_team.groupby('player_id').size().min() >= 5
```

---

## 🚀 **Deployment Strategy**

### **Streamlit Cloud (Free Tier)**

**Setup**:
1. Push code to GitHub repository
2. Connect repo to Streamlit Cloud (free account)
3. Auto-deploys on every push to main branch
4. Supabase connection credentials stored in Streamlit secrets

**Supabase Configuration**:
- Free tier provides a persistent Postgres instance (500MB storage, no dyno resets)
- Connection via `supabase-py` client using `SUPABASE_URL` and `SUPABASE_KEY`
- Trained model artefacts (`.joblib`) committed to repo under `data/models/` and loaded at runtime — MLflow is local-development only and must not be used in the deployed environment

**Configuration** (`.streamlit/secrets.toml`):
```toml
# Supabase credentials (gitignored)
SUPABASE_URL = "https://<project>.supabase.co"
SUPABASE_KEY = "<anon-public-key>"
FPL_API_URL = "https://fantasy.premierleague.com/api/"
```

**Streamlit Config** (`.streamlit/config.toml`):
```toml
[server]
headless = true
port = 8501

[theme]
primaryColor = "#1c39bb"
backgroundColor = "#ffffff"
```

---

## 📈 **Implementation Phases**

### **Phase 1: Foundation (Week 1)**
- [ ] Initialize project structure (`pyproject.toml`)
- [ ] Set up pre-commit hooks (ruff, black, mypy)
- [ ] Configure GitHub Actions CI/CD
- [ ] Create directory structure
- [ ] Set up Supabase project + schema (tables, indexes)

### **Phase 2: Data Pipeline (Week 2)**
- [ ] Implement FPL API ingestion (daily, includes `status` field)
- [ ] Implement vaastav historical GW stats loader
- [ ] Implement Understat ingestion (soccerdata, historical)
- [ ] Build data cleaning pipeline
- [ ] Build data merging pipeline
- [ ] Implement H2H feature calculation

### **Phase 3: Feature Engineering (Week 3)**
- [ ] Implement rolling features (3/5/10 GW)
- [ ] Implement opponent-adjusted stats
- [ ] Implement form metrics
- [ ] Implement context features
- [ ] Write feature tests

### **Phase 4: ML Models (Week 4-5)** ⭐ **PRIORITY**
- [ ] **Player Performance Model (LightGBM)**
  - [ ] Feature preparation
  - [ ] Model training
  - [ ] Hyperparameter tuning
  - [ ] Evaluation & validation
- [ ] Starting XI Predictor (LightGBM Classifier)
- [ ] Match Simulator (Monte Carlo + Dixon-Coles, DGW-aware)
- [ ] Team Optimizer (PuLP ILP — 15-player squad, injury filter)
- [ ] Transfer Advisor (delta-ILP, 1/2 transfer scenarios)
- [ ] **Backtesting harness**: walk-forward validation (train on GWs 1–N, predict N+1, record MAE/RMSE, roll forward) — used to validate all success metrics

### **Phase 5: Application (Week 6)**
- [ ] Streamlit dashboard setup
- [ ] Predictions page
- [ ] Team selector page
- [ ] Match preview page
- [ ] Transfer suggestions page
- [ ] Captain analysis page

### **Phase 6: Testing & Deployment (Week 7)**
- [ ] Write comprehensive tests (>80% coverage)
- [ ] Configure GitHub Actions for daily pipeline
- [ ] Set up manual retrain trigger
- [ ] Deploy to Streamlit Cloud
- [ ] Documentation (README, API docs)

---

## 📝 **Open Questions & Decisions**

### **Resolved**
| Question | Decision | Rationale |
|----------|----------|-----------|
| Name format | "First Last" | Consistent across all sources |
| H2H window | Full 3 seasons | Maximize data for training |
| Player H2H scope | Option C (team defense) | Best balance of data + specificity |
| Model retraining | Manual trigger | Start simple, automate later |
| Output format | Supabase (hosted Postgres) | Persistent across Streamlit Cloud deployments; free tier sufficient |
| Data sources | FPL API + vaastav + Understat | Comprehensive coverage; vaastav is primary historical training source |
| Priority | Player predictions | Most valuable for FPL managers |
| Starting XI model | LightGBM Classifier | Consistent with player model; better-calibrated probabilities |
| MLflow | Local development only | Incompatible with ephemeral Streamlit Cloud filesystem; deploy serialised model artefacts instead |

### **Implementation Notes**
- **Understat**: Use `soccerdata` package for consistent API — historical only, not live feeds
- **vaastav**: Primary historical training source; uses FPL IDs natively (no crosswalk needed)
- **H2H Scope**: Player vs team defense only (Option C)
- **Retraining**: Manual trigger via function call; MLflow for local dev only
- **Database**: Supabase (hosted Postgres) — connect via `supabase-py`, credentials in Streamlit secrets
- **Model artefacts**: Serialised as `.joblib` under `data/models/` for deployment; MLflow tracking for local experiments only
- **Testing**: Target >80% coverage with pytest
- **DGW/BGW**: Detected from FPL API fixture data; DGW players run two simulations with summed points

---

## 🔧 **Dependencies**

### **Core Dependencies** (`pyproject.toml`)
```toml
[project]
name = "fpl-pipeline"
version = "0.1.0"
dependencies = [
    # Data processing
    "polars>=1.0.0",
    "duckdb>=1.0.0",
    "pandas>=2.0.0",
    
    # ML & Statistics
    "lightgbm>=4.0.0",
    "scikit-learn>=1.3.0",
    "xgboost>=2.0.0",
    "pulp>=2.7.0",
    
    # Data ingestion
    "soccerdata>=0.15.0",
    "requests>=2.31.0",
    "supabase>=2.0.0",
    
    # Validation
    "pandera>=0.18.0",
    
    # Tracking
    "mlflow>=2.9.0",
    
    # UI
    "streamlit>=1.28.0",
    "streamlit-aggrid>=1.0.0",
    "plotly>=5.18.0",
    
    # Utilities
    "python-dotenv>=1.0.0",
]
```

### **Dev Dependencies**
```toml
[project.dev-dependencies]
testing = [
    "pytest>=7.4.0",
    "pytest-asyncio>=0.21.0",
    "pytest-cov>=4.1.0",
]
linting = [
    "ruff>=0.1.8",
    "black>=23.12.0",
    "mypy>=1.8.0",
]
pre-commit = [
    "pre-commit>=3.6.0",
]
```

---

## 📚 **Documentation**

### **README.md**
- Project overview
- Quick start guide
- Installation instructions
- Usage examples
- Contributing guidelines

### **API Documentation** (mkdocstrings)
- Module documentation
- Function signatures
- Type hints
- Examples

### **Model Documentation**
- Model architectures
- Training methodology
- Performance metrics
- Feature importance

---

## 🎯 **Success Metrics**

### **Model Performance**
- **Player Points MAE**: < 2.0 points
- **Goals RMSE**: < 0.5 goals
- **Assists RMSE**: < 0.5 assists
- **Clean Sheet Accuracy**: > 75%
- **Starting XI Accuracy**: > 80%

### **Pipeline Performance**
- **Data Ingestion**: < 5 minutes
- **Feature Engineering**: < 10 minutes
- **Model Prediction**: < 2 minutes
- **Total Pipeline**: < 20 minutes

### **Code Quality**
- **Test Coverage**: > 80%
- **Type Hints**: 100% for public APIs
- **Linting**: 0 errors (ruff)
- **Formatting**: 100% (black)

---

## 🚦 **Next Steps**

1. **Initialize Project**
   - Create `pyproject.toml`
   - Set up pre-commit hooks
   - Create directory structure

2. **Data Ingestion**
   - Implement FPL API collector
   - Implement Understat via `soccerdata`
   - Test data loading

3. **Build Core Pipeline**
   - Data cleaning & validation
   - Feature engineering
   - Supabase database setup

4. **Priority: Player Model**
   - LightGBM implementation
   - Training & evaluation
   - MLflow tracking

5. **Complete Remaining Components**
   - Starting XI predictor
   - Match simulator
   - Team optimizer

6. **Streamlit Dashboard**
   - Build all pages
   - Connect to Supabase
   - Deploy to Streamlit Cloud

---

**Ready to begin implementation!** 🚀
