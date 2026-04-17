# Fantasy Football Assistant - Project Documentation

## Problem Statement

Fantasy Premier League (FPL) managers face a challenging decision-making problem: select 15 players from a pool of 500+ within a £100M budget, subject to position constraints (2 GKP, 5 DEF, 5 MID, 5 FWD), maximum 3 players per club, while maximizing expected points across upcoming gameweeks.

**The core challenge:**
- Information overload: 500+ players, 38 gameweeks, multiple data sources
- Complex constraints: budget, positions, club limits, captain choices
- Uncertainty: player injuries, form fluctuations, fixture difficulty
- Time-intensive: analyzing fixtures, form, matchups manually

**Our approach:** A data-driven system that automates prediction and optimization to help managers make better decisions.

---

## The Role of AI in This Project

This project demonstrates how artificial intelligence can transform decision-making in competitive fantasy sports. With hundreds of players, constantly changing form, and complex constraints, manually analyzing all relevant factors is practically impossible for any human. AI solves this by processing historical data at scale, detecting patterns invisible to the human eye, and generating probabilistic forecasts that account for uncertainty. The LightGBM models learn from features like rolling performance, head-to-head records, fixture difficulty, and rest days to predict not just expected points, but also the probability of a player starting. Without AI, managers must rely on intuition or simple statistics; with AI, they gain data-driven confidence in their decisions. This architecture can be extended with future AI capabilities such as sentiment analysis on injury news and generative AI for natural language insights—further enhancing the assistant's value as an ongoing decision-support tool.

---

## Data Sources

![Data Sources Diagram](docs/diagrams/data_sources.png)

| Source | Description | Data Type | Update Frequency |
|--------|-------------|-----------|------------------|
| **FPL API** | Official Fantasy Premier League API | Player prices, selections, fixtures, team strength | Daily |
| **Vaastav** | Historical FPL gameweek data (2021-2025) | Per-player GW stats, points, minutes | One-time + annual |
| **Understat** | xG, xA, shot data | Match-level xG/xA, player match stats | Historical + seasonal |
| **Team Mappings** | Cross-source team ID mappings | FPL ↔ Understat ↔ Vaastav ID mappings | Auto-generated |

### Available Data Files

```
data/raw/
├── fpl/2025-26/
│   └── bootstrap-static.json    # Current season player/team data
├── vaastav/
│   ├── 2021-22/gws.parquet      # Historical GW data
│   ├── 2022-23/gws.parquet
│   ├── 2023-24/gws.parquet
│   └── 2024-25/gws.parquet
├── understat/
│   ├── 2021_22/                 # Player match stats, shots
│   ├── 2022_23/
│   ├── 2023_24/
│   ├── 2024_25/
│   └── 2025_26/
└── team_mappings.csv            # Cross-source mappings
```

---

## Architecture & Medallion Structure

![Architecture Diagram](docs/diagrams/architecture.png)

### Data Flow

| Layer | Tables | Purpose |
|-------|--------|---------|
| **Bronze** | `bronze_*` | Raw, immutable data as ingested |
| **Silver** | `silver_*` | Cleaned, standardized, crosswalk applied |
| **Gold** | `gold_*` | Features aggregated, predictions ready |

---

## Tools & Technologies

![Tech Stack Diagram](docs/diagrams/tech_stack.png)

### High-Level Categories

| Category | Tools | Purpose |
|----------|-------|---------|
| **Data Processing** | Polars, Pandas, NumPy | ETL, feature engineering |
| **ML Models** | LightGBM, Scikit-learn, XGBoost | Performance prediction, classification |
| **Optimization** | PuLP (ILP solver) | Squad optimization under constraints |
| **Simulation** | NumPy (Monte Carlo) | Point distribution generation |
| **Database** | Supabase (Postgres), DuckDB | Storage, querying |
| **MLOps** | MLflow | Model tracking, versioning |
| **Validation** | Pandera, Pydantic | Schema enforcement |
| **UI** | Streamlit | Interactive dashboard |
| **Code Quality** | Ruff, Black, MyPy, Pre-commit | Linting, formatting, type checking |
| **Testing** | Pytest, Coverage | Unit/integration testing |
| **CI/CD** | GitHub Actions | Automated pipeline |

---

## AI & Machine Learning Implementation

![AI Implementation Diagram](docs/diagrams/ai_implementation.png)

### Current AI/ML Components

| Component | Type | Description |
|-----------|------|-------------|
| **Points Predictor** | Regression (LightGBM) | Predicts expected points per gameweek |
| **XI Classifier** | Classification (LightGBM) | Predicts starting XI probability |
| **Monte Carlo Simulator** | Statistical Simulation | 10,000 iterations for point distributions |

### Core ML Models

```python
# Example: Points Prediction Model
from src.models.lightgbm_model import train_points_predictor

model = train_points_predictor(
    features_df,           # gold_player_features
    target="total_points",
    model_path="data/models/points_predictor.joblib"
)

# Example: XI Probability Model  
from src.models.starting_xi import train_xi_model

xi_model = train_xi_model(
    features_df,
    target="minutes",
    threshold=60,
    model_path="data/models/xi_classifier.joblib"
)
```

### Performance Targets

| Model | Metric | Target |
|-------|--------|--------|
| Points Predictor | MAE | < 2.0 points |
| Goals Predictor | RMSE | < 0.5 goals |
| XI Classifier | Accuracy | > 80% |
| Clean Sheet | Accuracy | > 75% |

---

## Future AI Implementations

### Planned Enhancements (BACKLOG-003)

| Feature | AI Type | Description |
|--------|---------|-------------|
| **Sentiment Analysis** | NLP | Analyze news articles, social media for player sentiment |
| **Generative AI Insights** | GenAI | Natural language transfer recommendations |
| **LLM Match Previews** | GenAI | Generate match narrative summaries |
| **Injury Impact Analysis** | NLP/Sentiment | Assess injury news impact on player value |

### Architecture for Future AI

![Future AI Diagram](docs/diagrams/future_ai.png)

### Suggested Stack for Future AI

| Component | Option | Notes |
|-----------|--------|-------|
| **Sentiment Analysis** | HuggingFace Transformers | Pre-trained BERT for sentiment |
| **LLM** | OpenAI GPT-4 / Anthropic Claude / Ollama (local) | For insight generation |
| **News API** | NewsAPI, Twitter/X API | Player news ingestion |

---

## Streamlit Dashboard Preview

### Current Pages

```python
# src/streamlit_app/pages/
1_Predictions.py      # Player expected points & probabilities
2_Team_Selector.py   # Squad optimizer (ILP)
3_Match_Preview.py    # H2H analysis
4_Transfer_Suggestions.py  # Transfer recommendations
5_Captain_Analysis.py     # Captain picks
```

### Page Wireframes

#### 1. Predictions Page
```
┌─────────────────────────────────────────────────────────────┐
│ ⚽ FPL Simulation Dashboard                    [Gameweek ▼] │
├─────────────────────────────────────────────────────────────┤
│ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐            │
│ │ Filters     │ │ Metrics     │ │ Top Players │            │
│ │             │ │             │ │             │            │
│ │ Position ▼  │ │ Avg xPTS 4.2│ │ 1. Salah 5.1│            │
│ │ Team     ▼  │ │ Players  50 │ │ 2. Haaland │            │
│ │ £4.0-£15.0  │ │ GW       22 │ │ 3. Saka   │            │
│ └─────────────┘ └─────────────┘ └─────────────┘            │
│                                                             │
│ ┌─────────────────────────────────────────────────────────┐│
│ │ Player Table                                            ││
│ │ Name      │ Team │ Pos │ Price │ Form │ xPTS │ Goal% ││
│ │─────────────────────────────────────────────────────────││
│ │ Salah     │ LIV  │ MID │ 12.8  │ 8.2  │ 5.1  │ 45%   ││
│ │ Haaland   │ MCI  │ FWD │ 14.2  │ 9.1  │ 5.0  │ 52%   ││
│ └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

#### 2. Team Optimizer Page
```
┌─────────────────────────────────────────────────────────────┐
│ 🏆 Team Selector - Optimal Squad Builder                     │
├─────────────────────────────────────────────────────────────┤
│ Budget: £100.0M  |  Constraints: [2 GKP, 5 DEF, 5 MID, 5 FWD]│
│                                                             │
│ ┌────────────────────────┐ ┌───────────────────────────────┐│
│ │ Optimal Squad (4-3-3)  │ │ Bench Options                 ││
│ │                        │ │                               ││
│ │ GKP: Ramsdale (£5.0)   │ │ GKP: Arek (£4.2)             ││
│ │ DEF: Saliba, Gabriel,  │ │ DEF: Reguilon, Botman        ││
│ │      Porro, Estupinan  │ │                               ││
│ │ MID: Salah, Saka,      │ │ Bench: Eze, Mac Allister     ││
│ │      Palmer, Bruno     │ │                               ││
│ │ FWD: Haaland, Watkins  │ │ Expected Points: 78.4       ││
│ └────────────────────────┘ └───────────────────────────────┘│
│                                                             │
│ [Optimize Squad]  [Lock Team]  [Export]                     │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow in Streamlit

![Streamlit Flow Diagram](docs/diagrams/streamlit_flow.png)

### Caching Strategy

All Supabase queries use `@st.cache_data` with 1-hour TTL:

```python
@st.cache_data(ttl=3600)
def load_players() -> pl.DataFrame:
    """Load player data - cached for 1 hour."""
    return read_from_supabase("silver_players", ...)
```

---

## Summary

| Aspect | Current State | Future State |
|--------|--------------|--------------|
| **Data** | FPL, Vaastav, Understat | + News APIs, Social Media |
| **ML** | LightGBM regression/classification | + Deep learning, Transformer models |
| **AI** | Statistical simulation | + Sentiment analysis, LLM insights |
| **UI** | Streamlit dashboard | + Mobile app, Slack bot |

The project provides a solid foundation for fantasy sports prediction and can be extended with additional AI capabilities for enhanced decision support.
