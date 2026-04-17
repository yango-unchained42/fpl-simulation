# Ticket: SPRINT0-004 - Directory Structure Setup

## Description
Create the complete project directory structure as defined in the project specification.

## Technical Requirements
Create the following directory structure:
```
fpl_simulation/
├── src/
│   ├── data/
│   │   ├── ingest_fpl.py       # FPL API collector (daily)
│   │   ├── ingest_vaastav.py   # vaastav historical GW stats loader
│   │   ├── ingest_understat.py # Understat scraper (soccerdata, historical)
│   │   ├── ingest_fbref.py     # FBRef parser (soccerdata, historical)
│   │   ├── clean.py            # Data cleaning & validation
│   │   ├── merge.py            # Data merging strategy
│   │   └── database.py         # Supabase (Postgres) client & operations
│   ├── features/
│   │   ├── h2h_metrics.py      # Head-to-head metrics
│   │   ├── rolling_features.py # Rolling averages
│   │   └── engineer.py         # Feature engineering
│   ├── models/
│   │   ├── player_predictor.py # LightGBM player model
│   │   ├── starting_xi.py      # Starting XI predictor
│   │   ├── match_simulator.py  # Monte Carlo simulator
│   │   └── team_optimizer.py   # ILP team optimizer
│   ├── utils/
│   │   ├── name_resolver.py    # Name standardization
│   │   ├── validators.py       # Data validators
│   │   └── mlflow_client.py    # MLflow integration
│   └── streamlit_app/
│       ├── pages/
│       │   ├── 1_📊_Predictions.py
│       │   ├── 2_⚽_Team_Selector.py
│       │   ├── 3_📈_Match_Preview.py
│       │   ├── 4_🔄_Transfer_Suggestions.py
│       │   └── 5_🎯_Captain_Analysis.py
│       └── components/
├── tests/
│   ├── test_data.py
│   ├── test_features.py
│   ├── test_models.py
│   └── test_integration.py
├── data/
│   ├── raw/
│   │   ├── fpl_api/
│   │   ├── understat/
│   │   └── fbref/
│   ├── processed/
│   └── models/
├── mlruns/                     # MLflow local tracking (gitignored)
└── outputs/
    └── predictions_export/     # Local prediction exports (optional backup)
```

## Acceptance Criteria
- [ ] All directories created with fpl_simulation/ layout
- [ ] __init__.py files added to Python packages
- [ ] .gitkeep files added to empty directories
- [ ] Directory structure documented in README
- [ ] mlruns/ added to .gitignore

## Definition of Done
- [ ] Code implemented and follows project conventions
- [ ] Unit tests written and passing (>80% coverage for this component)
- [ ] Type hints added (100% for public APIs)
- [ ] Code reviewed by reviewer
- [ ] Documentation updated
- [ ] Integrated into main pipeline


## Agent
devops

## Status
Done

## Progress Log

### 2026-04-01 — DevOps Review
Reviewed directory structure and .gitignore against scope requirements:
- All directories exist: src/data/, src/features/, src/models/, src/utils/, src/streamlit_app/pages/, src/streamlit_app/components/, tests/, data/raw/{fpl_api,understat,fbref}, data/processed, data/models, data/migrations, .github/workflows, .streamlit, outputs/predictions_export, mlruns ✓
- __init__.py files present in all packages ✓
- .gitkeep files in empty directories ✓
- .gitignore includes: mlruns/, .env, .streamlit/secrets.toml, __pycache__/, *.pyc, .mypy_cache/, .ruff_cache/, .pytest_cache/ ✓
- data/models/*.joblib is NOT gitignored (required at Streamlit Cloud runtime) ✓

### 2026-04-01 21:00:00 Quality review passed. All checks green. Ticket closed.

## Comments
[Agents can add questions, blockers, or notes here]
