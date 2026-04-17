# FPL Prediction Pipeline

A comprehensive Fantasy Premier League (FPL) prediction pipeline using modern data engineering, machine learning, and Monte Carlo simulations.

## 🎯 Overview

This system ingests data from multiple sources (FPL API, vaastav, Understat), engineers features, trains predictive models, and provides an interactive Streamlit dashboard for FPL managers.

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- pip

### Installation

```bash
# Clone the repository
git clone https://github.com/your-username/fpl_simulation.git
cd fpl_simulation

# Install dependencies
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install
```

### Configuration

Copy the secrets template and fill in your credentials:

```bash
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
```

### Running the Pipeline

```bash
# Run the full pipeline
python main.py

# Run tests
pytest

# Run tests with coverage
pytest --cov=src --cov-report=term-missing
```

### Running the Streamlit Dashboard

```bash
streamlit run src/streamlit_app/pages/1_Predictions.py
```

## 📁 Project Structure

```
fpl_simulation/
├── src/
│   ├── data/           — Data ingestion, cleaning, merging, Supabase writes
│   ├── features/       — Rolling stats, H2H metrics, form index
│   ├── models/         — LightGBM predictor, Monte Carlo simulator, ILP optimizer
│   ├── utils/          — Name resolver, validators, MLflow client
│   └── streamlit_app/  — Streamlit dashboard pages and components
├── tests/              — Unit and integration tests
├── data/               — Raw, processed, and model artefact storage
├── .github/workflows/  — CI/CD and scheduled pipeline
└── data/migrations/    — Supabase SQL migrations
```

## 📊 Data Sources

| Source | Purpose | Frequency |
|--------|---------|-----------|
| FPL API | Player stats, prices, fixtures | Daily |
| vaastav/FPL | Historical per-GW FPL stats | One-time + annual refresh |
| Understat | xG, xA, match-level data | Historical + seasonal refresh |

## 🏗️ Tech Stack

- **Data Processing**: Polars, DuckDB
- **ML**: LightGBM, Scikit-learn, XGBoost
- **Optimization**: PuLP (ILP)
- **Simulation**: NumPy (Monte Carlo)
- **Database**: Supabase (Postgres)
- **UI**: Streamlit 1.30+
- **Code Quality**: Ruff, Black, MyPy, Pandera

## 📈 Model Performance Targets

| Metric | Target |
|--------|--------|
| Player Points MAE | < 2.0 points |
| Goals RMSE | < 0.5 goals |
| Clean Sheet Accuracy | > 75% |
| Starting XI Accuracy | > 80% |
| Test Coverage | > 80% |

## 📄 License

MIT
