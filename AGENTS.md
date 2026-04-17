# AGENTS.md

## Silver Layer Pipeline Architecture

Before working on Silver layer tasks, always review: `docs/SILVER_LAYER_PIPELINE.md`

This document defines the 3-step pipeline:
1. **Team Mapping** → `silver_team_mapping` (unified_team_id)
2. **Player Mapping** → `silver_player_mapping` (unified_player_id)  
3. **Match Mapping** → `silver_match_mapping` (match_id with team UUIDs)

## Agent Roles and Permissions

### Role: Build Agent
The `build` agent implements tickets and writes code.

**Allowed to do WITHOUT asking:**
- Code implementation and refactoring
- Running tests (`pytest`)
- Running linting/formatting (`ruff`, `black`, `mypy`)
- Installing Python packages from PyPI
- Reading files and exploring the codebase

**Must ALWAYS ask before doing:**
- Any database schema changes (CREATE, ALTER, DROP tables)
- Running SQL scripts that modify data
- Installing system packages (npm, brew, etc.)
- Deleting files or data
- Making commits to git
- Accessing secrets or credentials

**CRITICAL: No Adhoc Database Fixes**
- NEVER fix data issues by running direct SQL or Python scripts in the console
- ALL fixes must be implemented in the actual pipeline code
- If data is broken, fix the code that generates/transforms the data, not the data itself
- This ensures the daily pipeline will fix the issue permanently

Example of WRONG:
```python
# Bad: Fixing data directly in console
client.table('some_table').update({...}).execute()
```

Example of CORRECT:
```python
# Good: Fix the code that produces the data
# 1. Update the transform function in src/
# 2. Re-run the daily pipeline
# 3. Verify the fix is applied automatically
```

### Role: Gandalf (Review Agent)
Reviews completed work and sets tickets to "Done".

**Permission:**
- Can set ticket Status to Done
- Can reject work with feedback

---

## Build, Lint, and Test Commands

### Running Tests

```bash
# Run all tests
pytest

# Run a single test file
pytest tests/path/to/test_file.py

# Run a single test
pytest tests/path/to/test_file.py::test_function_name

# Run tests with coverage
pytest --cov=. --cov-report=html

# Run tests with verbose output
pytest -v
```

### Linting and Formatting

```bash
# Run Ruff (linting)
ruff check .

# Run Ruff with auto-fix
ruff check . --fix

# Format with Black
black .

# Type checking with MyPy
mypy src/

# Run all checks
ruff check . && mypy src/ && black . --check
```

### Building and Installing

```bash
# Install dependencies
pip install -e ".[dev]"

# Build package
python -m build

# Install pre-commit hooks
pre-commit install
```

## Code Style Guidelines

### Python Style

- **Formatter**: Black (default settings)
- **Linter**: Ruff (use `ruff check . --fix` for auto-fix)
- **Type Checker**: MyPy
- **Line Length**: 88 characters (Black default)

### Imports

- Group imports in the following order:
  1. Standard library imports
  2. Third-party imports
  3. Local application imports (relative)
- Use absolute imports preferred over relative
- Avoid wildcard imports (`from module import *`)
- Sort imports alphabetically within each group

```python
# Correct
import os
import sys

import pandas as pd
import polars as pl

from src.config import settings
from src.utils.helpers import process_data
```

### Naming Conventions

- **Classes**: `PascalCase` (e.g., `DataPipeline`, `FeatureEngineer`)
- **Functions**: `snake_case` (e.g., `load_fpl_data`, `calculate_expected_points`)
- **Variables**: `snake_case` (e.g., `player_data`, `team_strength`)
- **Constants**: `UPPER_SNAKE_CASE` (e.g., `MAX_PLAYERS`, `DEFAULT_SEASON`)
- **Type Variables**: `PascalCase` with `T` prefix (e.g., `TData`, `TResult`)

### Type Hints

- **Always add type hints** to function signatures
- Use `from __future__ import annotations` for forward references
- Prefer `list[T]`, `dict[K, V]`, `tuple[T, ...]` over `List[T]`, `Dict[K, V]`
- Use `Optional[T]` for nullable types, not `Union[T, None]`
- Use `Literal[T]` for explicit value constraints

```python
from __future__ import annotations

from typing import Literal

def calculate_gameweek_points(
    player_id: int,
    gameweek: int,
    season: Literal["2023-24", "2024-25"],
) -> float | None:
    ...
```

### Error Handling

- Use specific exception types, not bare `except`
- Raise appropriate exception types (`ValueError`, `TypeError`, `KeyError`)
- Provide meaningful error messages
- Use custom exceptions for domain-specific errors

```python
class DataValidationError(Exception):
    """Raised when FPL data validation fails."""
    pass

def validate_player_data(player: dict) -> None:
    if "element" not in player:
        raise DataValidationError("Missing 'element' field in player data")
```

### File Organization

- Place source code in `src/fpl_simulation/`
- Place tests in `tests/`
- Match directory structure between `src/` and `tests/`
- Name test files as `test_*.py` or `*_test.py`

### Documentation

- Use Google-style docstrings
- Include type hints in docstrings
- Document all public functions, classes, and modules
- Include examples for complex functions

```python
def calculate_expected_points(
    player_id: int,
    gameweek: int,
    season: str,
) -> float:
    """Calculate expected points for a player in a given gameweek.

    Args:
        player_id: The FPL element ID
        gameweek: The gameweek number (1-38)
        season: The season in format "YYYY-YY"

    Returns:
        Expected points value

    Raises:
        ValueError: If gameweek is not in range 1-38
    """
    ...
```

### Testing Guidelines

- Write unit tests for all public functions
- Write integration tests for data pipelines
- Use pytest fixtures for common test data
- Aim for >80% code coverage
- Test edge cases and error conditions

### Data Engineering Best Practices

- Use Polars for data manipulation (preferred over pandas)
- Use DuckDB for local data analysis
- Validate data schemas before processing
- Log data quality issues
- Use type hints for dataframes

### Supabase Data Loading Patterns

**Always use bulk fetch + transform approach:**

```python
from src.utils.supabase_utils import fetch_all_paginated
import polars as pl

# 1. Bulk fetch from Supabase (handles pagination automatically)
data = fetch_all_paginated(client, "table_name", select_cols="col1,col2")

# 2. Transform in memory (Polars for joins, dicts for lookups)
df = pl.DataFrame(data)

# 3. Join/transform
result_df = df.join(other_df, on="key")

# 4. Save back to Supabase (batch upsert)
for row in result_df.to_dicts():
    client.table("target_table").upsert(row).execute()
```

**Never do per-row API calls in loops** - this is slow and hits rate limits.

**Standard pattern for UUID resolution:**

```python
# 1. Fetch lookups in bulk
lookup_data = fetch_all_paginated(client, "mapping_table", select_cols="key_col,uuid_col")
lookup = {r["key_col"]: r["uuid_col"] for r in lookup_data}

# 2. Fetch data needing update
data = fetch_all_paginated(client, "target_table", select_cols="id,key_col")

# 3. Join in memory
updates = [r for r in data if lookup.get(r["key_col"])]

# 4. Batch update
for rec in updates:
    client.table("target_table").update({"uuid": lookup[r["key_col"]]).eq("id", rec["id"]).execute()
```

```python
import polars as pl

def load_player_data(season: str) -> pl.DataFrame:
    """Load player data for a season with schema validation."""
    df = pl.read_csv(f"data/raw/{season}/players.csv")
    
    expected_schema = {
        "element": pl.Int64,
        "web_name": pl.Utf8,
        "total_points": pl.Int64,
    }
    
    if df.schema != expected_schema:
        raise DataValidationError(f"Schema mismatch: {df.schema}")
    
    return df
```

### Frontend Guidelines (Streamlit)

- Use Streamlit Pages API for navigation
- Separate concerns: data loading, processing, and display
- Use caching for expensive operations (`@st.cache_data`)
- Provide user feedback for long-running operations

```python
import streamlit as st
import polars as pl

@st.cache_data
def load_predictions(gameweek: int) -> pl.DataFrame:
    """Load and cache predictions for a gameweek."""
    return pl.read_parquet(f"data/predictions/gw{gameweek}.parquet")
```

### Database Guidelines

- Use Supabase Postgres for production
- Use DuckDB for local development
- Write migrations for schema changes
- Validate database connections before operations

### Pre-commit Hooks

The project uses pre-commit with the following hooks:
- `ruff`: Linting
- `black`: Formatting
- `mypy`: Type checking
- `commitizen`: Commit message formatting

Run manually with:
```bash
pre-commit run --all-files
```

### Commit Message Convention

Use conventional commits:
```
<type>(<scope>): <description>

[optional body]

<type>: feat | fix | chore | docs | style | refactor | perf | test | build | ci | revert
<scope>: data | model | frontend | config | docs | etc.
```

Examples:
```
feat(data): add FPL player data loader
fix(model): correct expected points calculation
docs(readme): update installation instructions
chore: update dependencies
```
