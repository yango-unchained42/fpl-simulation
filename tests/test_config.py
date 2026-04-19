"""Tests for the config module."""

from __future__ import annotations

import os
from pathlib import Path


def test_config_imports():
    """Test that config module imports successfully."""
    from src.config import (
        ALL_SEASONS,
        BATCH_SIZE,
        BUDGET,
        CURRENT_SEASON,
        CURRENT_SEASON_UNDERSTAT,
        DATA_DIR,
        FPL_API_BASE,
        MAX_PER_CLUB,
        MODEL_DIR,
        N_SIMULATIONS,
        POSITION_COUNTS,
        PROCESSED_DATA_DIR,
        PROJECT_ROOT,
        RAW_DATA_DIR,
        SQUAD_SIZE,
    )

    # Verify types
    assert isinstance(CURRENT_SEASON, str)
    assert "-" in CURRENT_SEASON  # e.g. "2025-26"
    assert isinstance(CURRENT_SEASON_UNDERSTAT, str)
    assert "/" in CURRENT_SEASON_UNDERSTAT  # e.g. "2025/26"
    assert isinstance(ALL_SEASONS, list)
    assert len(ALL_SEASONS) >= 1
    assert isinstance(FPL_API_BASE, str)
    assert FPL_API_BASE.startswith("https://")
    assert isinstance(BATCH_SIZE, int)
    assert BATCH_SIZE > 0
    assert isinstance(BUDGET, float)
    assert BUDGET == 100.0
    assert isinstance(SQUAD_SIZE, int)
    assert SQUAD_SIZE == 15
    assert isinstance(MAX_PER_CLUB, int)
    assert MAX_PER_CLUB == 3
    assert isinstance(POSITION_COUNTS, dict)
    assert set(POSITION_COUNTS.keys()) == {"GK", "DEF", "MID", "FWD"}
    assert sum(POSITION_COUNTS.values()) == SQUAD_SIZE
    assert isinstance(N_SIMULATIONS, int)
    assert N_SIMULATIONS > 0
    assert isinstance(PROJECT_ROOT, Path)
    assert isinstance(DATA_DIR, Path)
    assert isinstance(RAW_DATA_DIR, Path)
    assert isinstance(PROCESSED_DATA_DIR, Path)
    assert isinstance(MODEL_DIR, Path)


def test_season_format():
    """Test season format consistency."""
    from src.config import ALL_SEASONS, CURRENT_SEASON, CURRENT_SEASON_UNDERSTAT

    # Current season dash format
    assert len(CURRENT_SEASON) == 7  # "2025-26"
    parts = CURRENT_SEASON.split("-")
    assert len(parts) == 2
    assert len(parts[0]) == 4
    assert len(parts[1]) == 2

    # Understat format
    assert "/" in CURRENT_SEASON_UNDERSTAT
    us_parts = CURRENT_SEASON_UNDERSTAT.split("/")
    assert len(us_parts) == 2

    # All seasons follow dash format
    for season in ALL_SEASONS:
        assert "-" in season, f"Season {season} missing dash"
        assert len(season) == 7, f"Season {season} wrong length"


def test_get_supabase_missing_env():
    """Test that get_supabase raises ValueError when env vars are missing."""

    # Save and clear env
    saved_url = os.environ.get("SUPABASE_URL", "")
    saved_key = os.environ.get("SUPABASE_KEY", "")

    try:
        os.environ["SUPABASE_URL"] = ""
        os.environ["SUPABASE_KEY"] = ""

        # Need to reload config to pick up empty env
        import importlib

        import src.config

        importlib.reload(src.config)
        from src.config import get_supabase as get_sb

        try:
            get_sb()
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "SUPABASE_URL" in str(e) or "SUPABASE_KEY" in str(e)
    finally:
        os.environ["SUPABASE_URL"] = saved_url
        os.environ["SUPABASE_KEY"] = saved_key


def test_fpl_constraints():
    """Test that FPL constraints are internally consistent."""
    from src.config import BUDGET, MAX_PER_CLUB, POSITION_COUNTS, SQUAD_SIZE

    # Squad size matches position counts
    assert sum(POSITION_COUNTS.values()) == SQUAD_SIZE

    # Budget is realistic
    assert BUDGET == 100.0

    # Max per club constraint
    assert MAX_PER_CLUB == 3
