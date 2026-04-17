"""Tests for player mapping module."""

from __future__ import annotations

from src.silver.player_mapping import (
    EXACT_MATCH,
    HIGH_CONFIDENCE,
    LOW_CONFIDENCE,
    MEDIUM_CONFIDENCE,
    MIN_FUZZY_THRESHOLD,
    LAST_NAME_BOOST,
    TRANSFER_CONFIDENCE,
    _normalize_position,
)


def test_threshold_ordering():
    """Test that thresholds are in correct order."""
    assert EXACT_MATCH > HIGH_CONFIDENCE > MEDIUM_CONFIDENCE > LOW_CONFIDENCE > MIN_FUZZY_THRESHOLD
    assert EXACT_MATCH == 1.0
    assert MIN_FUZZY_THRESHOLD >= 0.0


def test_normalize_position():
    """Test position normalization."""
    # FPL positions
    assert _normalize_position("GKP") == "GKP"
    assert _normalize_position("DEF") == "DEF"
    assert _normalize_position("MID") == "MID"
    assert _normalize_position("FWD") == "FWD"

    # Understat positions
    assert _normalize_position("GK") == "GKP"
    assert _normalize_position("DC") == "DEF"
    assert _normalize_position("DL") == "DEF"
    assert _normalize_position("MC") == "MID"
    assert _normalize_position("FW") == "FWD"
    assert _normalize_position("AMC") == "FWD"
    assert _normalize_position("AML") == "FWD"

    # Edge cases
    assert _normalize_position(None) == ""
    assert _normalize_position("") == ""
    assert _normalize_position("Goalkeeper") == "GKP"


def test_boost_calculation():
    """Test that boost values are reasonable."""
    assert LAST_NAME_BOOST > 0
    assert LAST_NAME_BOOST < 1.0
    assert TRANSFER_CONFIDENCE > HIGH_CONFIDENCE
    assert TRANSFER_CONFIDENCE < EXACT_MATCH
