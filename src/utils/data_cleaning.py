"""Data cleaning utilities for Bronze to Silver transformations.

This module provides reusable cleaning functions for FPL data.
"""

from __future__ import annotations

from typing import Any


# Fields that should be integers
INT_FIELDS = {
    "gw": [
        "total_points",
        "minutes",
        "goals_scored",
        "assists",
        "clean_sheets",
        "goals_conceded",
        "own_goals",
        "penalties_saved",
        "penalties_missed",
        "yellow_cards",
        "red_cards",
        "saves",
        "bonus",
        "bps",
        "starts",
        "value",
        "transfers_balance",
        "selected",
        "transfers_in",
        "transfers_out",
        "team_h_score",
        "team_a_score",
        "round",
        "clearances_blocks_interceptions",
        "recoveries",
        "tackles",
    ],
    "vaastav_gw": [
        "player_id",
        "gameweek",
        "minutes",
        "goals_scored",
        "assists",
        "clean_sheets",
        "goals_conceded",
        "total_points",
        "bonus",
        "bps",
        "saves",
        "starts",
        "own_goals",
        "penalties_missed",
        "penalties_saved",
        "yellow_cards",
        "red_cards",
        "creativity",
        "influence",
        "threat",
        "ict_index",
        "transfers_in",
        "transfers_out",
        "value",
        "selected",
        "fixture",
        "team_a_score",
        "team_h_score",
    ],
    "player_state": [
        "now_cost",
        "chance_of_playing_next_round",
        "chance_of_playing_this_round",
        "corners_and_indirect_freekicks_order",
        "direct_freekicks_order",
        "penalties_order",
        "transfers_in",
        "transfers_out",
    ],
}

# Fields that should be floats
FLOAT_FIELDS = {
    "gw": [
        "influence",
        "creativity",
        "threat",
        "ict_index",
        "defensive_contribution",
        "expected_goals",
        "expected_assists",
        "expected_goal_involvements",
        "expected_goals_conceded",
    ],
    "vaastav_gw": [
        "expected_goals",
        "expected_assists",
        "expected_goal_involvements",
        "expected_goals_conceded",
        "xp",
    ],
    "player_state": [],
}

# Critical fields that must not be null
CRITICAL_FIELDS = {
    "gw": ["element", "round", "season", "total_points"],
    "player_state": ["id", "season", "gameweek"],
    "vaastav_gw": ["player_id", "gameweek", "season", "total_points"],
}


def clean_numeric_fields(
    record: dict[str, Any], category: str = "gw"
) -> dict[str, Any]:
    """Clean numeric fields to proper types."""
    for field in INT_FIELDS.get(category, []):
        if field in record and record[field] is not None:
            try:
                record[field] = int(record[field])
            except (ValueError, TypeError):
                record[field] = None

    for field in FLOAT_FIELDS.get(category, []):
        if field in record and record[field] is not None:
            try:
                record[field] = float(record[field])
            except (ValueError, TypeError):
                record[field] = None

    return record


def validate_ranges(record: dict[str, Any]) -> dict[str, Any]:
    """Validate and clamp numeric fields to reasonable ranges."""
    # Minutes should not exceed 90
    if record.get("minutes") is not None and record["minutes"] > 90:
        record["minutes"] = 90

    # Goals should not be negative
    if record.get("goals_scored") is not None and record["goals_scored"] < 0:
        record["goals_scored"] = 0

    if record.get("goals_conceded") is not None and record["goals_conceded"] < 0:
        record["goals_conceded"] = 0

    if record.get("assists") is not None and record["assists"] < 0:
        record["assists"] = 0

    # Yellow/red cards should not be negative
    if record.get("yellow_cards") is not None and record["yellow_cards"] < 0:
        record["yellow_cards"] = 0

    if record.get("red_cards") is not None and record["red_cards"] < 0:
        record["red_cards"] = 0

    return record


def add_quality_flags(record: dict[str, Any], category: str = "gw") -> dict[str, Any]:
    """Add data quality flags to a record."""
    critical = CRITICAL_FIELDS.get(category, [])

    missing_fields = []
    for field in critical:
        if record.get(field) is None:
            missing_fields.append(field)

    # Calculate quality score
    total_fields = len(record)
    non_null_fields = sum(1 for v in record.values() if v is not None)
    quality_score = non_null_fields / total_fields if total_fields > 0 else 0.0

    record["data_quality_score"] = round(quality_score, 3)
    record["is_incomplete"] = len(missing_fields) > 0
    record["missing_fields"] = missing_fields if missing_fields else []

    return record


def clean_and_flag_record(
    record: dict[str, Any], category: str = "gw"
) -> dict[str, Any]:
    """Apply full cleaning pipeline to a record."""
    record = clean_numeric_fields(record, category)
    record = validate_ranges(record)
    record = add_quality_flags(record, category)
    return record
