"""Tests for utility modules."""

from __future__ import annotations

import polars as pl
import pytest

from src.utils.name_resolver import build_name_mapping, standardize_name
from src.utils.validators import (
    check_data_completeness,
    validate_gameweek_range,
    validate_player_ids,
)


class TestStandardizeName:
    """Tests for name standardization."""

    def test_basic_standardization(self) -> None:
        assert standardize_name("saka") == "Saka"
        assert standardize_name("MOHAMED SALAH") == "Mohamed Salah"

    def test_removes_parentheses(self) -> None:
        assert standardize_name("Saka (Captain)") == "Saka"

    def test_removes_brackets(self) -> None:
        assert standardize_name("Salah [Injured]") == "Salah"

    def test_strips_whitespace(self) -> None:
        assert standardize_name("  haaland  ") == "Haaland"


class TestBuildNameMapping:
    """Tests for name mapping construction."""

    def test_exact_match(self) -> None:
        source = ["Bukayo Saka", "Mohamed Salah"]
        target = ["Bukayo Saka", "Mohamed Salah"]
        mapping = build_name_mapping(source, target)
        assert mapping["Bukayo Saka"] == ("Bukayo Saka", 1.0)
        assert mapping["Mohamed Salah"] == ("Mohamed Salah", 1.0)

    def test_case_insensitive(self) -> None:
        source = ["bukayo saka", "MOHAMED SALAH"]
        target = ["Bukayo Saka", "Mohamed Salah"]
        mapping = build_name_mapping(source, target)
        assert len(mapping) == 2
        assert mapping["bukayo saka"][1] == 1.0

    def test_no_match_returns_original(self) -> None:
        source = ["Unknown Player"]
        target = ["Bukayo Saka"]
        mapping = build_name_mapping(source, target)
        assert "Unknown Player" in mapping
        assert mapping["Unknown Player"][1] < 0.8


class TestValidatePlayerIds:
    """Tests for player ID validation."""

    def test_filters_invalid_ids(self) -> None:
        df = pl.DataFrame({"player_id": [1, 2, 999]})
        result = validate_player_ids(df, {1, 2})
        assert result.shape[0] == 2

    def test_no_player_id_column(self) -> None:
        df = pl.DataFrame({"name": ["Saka"]})
        result = validate_player_ids(df, {1})
        assert result.shape[0] == 1


class TestValidateGameweekRange:
    """Tests for gameweek range validation."""

    def test_filters_out_of_range(self) -> None:
        df = pl.DataFrame({"gameweek": [1, 20, 38, 39]})
        result = validate_gameweek_range(df)
        assert result.shape[0] == 3

    def test_no_gameweek_column(self) -> None:
        df = pl.DataFrame({"points": [5]})
        result = validate_gameweek_range(df)
        assert result.shape[0] == 1


class TestDataCompleteness:
    """Tests for data completeness checking."""

    def test_complete_data(self) -> None:
        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        result = check_data_completeness(df)
        assert result["a"] == 1.0
        assert result["b"] == 1.0

    def test_partial_completeness(self) -> None:
        df = pl.DataFrame({"a": [1, None, 3], "b": [4, 5, 6]})
        result = check_data_completeness(df)
        assert result["a"] == pytest.approx(2 / 3)
        assert result["b"] == 1.0

    def test_empty_dataframe(self) -> None:
        df = pl.DataFrame({"a": []})
        result = check_data_completeness(df)
        assert result["a"] == 0.0
