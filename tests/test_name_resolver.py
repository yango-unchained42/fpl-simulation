"""Tests for name resolution and standardization utilities."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.utils.name_resolver import (
    KNOWN_VARIATIONS,
    _confidence_score,
    _levenshtein_distance,
    build_name_mapping,
    fuzzy_match_name,
    resolve_names,
    standardize_name,
)


class TestStandardizeName:
    """Tests for standardize_name function."""

    def test_basic_title_case(self) -> None:
        """Test basic title casing."""
        assert standardize_name("saka") == "Saka"
        assert standardize_name("SALAHA") == "Salaha"

    def test_strips_whitespace(self) -> None:
        """Test whitespace stripping."""
        assert standardize_name("  saka  ") == "Saka"

    def test_removes_parentheses(self) -> None:
        """Test removal of parenthetical suffixes."""
        assert standardize_name("Saka (Captain)") == "Saka"
        assert standardize_name("Salah (C)") == "Salah"

    def test_removes_brackets(self) -> None:
        """Test removal of bracketed suffixes."""
        assert standardize_name("Saka [Injured]") == "Saka"
        assert standardize_name("Salah [Doubtful]") == "Salah"

    def test_last_first_format(self) -> None:
        """Test conversion from 'Last, First' to 'First Last'."""
        assert standardize_name("Saka, Bukayo") == "Bukayo Saka"
        assert standardize_name("Salah, Mohamed") == "Mohamed Salah"

    def test_accented_characters(self) -> None:
        """Test normalization of accented characters."""
        result = standardize_name("José")
        assert result == "Jose"

    def test_complex_name(self) -> None:
        """Test complex name with multiple variations."""
        result = standardize_name("  haaland, erling (Captain)  ")
        assert result == "Erling Haaland"


class TestLevenshteinDistance:
    """Tests for Levenshtein distance calculation."""

    def test_identical_strings(self) -> None:
        """Test distance between identical strings."""
        assert _levenshtein_distance("saka", "saka") == 0

    def test_single_character_difference(self) -> None:
        """Test distance with single character difference."""
        assert _levenshtein_distance("saka", "baka") == 1

    def test_completely_different(self) -> None:
        """Test distance between completely different strings."""
        assert _levenshtein_distance("abc", "xyz") == 3

    def test_empty_string(self) -> None:
        """Test distance with empty string."""
        assert _levenshtein_distance("", "abc") == 3
        assert _levenshtein_distance("abc", "") == 3

    def test_insertion(self) -> None:
        """Test distance for insertion."""
        assert _levenshtein_distance("saka", "sakaa") == 1

    def test_deletion(self) -> None:
        """Test distance for deletion."""
        assert _levenshtein_distance("sakaa", "saka") == 1


class TestConfidenceScore:
    """Tests for confidence score calculation."""

    def test_perfect_match(self) -> None:
        """Test confidence for perfect match."""
        assert _confidence_score(0, 10) == 1.0

    def test_no_match(self) -> None:
        """Test confidence for completely different strings."""
        assert _confidence_score(10, 10) == 0.0

    def test_partial_match(self) -> None:
        """Test confidence for partial match."""
        score = _confidence_score(2, 10)
        assert score == pytest.approx(0.8)

    def test_empty_strings(self) -> None:
        """Test confidence for empty strings."""
        assert _confidence_score(0, 0) == 1.0


class TestFuzzyMatchName:
    """Tests for fuzzy_match_name function."""

    def test_exact_match(self) -> None:
        """Test exact name match."""
        candidates = ["Saka", "Salah", "Haaland"]
        match, score = fuzzy_match_name("Saka", candidates)
        assert match == "Saka"
        assert score == 1.0

    def test_fuzzy_match(self) -> None:
        """Test fuzzy name match."""
        candidates = ["Saka", "Salah", "Haaland"]
        match, score = fuzzy_match_name("Ska", candidates, threshold=0.5)
        assert match == "Saka"
        assert score > 0.5

    def test_no_match_below_threshold(self) -> None:
        """Test no match when below threshold."""
        candidates = ["Saka", "Salah", "Haaland"]
        match, score = fuzzy_match_name("Xylophone", candidates, threshold=0.9)
        assert match is None

    def test_case_insensitive(self) -> None:
        """Test case insensitive matching."""
        candidates = ["Saka", "Salah"]
        match, score = fuzzy_match_name("SAKA", candidates)
        assert match == "Saka"
        assert score == 1.0


class TestBuildNameMapping:
    """Tests for build_name_mapping function."""

    def test_known_variations(self) -> None:
        """Test mapping using known variations."""
        source = ["Alisson Ramses Becker", "Erling Braut Haaland"]
        target = ["Alisson", "Erling Haaland", "Salah"]
        mapping = build_name_mapping(source, target)

        assert mapping["Alisson Ramses Becker"] == ("Alisson", 1.0)
        assert mapping["Erling Braut Haaland"] == ("Erling Haaland", 1.0)

    def test_exact_normalized_match(self) -> None:
        """Test exact match on normalized names."""
        source = ["Saka", "Salah"]
        target = ["Saka", "Salah", "Haaland"]
        mapping = build_name_mapping(source, target)

        assert mapping["Saka"] == ("Saka", 1.0)
        assert mapping["Salah"] == ("Salah", 1.0)

    def test_fuzzy_fallback(self) -> None:
        """Test fuzzy matching as fallback."""
        source = ["Ska"]
        target = ["Saka", "Salah"]
        mapping = build_name_mapping(source, target, threshold=0.5)

        assert mapping["Ska"][0] == "Saka"
        assert mapping["Ska"][1] > 0.5

    def test_no_match_returns_original(self) -> None:
        """Test no match returns original name with low confidence."""
        source = ["Xylophone"]
        target = ["Saka", "Salah"]
        mapping = build_name_mapping(source, target, threshold=0.9)

        assert mapping["Xylophone"][0] == "Xylophone"
        assert mapping["Xylophone"][1] < 0.9

    def test_last_first_format(self) -> None:
        """Test mapping handles 'Last, First' format."""
        source = ["Saka, Bukayo"]
        target = ["Bukayo Saka", "Salah"]
        mapping = build_name_mapping(source, target)

        assert mapping["Saka, Bukayo"] == ("Bukayo Saka", 1.0)


class TestResolveNames:
    """Tests for resolve_names function."""

    def test_resolves_all_names(self) -> None:
        """Test that all names are resolved."""
        source = ["Saka", "Salah", "Haaland"]
        target = ["Saka", "Salah", "Haaland"]
        resolved, confidence = resolve_names(source, target, log_to_mlflow=False)

        assert resolved == {"Saka": "Saka", "Salah": "Salah", "Haaland": "Haaland"}
        assert all(s == 1.0 for s in confidence.values())

    def test_logs_low_confidence_warnings(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that low confidence matches generate warnings."""
        # "Sak" vs "Saka": distance=1, max_len=4, score=0.75
        # With threshold=0.9 in resolve_names but fuzzy_match threshold=0.8,
        # it will match with 0.75 confidence which is > 0.0 but < 0.9
        source = ["Sak"]
        target = ["Saka", "Salah"]
        with caplog.at_level("WARNING", logger="src.utils.name_resolver"):
            resolved, confidence = resolve_names(
                source, target, threshold=0.9, log_to_mlflow=False
            )

        assert "low-confidence" in caplog.text.lower()

    def test_logs_to_mlflow_when_enabled(self) -> None:
        """Test that results are logged to MLflow when enabled."""
        mock_mlflow = MagicMock()
        mock_mlflow.start_run.return_value.__enter__ = MagicMock()
        mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

        source = ["Saka", "Salah"]
        target = ["Saka", "Salah"]

        with patch("src.utils.mlflow_client._get_mlflow", return_value=mock_mlflow):
            resolve_names(source, target, log_to_mlflow=True)

        mock_mlflow.log_param.assert_called()
        mock_mlflow.log_metric.assert_called()

    def test_skips_mlflow_when_disabled(self) -> None:
        """Test that MLflow logging is skipped when disabled."""
        source = ["Saka", "Salah"]
        target = ["Saka", "Salah"]

        with patch("src.utils.mlflow_client._get_mlflow") as mock_get:
            resolve_names(source, target, log_to_mlflow=False)
            mock_get.assert_not_called()


class TestKnownVariations:
    """Tests for known name variations mapping."""

    def test_has_goalkeeper_variations(self) -> None:
        """Test that goalkeeper variations are defined."""
        assert "Alisson Ramses Becker" in KNOWN_VARIATIONS
        assert "Ederson Santana de Moraes" in KNOWN_VARIATIONS

    def test_has_outfield_variations(self) -> None:
        """Test that outfield player variations are defined."""
        assert "Erling Braut Haaland" in KNOWN_VARIATIONS
        assert "Mohamed Salah Hamed Mahrous Ghaly" in KNOWN_VARIATIONS

    def test_has_team_variations(self) -> None:
        """Test that team name variations are defined."""
        assert "Man City" in KNOWN_VARIATIONS
        assert "Spurs" in KNOWN_VARIATIONS
        assert "Wolves" in KNOWN_VARIATIONS
