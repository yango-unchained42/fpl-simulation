"""Tests for data validation pipeline."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import polars as pl

from src.data.validate import (
    ValidationIssue,
    ValidationReport,
    run_validation,
    validate_completeness,
    validate_consistency,
    validate_ranges,
    validate_schema,
    validate_uniqueness,
)


class TestValidationReport:
    """Tests for ValidationReport dataclass."""

    def test_is_valid_with_no_errors(self) -> None:
        """Test is_valid returns True when no errors."""
        report = ValidationReport(
            issues=[ValidationIssue("check", "col", "warning", "msg")]
        )
        assert report.is_valid is True

    def test_is_valid_with_errors(self) -> None:
        """Test is_valid returns False when errors exist."""
        report = ValidationReport(
            issues=[ValidationIssue("check", "col", "error", "msg")]
        )
        assert report.is_valid is False

    def test_summary_format(self) -> None:
        """Test summary generates readable output."""
        report = ValidationReport(
            issues=[ValidationIssue("range", "goals", "error", "negative")],
            passed_checks=["schema"],
            total_rows=100,
        )
        summary = report.summary()
        assert "Validation Report" in summary
        assert "100" in summary
        assert "errors" in summary.lower()


class TestValidateSchema:
    """Tests for schema validation."""

    def test_missing_required_column(self) -> None:
        """Test detection of missing required columns."""
        df = pl.DataFrame({"a": [1]})
        issues = validate_schema(df, ["a", "b"])
        assert len(issues) == 1
        assert issues[0].severity == "error"
        assert "b" in issues[0].message

    def test_all_required_present(self) -> None:
        """Test no issues when all required columns present."""
        df = pl.DataFrame({"a": [1], "b": [2]})
        issues = validate_schema(df, ["a", "b"])
        assert len(issues) == 0

    def test_type_mismatch_warning(self) -> None:
        """Test type mismatch generates warning."""
        df = pl.DataFrame({"id": [1, 2, 3]})
        issues = validate_schema(df, ["id"], expected_types={"id": pl.String()})
        assert len(issues) == 1
        assert issues[0].severity == "warning"

    def test_type_match_no_issue(self) -> None:
        """Test matching types generate no issue."""
        df = pl.DataFrame({"id": [1, 2, 3]})
        issues = validate_schema(df, ["id"], expected_types={"id": pl.Int64()})
        assert len(issues) == 0


class TestValidateRanges:
    """Tests for range validation."""

    def test_values_within_range(self) -> None:
        """Test no issues when values are within range."""
        df = pl.DataFrame({"value": [1, 2, 3, 4, 5]})
        issues = validate_ranges(df, {"value": (0, 10)})
        assert len(issues) == 0

    def test_values_below_min(self) -> None:
        """Test detection of values below minimum."""
        df = pl.DataFrame({"value": [-1, 2, 3]})
        issues = validate_ranges(df, {"value": (0, None)})
        assert len(issues) == 1
        assert issues[0].severity == "error"

    def test_values_above_max(self) -> None:
        """Test detection of values above maximum."""
        df = pl.DataFrame({"gw": [1, 2, 40]})
        issues = validate_ranges(df, {"gw": (1, 38)})
        assert len(issues) == 1
        assert "40" in issues[0].message or "38" in issues[0].message

    def test_missing_column_skipped(self) -> None:
        """Test missing columns are skipped."""
        df = pl.DataFrame({"a": [1]})
        issues = validate_ranges(df, {"nonexistent": (0, 10)})
        assert len(issues) == 0


class TestValidateConsistency:
    """Tests for consistency validation."""

    def test_implies_rule_violation(self) -> None:
        """Test detection of implies rule violations."""
        df = pl.DataFrame({"minutes": [90, 0, None], "points": [6, 0, None]})
        # When minutes is set, points should also be set
        issues = validate_consistency(
            df, [{"type": "implies", "when": "minutes", "then": "points"}]
        )
        # Row with minutes=None should not trigger
        assert len(issues) == 0

    def test_implies_rule_violation_detected(self) -> None:
        """Test detection when when_col is set but then_col is null."""
        df = pl.DataFrame({"minutes": [90, 90, None], "points": [6, None, None]})
        issues = validate_consistency(
            df, [{"type": "implies", "when": "minutes", "then": "points"}]
        )
        assert len(issues) == 1
        assert issues[0].row_count == 1

    def test_non_negative_violation(self) -> None:
        """Test detection of negative values."""
        df = pl.DataFrame({"goals": [1, -1, 2]})
        issues = validate_consistency(
            df, [{"type": "non_negative", "columns": ["goals"]}]
        )
        assert len(issues) == 1
        assert issues[0].severity == "error"

    def test_non_negative_passes(self) -> None:
        """Test no issues when all values are non-negative."""
        df = pl.DataFrame({"goals": [0, 1, 2]})
        issues = validate_consistency(
            df, [{"type": "non_negative", "columns": ["goals"]}]
        )
        assert len(issues) == 0


class TestValidateCompleteness:
    """Tests for completeness validation."""

    def test_complete_data_passes(self) -> None:
        """Test complete data passes validation."""
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        issues = validate_completeness(df, min_completeness=0.95)
        assert len(issues) == 0

    def test_critical_column_null_fails(self) -> None:
        """Test null in critical column generates error."""
        df = pl.DataFrame({"id": [1, None, 3]})
        issues = validate_completeness(df, critical_columns=["id"])
        assert len(issues) == 1
        assert issues[0].severity == "error"

    def test_below_threshold_warning(self) -> None:
        """Test below-threshold completeness generates warning."""
        df = pl.DataFrame({"value": [1, None, None, None]})
        issues = validate_completeness(df, min_completeness=0.5)
        assert len(issues) == 1
        assert issues[0].severity == "warning"


class TestValidateUniqueness:
    """Tests for uniqueness validation."""

    def test_unique_values_pass(self) -> None:
        """Test unique values pass validation."""
        df = pl.DataFrame({"id": [1, 2, 3]})
        issues = validate_uniqueness(df, unique_columns=["id"])
        assert len(issues) == 0

    def test_duplicate_values_fail(self) -> None:
        """Test duplicate values are detected."""
        df = pl.DataFrame({"id": [1, 1, 2]})
        issues = validate_uniqueness(df, unique_columns=["id"])
        assert len(issues) == 1
        assert issues[0].severity == "error"

    def test_composite_key_duplicate(self) -> None:
        """Test composite key duplicates are detected."""
        df = pl.DataFrame({"player_id": [1, 1, 2], "gameweek": [1, 1, 1]})
        issues = validate_uniqueness(df, composite_keys=[["player_id", "gameweek"]])
        assert len(issues) == 1

    def test_composite_key_unique(self) -> None:
        """Test unique composite keys pass."""
        df = pl.DataFrame({"player_id": [1, 1, 2], "gameweek": [1, 2, 1]})
        issues = validate_uniqueness(df, composite_keys=[["player_id", "gameweek"]])
        assert len(issues) == 0


class TestRunValidation:
    """Tests for the full validation pipeline."""

    def test_valid_data_passes_all_checks(self) -> None:
        """Test valid data passes all validation checks."""
        df = pl.DataFrame(
            {
                "player_id": [1, 2, 3],
                "gameweek": [1, 2, 3],
                "goals": [1, 0, 2],
            }
        )
        config = {
            "required_columns": ["player_id", "gameweek"],
            "range_checks": {"gameweek": (1, 38), "goals": (0, None)},
            "consistency_rules": [{"type": "non_negative", "columns": ["goals"]}],
            "unique_columns": ["player_id"],
        }
        report = run_validation(df, config, log_to_mlflow=False)
        assert report.is_valid is True
        assert report.error_count == 0

    def test_invalid_data_fails(self) -> None:
        """Test invalid data fails validation."""
        df = pl.DataFrame(
            {
                "player_id": [1, 1, 3],  # duplicate
                "gameweek": [1, 2, 40],  # out of range
                "goals": [-1, 0, 2],  # negative
            }
        )
        config = {
            "required_columns": ["player_id", "gameweek"],
            "range_checks": {"gameweek": (1, 38), "goals": (0, None)},
            "consistency_rules": [{"type": "non_negative", "columns": ["goals"]}],
            "unique_columns": ["player_id"],
        }
        report = run_validation(df, config, log_to_mlflow=False)
        assert report.is_valid is False
        assert report.error_count > 0

    def test_logs_to_mlflow_when_enabled(self) -> None:
        """Test that results are logged to MLflow when enabled."""
        mock_mlflow = MagicMock()
        mock_mlflow.start_run.return_value.__enter__ = MagicMock()
        mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

        df = pl.DataFrame({"id": [1, 2, 3]})
        config = {"required_columns": ["id"]}

        with patch("src.utils.mlflow_client._get_mlflow", return_value=mock_mlflow):
            run_validation(df, config, log_to_mlflow=True)

        mock_mlflow.log_param.assert_called()

    def test_skips_mlflow_when_disabled(self) -> None:
        """Test that MLflow logging is skipped when disabled."""
        df = pl.DataFrame({"id": [1, 2, 3]})
        config = {"required_columns": ["id"]}

        with patch("src.utils.mlflow_client._get_mlflow") as mock_get:
            run_validation(df, config, log_to_mlflow=False)
            mock_get.assert_not_called()
