"""Comprehensive data validation pipeline.

Implements schema, range, consistency, completeness, and uniqueness
validation checks for FPL data. Generates validation reports, alerts
on failures, and logs metrics to MLflow.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)


@dataclass
class ValidationIssue:
    """A single validation issue."""

    check: str
    column: str
    severity: str  # "error", "warning", "info"
    message: str
    row_count: int = 0


@dataclass
class ValidationReport:
    """Report summarizing all validation results."""

    issues: list[ValidationIssue] = field(default_factory=list)
    passed_checks: list[str] = field(default_factory=list)
    total_rows: int = 0

    @property
    def is_valid(self) -> bool:
        """Whether the data passed all error-level checks."""
        return not any(i.severity == "error" for i in self.issues)

    @property
    def error_count(self) -> int:
        """Number of error-level issues."""
        return sum(1 for i in self.issues if i.severity == "error")

    @property
    def warning_count(self) -> int:
        """Number of warning-level issues."""
        return sum(1 for i in self.issues if i.severity == "warning")

    def summary(self) -> str:
        """Generate a human-readable summary."""
        lines = [
            "Validation Report",
            "=" * 40,
            f"  Total rows:     {self.total_rows}",
            f"  Passed checks:  {len(self.passed_checks)}",
            f"  Errors:         {self.error_count}",
            f"  Warnings:       {self.warning_count}",
            f"  Valid:          {self.is_valid}",
        ]
        if self.issues:
            lines.append("\nIssues:")
            for issue in self.issues:
                lines.append(
                    f"  [{issue.severity.upper()}] {issue.check}: "
                    f"{issue.column} - {issue.message}"
                )
        return "\n".join(lines)


def validate_schema(
    df: pl.DataFrame,
    required_columns: list[str],
    expected_types: dict[str, pl.DataType] | None = None,
) -> list[ValidationIssue]:
    """Validate that DataFrame has required columns with expected types.

    Args:
        df: Input DataFrame.
        required_columns: List of required column names.
        expected_types: Optional dict mapping column names to expected types.

    Returns:
        List of validation issues.
    """
    issues: list[ValidationIssue] = []

    for col in required_columns:
        if col not in df.columns:
            issues.append(
                ValidationIssue(
                    check="schema",
                    column=col,
                    severity="error",
                    message=f"Missing required column: {col}",
                )
            )

    if expected_types:
        for col, expected_type in expected_types.items():
            if col in df.columns:
                actual_type = df.schema[col]
                if actual_type != expected_type:
                    issues.append(
                        ValidationIssue(
                            check="schema",
                            column=col,
                            severity="warning",
                            message=f"Expected {expected_type}, got {actual_type}",
                        )
                    )

    return issues


def validate_ranges(
    df: pl.DataFrame,
    range_checks: dict[str, tuple[float | None, float | None]],
) -> list[ValidationIssue]:
    """Validate that column values are within specified ranges.

    Args:
        df: Input DataFrame.
        range_checks: Dict mapping column names to (min, max) tuples.
            Use None for unbounded (e.g., (0, None) means >= 0).

    Returns:
        List of validation issues.
    """
    issues: list[ValidationIssue] = []

    for col, (min_val, max_val) in range_checks.items():
        if col not in df.columns:
            continue

        col_data = df[col].drop_nulls()
        if col_data.is_empty():
            continue

        violations: list[str] = []
        if min_val is not None:
            below = col_data.filter(col_data < min_val)
            if not below.is_empty():
                violations.append(f"{below.shape[0]} values < {min_val}")

        if max_val is not None:
            above = col_data.filter(col_data > max_val)
            if not above.is_empty():
                violations.append(f"{above.shape[0]} values > {max_val}")

        if violations:
            issues.append(
                ValidationIssue(
                    check="range",
                    column=col,
                    severity="error",
                    message="; ".join(violations),
                    row_count=sum(int(v.split()[0]) for v in violations),
                )
            )

    return issues


def validate_consistency(
    df: pl.DataFrame,
    rules: list[dict[str, Any]],
) -> list[ValidationIssue]:
    """Validate cross-field consistency rules.

    Args:
        df: Input DataFrame.
        rules: List of rule dicts. Supported rule types:
            - {"type": "implies", "when": col_a, "then": col_b}
              When col_a is not null, col_b must also be not null.
            - {"type": "sum_check", "columns": [a, b], "equals": c}
              a + b should equal c.
            - {"type": "non_negative", "columns": [a, b]}
              All specified columns must be >= 0.

    Returns:
        List of validation issues.
    """
    issues: list[ValidationIssue] = []

    for rule in rules:
        rule_type = rule.get("type")

        if rule_type == "implies":
            when_col = rule["when"]
            then_col = rule["then"]
            if when_col not in df.columns or then_col not in df.columns:
                continue

            when_not_null = df.filter(pl.col(when_col).is_not_null())
            if not when_not_null.is_empty():
                then_null = when_not_null.filter(pl.col(then_col).is_null())
                if not then_null.is_empty():
                    issues.append(
                        ValidationIssue(
                            check="consistency",
                            column=then_col,
                            severity="warning",
                            message=f"{then_null.shape[0]} rows where "
                            f"{when_col} is set but {then_col} is null",
                            row_count=then_null.shape[0],
                        )
                    )

        elif rule_type == "sum_check":
            columns = rule["columns"]
            equals_col = rule["equals"]
            if not all(c in df.columns for c in columns + [equals_col]):
                continue

            expected = sum(pl.col(c) for c in columns)
            diff = (df[equals_col] - expected).abs()
            violations = diff.filter(diff > 0.01)
            if not violations.is_empty():
                issues.append(
                    ValidationIssue(
                        check="consistency",
                        column=equals_col,
                        severity="warning",
                        message=f"Sum of {columns} doesn't match {equals_col} "
                        f"in {violations.shape[0]} rows",
                        row_count=violations.shape[0],
                    )
                )

        elif rule_type == "non_negative":
            columns = rule["columns"]
            for col in columns:
                if col not in df.columns:
                    continue
                negatives = df.filter(pl.col(col) < 0)
                if not negatives.is_empty():
                    issues.append(
                        ValidationIssue(
                            check="consistency",
                            column=col,
                            severity="error",
                            message=f"{negatives.shape[0]} negative values",
                            row_count=negatives.shape[0],
                        )
                    )

    return issues


def validate_completeness(
    df: pl.DataFrame,
    min_completeness: float = 0.95,
    critical_columns: list[str] | None = None,
) -> list[ValidationIssue]:
    """Validate data completeness (non-null ratios).

    Args:
        df: Input DataFrame.
        min_completeness: Minimum non-null ratio for non-critical columns.
        critical_columns: Columns that must be 100% complete.

    Returns:
        List of validation issues.
    """
    issues: list[ValidationIssue] = []
    critical_set = set(critical_columns or [])

    for col in df.columns:
        non_null = df.select(pl.col(col).drop_nulls()).height
        ratio = non_null / max(df.height, 1)

        if col in critical_set:
            if ratio < 1.0:
                missing = df.height - non_null
                issues.append(
                    ValidationIssue(
                        check="completeness",
                        column=col,
                        severity="error",
                        message=f"Critical column: {missing} null values "
                        f"({(1 - ratio) * 100:.1f}%)",
                        row_count=missing,
                    )
                )
        elif ratio < min_completeness:
            missing = df.height - non_null
            issues.append(
                ValidationIssue(
                    check="completeness",
                    column=col,
                    severity="warning",
                    message=f"Completeness {ratio:.1%} < {min_completeness:.1%} "
                    f"({missing} null values)",
                    row_count=missing,
                )
            )

    return issues


def validate_uniqueness(
    df: pl.DataFrame,
    unique_columns: list[str] | None = None,
    composite_keys: list[list[str]] | None = None,
) -> list[ValidationIssue]:
    """Validate uniqueness constraints.

    Args:
        df: Input DataFrame.
        unique_columns: Columns that must have unique values.
        composite_keys: Lists of columns that form composite unique keys.

    Returns:
        List of validation issues.
    """
    issues: list[ValidationIssue] = []

    for col in unique_columns or []:
        if col not in df.columns:
            continue
        dupes = df.filter(pl.col(col).is_duplicated())
        if not dupes.is_empty():
            issues.append(
                ValidationIssue(
                    check="uniqueness",
                    column=col,
                    severity="error",
                    message=f"{dupes.shape[0]} duplicate values in {col}",
                    row_count=dupes.shape[0],
                )
            )

    for key in composite_keys or []:
        valid_keys = [k for k in key if k in df.columns]
        if len(valid_keys) < 2:
            continue
        dupes = df.filter(pl.struct(valid_keys).is_duplicated())
        if not dupes.is_empty():
            issues.append(
                ValidationIssue(
                    check="uniqueness",
                    column=",".join(valid_keys),
                    severity="error",
                    message=f"{dupes.shape[0]} duplicate composite keys "
                    f"({','.join(valid_keys)})",
                    row_count=dupes.shape[0],
                )
            )

    return issues


def run_validation(
    df: pl.DataFrame,
    config: dict[str, Any],
    log_to_mlflow: bool = True,
) -> ValidationReport:
    """Run full validation pipeline on a DataFrame.

    Args:
        df: Input DataFrame to validate.
        config: Validation configuration dict with keys:
            - required_columns: list[str]
            - expected_types: dict[str, pl.DataType] (optional)
            - range_checks: dict[str, tuple[float|None, float|None]] (optional)
            - consistency_rules: list[dict] (optional)
            - min_completeness: float (default 0.95)
            - critical_columns: list[str] (optional)
            - unique_columns: list[str] (optional)
            - composite_keys: list[list[str]] (optional)
        log_to_mlflow: Whether to log results to MLflow.

    Returns:
        ValidationReport with all issues and passed checks.
    """
    report = ValidationReport(total_rows=df.height)
    all_issues: list[ValidationIssue] = []

    # Schema validation
    schema_issues = validate_schema(
        df,
        config.get("required_columns", []),
        config.get("expected_types"),
    )
    all_issues.extend(schema_issues)
    if not schema_issues:
        report.passed_checks.append("schema")

    # Range validation
    range_checks = config.get("range_checks", {})
    if range_checks:
        range_issues = validate_ranges(df, range_checks)
        all_issues.extend(range_issues)
        if not range_issues:
            report.passed_checks.append("range")

    # Consistency validation
    consistency_rules = config.get("consistency_rules", [])
    if consistency_rules:
        consistency_issues = validate_consistency(df, consistency_rules)
        all_issues.extend(consistency_issues)
        if not consistency_issues:
            report.passed_checks.append("consistency")

    # Completeness validation
    completeness_issues = validate_completeness(
        df,
        min_completeness=config.get("min_completeness", 0.95),
        critical_columns=config.get("critical_columns"),
    )
    all_issues.extend(completeness_issues)
    if not completeness_issues:
        report.passed_checks.append("completeness")

    # Uniqueness validation
    uniqueness_issues = validate_uniqueness(
        df,
        unique_columns=config.get("unique_columns"),
        composite_keys=config.get("composite_keys"),
    )
    all_issues.extend(uniqueness_issues)
    if not uniqueness_issues:
        report.passed_checks.append("uniqueness")

    report.issues = all_issues

    # Alert on errors
    if report.error_count > 0:
        logger.error(
            "Validation FAILED: %d errors, %d warnings",
            report.error_count,
            report.warning_count,
        )
    elif report.warning_count > 0:
        logger.warning(
            "Validation passed with warnings: %d warnings",
            report.warning_count,
        )
    else:
        logger.info("Validation passed: all checks OK")

    logger.info("\n%s", report.summary())

    if log_to_mlflow:
        _log_validation_to_mlflow(report)

    return report


def _log_validation_to_mlflow(report: ValidationReport) -> None:
    """Log validation results to MLflow.

    Args:
        report: ValidationReport with validation results.
    """
    try:
        from src.utils.mlflow_client import _get_mlflow

        mlflow = _get_mlflow()
        if mlflow is None:
            logger.debug("MLflow not available, skipping validation logging")
            return

        mlflow.set_tracking_uri("mlruns")
        mlflow.set_experiment("fpl_data_cleaning")
        with mlflow.start_run(run_name="data_validation"):
            mlflow.log_param("total_rows", report.total_rows)
            mlflow.log_param("passed_checks", len(report.passed_checks))
            mlflow.log_param("error_count", report.error_count)
            mlflow.log_param("warning_count", report.warning_count)
            mlflow.log_param("is_valid", report.is_valid)
            if report.issues:
                mlflow.log_param(
                    "issues_summary",
                    str(
                        [
                            {
                                "check": i.check,
                                "column": i.column,
                                "severity": i.severity,
                            }
                            for i in report.issues[:20]
                        ]
                    ),
                )
    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to log validation to MLflow: %s", e)
