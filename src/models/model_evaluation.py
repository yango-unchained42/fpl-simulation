"""Model evaluation and validation module.

Comprehensive evaluation of the LightGBM player predictor including
regression metrics, residual analysis, SHAP values, and baseline comparison.
"""

from __future__ import annotations

import logging
from typing import Any

import lightgbm as lgb
import numpy as np
import numpy.typing as npt
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    mean_squared_error,
    r2_score,
)

logger = logging.getLogger(__name__)


def compute_regression_metrics(
    y_true: npt.NDArray[np.float64],
    y_pred: npt.NDArray[np.float64],
) -> dict[str, float]:
    """Compute standard regression metrics.

    Args:
        y_true: Ground truth target values.
        y_pred: Predicted target values.

    Returns:
        Dict with RMSE, MAE, R², and MAPE.
    """
    # Avoid division by zero in MAPE
    mask = y_true != 0
    if mask.sum() == 0:
        mape = 0.0
    else:
        mape = mean_absolute_percentage_error(y_true[mask], y_pred[mask])

    metrics = {
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "r2": float(r2_score(y_true, y_pred)),
        "mape": float(mape),
    }

    logger.info(
        "Regression metrics: RMSE=%.3f, MAE=%.3f, R²=%.3f, MAPE=%.3f",
        metrics["rmse"],
        metrics["mae"],
        metrics["r2"],
        metrics["mape"],
    )

    return metrics


def compute_residuals(
    y_true: npt.NDArray[np.float64],
    y_pred: npt.NDArray[np.float64],
) -> dict[str, Any]:
    """Compute residual analysis statistics.

    Args:
        y_true: Ground truth target values.
        y_pred: Predicted target values.

    Returns:
        Dict with residuals, mean residual, std residual, and bias.
    """
    residuals = y_true - y_pred

    analysis = {
        "residuals": residuals,
        "mean_residual": float(np.mean(residuals)),
        "std_residual": float(np.std(residuals)),
        "median_residual": float(np.median(residuals)),
        "bias": float(np.mean(residuals)),  # Systematic bias
        "max_overprediction": float(np.min(residuals)),
        "max_underprediction": float(np.max(residuals)),
    }

    logger.info(
        "Residual analysis: mean=%.3f, std=%.3f, bias=%.3f",
        analysis["mean_residual"],
        analysis["std_residual"],
        analysis["bias"],
    )

    return analysis


def compute_error_by_category(
    y_true: npt.NDArray[np.float64],
    y_pred: npt.NDArray[np.float64],
    categories: npt.NDArray[Any],
) -> dict[str, dict[str, float]]:
    """Compute error metrics broken down by category (e.g., position, price range).

    Args:
        y_true: Ground truth target values.
        y_pred: Predicted target values.
        categories: Array of category labels for each sample.

    Returns:
        Dict mapping category to error metrics (MAE, RMSE, count).
    """
    unique_cats = np.unique(categories)
    results: dict[str, dict[str, float]] = {}

    for cat in unique_cats:
        mask = categories == cat
        if mask.sum() == 0:
            continue

        cat_true = y_true[mask]
        cat_pred = y_pred[mask]

        results[str(cat)] = {
            "mae": float(mean_absolute_error(cat_true, cat_pred)),
            "rmse": float(np.sqrt(mean_squared_error(cat_true, cat_pred))),
            "count": int(mask.sum()),
        }

    return results


def compute_feature_importance(
    model: lgb.LGBMRegressor,
    feature_names: list[str],
    top_n: int = 20,
    method: str = "gain",
) -> dict[str, float]:
    """Compute feature importance from LightGBM model.

    Args:
        model: Trained LightGBM model.
        feature_names: List of feature names.
        top_n: Number of top features to return.
        method: Importance method ('gain', 'split', or 'permutation').

    Returns:
        Dict mapping feature names to importance values.
    """
    if method == "gain":
        importances = model.booster_.feature_importance(importance_type="gain")
    elif method == "split":
        importances = model.booster_.feature_importance(importance_type="split")
    else:
        importances = model.feature_importances_

    importance_dict = dict(zip(feature_names, importances.tolist()))
    importance_dict = dict(
        sorted(importance_dict.items(), key=lambda x: x[1], reverse=True)
    )
    return dict(list(importance_dict.items())[:top_n])


def compute_shap_values(
    model: lgb.LGBMRegressor,
    x: npt.NDArray[np.float64],
    feature_names: list[str],
    sample_size: int | None = None,
) -> dict[str, Any]:
    """Compute SHAP values for model interpretability.

    Args:
        model: Trained LightGBM model.
        x: Feature matrix.
        feature_names: List of feature names.
        sample_size: Number of samples to use (None = all).

    Returns:
        Dict with SHAP values and summary statistics.
    """
    try:
        import shap  # noqa: F401
    except ImportError:
        logger.warning("shap not installed, skipping SHAP analysis")
        return {}

    # Use a subset for efficiency
    if sample_size is not None and x.shape[0] > sample_size:
        indices = np.random.choice(x.shape[0], sample_size, replace=False)
        x_sample = x[indices]
    else:
        x_sample = x

    # LightGBM has built-in SHAP support
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(x_sample)

    # Summary: mean absolute SHAP value per feature
    mean_abs_shap = np.abs(shap_values).mean(axis=0)
    importance = dict(zip(feature_names, mean_abs_shap.tolist()))
    importance = dict(sorted(importance.items(), key=lambda x: x[1], reverse=True))

    return {
        "shap_values": shap_values,
        "feature_importance": importance,
        "base_value": (
            float(explainer.expected_value)
            if hasattr(explainer.expected_value, "__float__")
            else float(explainer.expected_value[0])
        ),
    }


def compare_with_baseline(
    y_true: npt.NDArray[np.float64],
    y_pred: npt.NDArray[np.float64],
) -> dict[str, dict[str, float]]:
    """Compare model performance against simple baselines.

    Baselines:
    - Mean: predict the mean of training data
    - Last GW: predict the last known value (simulated as y_true with noise)

    Args:
        y_true: Ground truth target values.
        y_pred: Model predictions.

    Returns:
        Dict with metrics for model and each baseline.
    """
    results: dict[str, Any] = {}

    # Model metrics
    results["model"] = compute_regression_metrics(y_true, y_pred)

    # Mean baseline
    mean_pred = np.full_like(y_true, np.mean(y_true))
    results["mean_baseline"] = compute_regression_metrics(y_true, mean_pred)

    # Median baseline
    median_pred = np.full_like(y_true, np.median(y_true))
    results["median_baseline"] = compute_regression_metrics(y_true, median_pred)

    # Improvement over mean baseline
    model_mae = results["model"]["mae"]
    baseline_mae = results["mean_baseline"]["mae"]
    improvement = (baseline_mae - model_mae) / baseline_mae * 100

    logger.info(
        "Baseline comparison: Model MAE=%.3f, Mean Baseline MAE=%.3f, "
        "Improvement=%.1f%%",
        model_mae,
        baseline_mae,
        improvement,
    )

    results["improvement_over_mean_baseline_pct"] = improvement

    return results


def evaluate_model(
    model: lgb.LGBMRegressor,
    x_test: npt.NDArray[np.float64],
    y_test: npt.NDArray[np.float64],
    feature_names: list[str],
    test_categories: dict[str, npt.NDArray[Any]] | None = None,
    log_to_mlflow: bool = True,
) -> dict[str, Any]:
    """Comprehensive model evaluation.

    Args:
        model: Trained LightGBM model.
        x_test: Test feature matrix.
        y_test: Test target values.
        feature_names: List of feature names.
        test_categories: Optional dict of category arrays for error analysis.
        log_to_mlflow: Whether to log to MLflow.

    Returns:
        Dict with all evaluation results.
    """
    from src.models.lightgbm_model import predict_points

    # Predictions
    y_pred = predict_points(model, x_test)

    # Regression metrics
    metrics = compute_regression_metrics(y_test, y_pred)

    # Residual analysis
    residuals = compute_residuals(y_test, y_pred)

    # Error by category
    category_errors: dict[str, dict[str, dict[str, float]]] = {}
    if test_categories:
        for name, cats in test_categories.items():
            category_errors[name] = compute_error_by_category(y_test, y_pred, cats)

    # Feature importance
    importance = compute_feature_importance(model, feature_names)

    # Baseline comparison
    baseline_comparison = compare_with_baseline(y_test, y_pred)

    # SHAP values (optional, can be slow)
    shap_results = compute_shap_values(model, x_test, feature_names, sample_size=1000)

    results = {
        "metrics": metrics,
        "residuals": {k: v for k, v in residuals.items() if k != "residuals"},
        "category_errors": category_errors,
        "feature_importance": importance,
        "baseline_comparison": baseline_comparison,
        "shap_summary": shap_results.get("feature_importance", {}),
    }

    if log_to_mlflow:
        _log_evaluation_to_mlflow(results)

    return results


def _log_evaluation_to_mlflow(results: dict[str, Any]) -> None:
    """Log evaluation results to MLflow."""
    try:
        from src.utils.mlflow_client import _get_mlflow

        mlflow = _get_mlflow()
        if mlflow is None:
            logger.debug("MLflow not available, skipping evaluation logging")
            return

        mlflow.set_tracking_uri("mlruns")
        mlflow.set_experiment("fpl_model_training")
        with mlflow.start_run(run_name="model_evaluation"):
            # Log metrics
            for key, value in results["metrics"].items():
                mlflow.log_metric(f"test_{key}", value)

            # Log baseline comparison
            for key, value in results["baseline_comparison"].items():
                if isinstance(value, (int, float)):
                    mlflow.log_metric(f"baseline_{key}", value)

            # Log top feature importance
            for feat, imp in list(results["feature_importance"].items())[:10]:
                mlflow.log_metric(f"importance_{feat}", imp)

            # Log residual stats
            for key, value in results["residuals"].items():
                if isinstance(value, (int, float)):
                    mlflow.log_metric(f"residual_{key}", value)

    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to log evaluation to MLflow: %s", e)
