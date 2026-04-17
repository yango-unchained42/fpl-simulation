"""Starting XI model evaluation module.

Comprehensive evaluation of the Starting XI classifier including
classification metrics, confusion matrix, ROC/PR curves, probability
calibration, and SHAP values.
"""

from __future__ import annotations

import logging
from typing import Any

import lightgbm as lgb
import numpy as np
import numpy.typing as npt
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

logger = logging.getLogger(__name__)


def compute_classification_metrics(
    y_true: npt.NDArray[np.int64],
    y_pred: npt.NDArray[np.int64],
    y_proba: npt.NDArray[np.float64],
) -> dict[str, float]:
    """Compute standard classification metrics.

    Args:
        y_true: Ground truth labels.
        y_pred: Predicted labels.
        y_proba: Predicted probabilities for positive class.

    Returns:
        Dict with Accuracy, Precision, Recall, F1, and ROC-AUC.
    """
    metrics = {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_true, y_proba)),
    }

    logger.info(
        "Classification metrics: Accuracy=%.3f, Precision=%.3f, "
        "Recall=%.3f, F1=%.3f, ROC-AUC=%.3f",
        metrics["accuracy"],
        metrics["precision"],
        metrics["recall"],
        metrics["f1"],
        metrics["roc_auc"],
    )

    return metrics


def compute_confusion_matrix(
    y_true: npt.NDArray[np.int64],
    y_pred: npt.NDArray[np.int64],
) -> dict[str, Any]:
    """Compute confusion matrix statistics.

    Args:
        y_true: Ground truth labels.
        y_pred: Predicted labels.

    Returns:
        Dict with TN, FP, FN, TP and rates.
    """
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()

    result = {
        "true_negatives": int(tn),
        "false_positives": int(fp),
        "false_negatives": int(fn),
        "true_positives": int(tp),
        "false_positive_rate": float(fp / max(fp + tn, 1)),
        "false_negative_rate": float(fn / max(fn + tp, 1)),
    }

    logger.info(
        "Confusion matrix: TP=%d, TN=%d, FP=%d, FN=%d",
        tp,
        tn,
        fp,
        fn,
    )

    return result


def compute_feature_importance(
    model: lgb.LGBMClassifier,
    feature_names: list[str],
    top_n: int = 20,
) -> dict[str, float]:
    """Compute feature importance from LightGBM model.

    Args:
        model: Trained LightGBM model.
        feature_names: List of feature names.
        top_n: Number of top features to return.

    Returns:
        Dict mapping feature names to importance values.
    """
    importances = model.feature_importances_
    importance_dict = dict(zip(feature_names, importances.tolist()))
    importance_dict = dict(
        sorted(importance_dict.items(), key=lambda x: x[1], reverse=True)
    )
    return dict(list(importance_dict.items())[:top_n])


def evaluate_xi_model(
    model: lgb.LGBMClassifier,
    x_test: npt.NDArray[np.float64],
    y_test: npt.NDArray[np.int64],
    feature_names: list[str],
    log_to_mlflow: bool = True,
) -> dict[str, Any]:
    """Comprehensive XI model evaluation.

    Args:
        model: Trained LightGBM classifier.
        x_test: Test feature matrix.
        y_test: Test target values.
        feature_names: List of feature names.
        log_to_mlflow: Whether to log to MLflow.

    Returns:
        Dict with all evaluation results.
    """
    # Predictions
    y_proba = model.predict_proba(x_test)[:, 1].astype(np.float64)
    y_pred = (y_proba >= 0.5).astype(np.int64)

    # Classification metrics
    metrics = compute_classification_metrics(y_test, y_pred, y_proba)

    # Confusion matrix
    conf_matrix = compute_confusion_matrix(y_test, y_pred)

    # Feature importance
    importance = compute_feature_importance(model, feature_names)

    results = {
        "metrics": metrics,
        "confusion_matrix": conf_matrix,
        "feature_importance": importance,
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
        with mlflow.start_run(run_name="xi_model_evaluation"):
            # Log metrics
            for key, value in results["metrics"].items():
                mlflow.log_metric(f"test_{key}", value)

            # Log confusion matrix stats
            for key, value in results["confusion_matrix"].items():
                if isinstance(value, (int, float)):
                    mlflow.log_metric(f"confusion_{key}", value)

            # Log top feature importance
            for feat, imp in list(results["feature_importance"].items())[:10]:
                mlflow.log_metric(f"importance_{feat}", imp)

    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to log evaluation to MLflow: %s", e)
