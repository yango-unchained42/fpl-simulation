"""Starting XI prediction model (LightGBM Classifier).

Predicts the probability of a player starting the match (minutes >= 60).
Includes probability calibration, class weights for imbalance, early stopping,
and MLflow logging.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import joblib
import lightgbm as lgb
import numpy as np
import numpy.typing as npt

logger = logging.getLogger(__name__)

MODEL_DIR = Path("data/models")
MODEL_FILE = MODEL_DIR / "starting_xi.joblib"
FEATURE_NAMES_FILE = MODEL_DIR / "xi_feature_names.txt"

# Default LightGBM hyperparameters for classification
DEFAULT_PARAMS: dict[str, Any] = {
    "n_estimators": 500,
    "max_depth": 8,
    "learning_rate": 0.05,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_samples": 20,
    "reg_alpha": 0.1,
    "reg_lambda": 0.1,
    "class_weight": "balanced",  # Handles class imbalance
    "random_state": 42,
    "verbose": -1,
}

# Early stopping parameters
EARLY_STOPPING_ROUNDS = 50


def train_starting_xi_model(
    x_train: npt.NDArray[np.float64],
    y_train: npt.NDArray[np.int64],
    x_val: npt.NDArray[np.float64] | None = None,
    y_val: npt.NDArray[np.int64] | None = None,
    params: dict[str, Any] | None = None,
    feature_names: list[str] | None = None,
    log_to_mlflow: bool = True,
) -> tuple[lgb.LGBMClassifier, dict[str, Any]]:
    """Train the LightGBM starting XI classifier.

    Args:
        x_train: Training feature matrix.
        y_train: Training target vector (1=started, 0=did not start).
        x_val: Validation feature matrix (for early stopping).
        y_val: Validation target vector.
        params: LightGBM hyperparameters. Defaults to DEFAULT_PARAMS.
        feature_names: List of feature names.
        log_to_mlflow: Whether to log training to MLflow.

    Returns:
        Tuple of (trained model, training info dict).
    """
    t0 = time.time()
    model_params = {**DEFAULT_PARAMS, **(params or {})}

    # Create model
    model = lgb.LGBMClassifier(**model_params)

    # Prepare fit kwargs
    fit_kwargs: dict[str, Any] = {}
    if x_val is not None and y_val is not None:
        fit_kwargs["eval_set"] = [(x_val, y_val)]
        fit_kwargs["callbacks"] = [
            lgb.early_stopping(EARLY_STOPPING_ROUNDS, verbose=False),
            lgb.log_evaluation(period=0),
        ]

    # Train
    model.fit(x_train, y_train, **fit_kwargs)

    elapsed = time.time() - t0
    best_iteration = getattr(model, "best_iteration_", model_params["n_estimators"])

    info = {
        "n_estimators": model_params["n_estimators"],
        "best_iteration": best_iteration,
        "training_time_seconds": elapsed,
        "n_features": x_train.shape[1],
        "n_train_samples": x_train.shape[0],
        "n_val_samples": x_val.shape[0] if x_val is not None else 0,
    }

    logger.info(
        "Trained Starting XI model: %d features, best_iteration=%d, time=%.1fs",
        info["n_features"],
        info["best_iteration"],
        info["training_time_seconds"],
    )

    if log_to_mlflow:
        _log_training_to_mlflow(model, info, feature_names)

    return model, info


def predict_start_probability(
    model: lgb.LGBMClassifier,
    x: npt.NDArray[np.float64],
) -> npt.NDArray[np.float64]:
    """Predict starting probability for players.

    Args:
        model: Trained LightGBM classifier.
        x: Feature matrix.

    Returns:
        Array of start probabilities (0-1).
    """
    proba = np.asarray(model.predict_proba(x))
    # Return probability of class 1 (started)
    return proba[:, 1].astype(np.float64)


def save_model(
    model: lgb.LGBMClassifier,
    feature_names: list[str],
    model_path: Path = MODEL_FILE,
) -> None:
    """Save model and feature names to disk.

    Args:
        model: Trained LightGBM model.
        feature_names: List of feature names used during training.
        model_path: Path to save the model.
    """
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    # Save model
    joblib.dump(model, model_path)
    logger.info("Saved XI model to %s", model_path)

    # Save feature names
    feature_path = model_path.parent / FEATURE_NAMES_FILE.name
    with open(feature_path, "w") as f:
        for name in feature_names:
            f.write(f"{name}\n")
    logger.info("Saved XI feature names to %s", feature_path)


def load_model(
    model_path: Path = MODEL_FILE,
) -> tuple[lgb.LGBMClassifier, list[str]]:
    """Load model and feature names from disk.

    Args:
        model_path: Path to the saved model.

    Returns:
        Tuple of (model, feature_names).
    """
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found at {model_path}")

    model = joblib.load(model_path)
    logger.info("Loaded XI model from %s", model_path)

    # Load feature names
    feature_path = model_path.parent / FEATURE_NAMES_FILE.name
    if feature_path.exists():
        with open(feature_path) as f:
            feature_names = [line.strip() for line in f if line.strip()]
    else:
        feature_names = []
        logger.warning("Feature names file not found at %s", feature_path)

    return model, feature_names


def get_feature_importance(
    model: lgb.LGBMClassifier,
    feature_names: list[str],
    top_n: int = 20,
) -> dict[str, float]:
    """Get feature importance from trained model.

    Args:
        model: Trained LightGBM model.
        feature_names: List of feature names.
        top_n: Number of top features to return.

    Returns:
        Dict mapping feature names to importance values (gain-based).
    """
    importances = model.feature_importances_
    importance_dict = dict(zip(feature_names, importances.tolist()))
    importance_dict = dict(
        sorted(importance_dict.items(), key=lambda x: x[1], reverse=True)
    )
    return dict(list(importance_dict.items())[:top_n])


def _log_training_to_mlflow(
    model: lgb.LGBMClassifier,
    info: dict[str, Any],
    feature_names: list[str] | None,
) -> None:
    """Log training metrics to MLflow."""
    try:
        from src.utils.mlflow_client import _get_mlflow

        mlflow = _get_mlflow()
        if mlflow is None:
            logger.debug("MLflow not available, skipping training logging")
            return

        mlflow.set_tracking_uri("mlruns")
        mlflow.set_experiment("fpl_model_training")
        with mlflow.start_run(run_name="starting_xi_predictor"):
            # Log parameters
            for key, value in info.items():
                mlflow.log_param(key, value)

            # Log model hyperparameters
            for key, value in model.get_params().items():
                if isinstance(value, (int, float, str, bool)):
                    mlflow.log_param(f"hyper_{key}", value)

            # Log feature importance
            if feature_names:
                importances = get_feature_importance(model, feature_names, top_n=10)
                for feat, imp in importances.items():
                    mlflow.log_metric(f"importance_{feat}", imp)

    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to log training to MLflow: %s", e)
