"""Hyperparameter optimization for LightGBM player predictor.

Implements time-series cross-validation and randomized search to find
optimal LightGBM hyperparameters for FPL point prediction.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
import numpy.typing as npt
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit

from src.models.lightgbm_model import MODEL_DIR

logger = logging.getLogger(__name__)

OPTIMIZATION_FILE = MODEL_DIR / "optimization_results.json"

# Hyperparameter search space
PARAM_DISTRIBUTIONS: dict[str, Any] = {
    "n_estimators": [100, 200, 300, 500, 800, 1000],
    "max_depth": [3, 5, 7, 10, 15],
    "learning_rate": [0.01, 0.02, 0.05, 0.1, 0.2],
    "subsample": [0.6, 0.7, 0.8, 0.9, 1.0],
    "colsample_bytree": [0.6, 0.7, 0.8, 0.9, 1.0],
    "min_child_samples": [5, 10, 20, 30, 50],
    "reg_alpha": [0.0, 0.1, 0.5, 1.0, 5.0],
    "reg_lambda": [0.0, 0.1, 0.5, 1.0, 5.0],
}

# Optimization defaults
N_ITER = 50
CV_FOLDS = 5
SCORING = "neg_mean_absolute_error"
RANDOM_STATE = 42


def optimize_hyperparameters(
    x_train: npt.NDArray[np.float64],
    y_train: npt.NDArray[np.float64],
    param_distributions: dict[str, Any] | None = None,
    n_iter: int = N_ITER,
    cv_folds: int = CV_FOLDS,
    scoring: str = SCORING,
    random_state: int = RANDOM_STATE,
    feature_names: list[str] | None = None,
    log_to_mlflow: bool = True,
) -> tuple[lgb.LGBMRegressor, dict[str, Any], dict[str, Any]]:
    """Optimize LightGBM hyperparameters using RandomizedSearchCV.

    Uses time-series cross-validation to prevent data leakage.

    Args:
        X_train: Training feature matrix.
        y_train: Training target vector.
        param_distributions: Hyperparameter search space.
        n_iter: Number of parameter settings to sample.
        cv_folds: Number of CV folds.
        scoring: Scoring metric for optimization.
        random_state: Random seed for reproducibility.
        feature_names: List of feature names.
        log_to_mlflow: Whether to log to MLflow.

    Returns:
        Tuple of (best_model, best_params, optimization_info).
    """
    t0 = time.time()
    params = param_distributions or PARAM_DISTRIBUTIONS

    # Create base model
    base_model = lgb.LGBMRegressor(random_state=random_state, verbose=-1)

    # Time-series cross-validation
    tscv = TimeSeriesSplit(n_splits=cv_folds)

    # Randomized search
    search = RandomizedSearchCV(
        estimator=base_model,
        param_distributions=params,
        n_iter=n_iter,
        cv=tscv,
        scoring=scoring,
        random_state=random_state,
        n_jobs=-1,
        return_train_score=True,
    )

    logger.info(
        "Starting hyperparameter optimization: %d iterations, %d-fold time-series CV",
        n_iter,
        cv_folds,
    )

    search.fit(x_train, y_train)

    elapsed = time.time() - t0
    best_params = search.best_params_
    best_score = search.best_score_

    info = {
        "best_score": float(best_score),
        "n_iter": n_iter,
        "cv_folds": cv_folds,
        "scoring": scoring,
        "optimization_time_seconds": elapsed,
        "n_candidates_evaluated": len(search.cv_results_["mean_test_score"]),
    }

    logger.info(
        "Optimization complete: best_score=%.4f, time=%.1fs",
        best_score,
        elapsed,
    )
    logger.info("Best parameters: %s", best_params)

    # Log to MLflow
    if log_to_mlflow:
        _log_optimization_to_mlflow(search, info, feature_names)

    return search.best_estimator_, best_params, info


def save_optimization_results(
    best_params: dict[str, Any],
    info: dict[str, Any],
    results_path: Path = OPTIMIZATION_FILE,
) -> None:
    """Save optimization results to disk.

    Args:
        best_params: Best hyperparameters found.
        info: Optimization metadata.
        results_path: Path to save results.
    """
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    import json

    results = {
        "best_params": best_params,
        "info": info,
    }

    with open(results_path, "w") as f:
        json.dump(results, f, indent=2)

    logger.info("Saved optimization results to %s", results_path)


def load_optimization_results(
    results_path: Path = OPTIMIZATION_FILE,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Load optimization results from disk.

    Args:
        results_path: Path to saved results.

    Returns:
        Tuple of (best_params, info).
    """
    import json

    if not results_path.exists():
        raise FileNotFoundError(f"Optimization results not found at {results_path}")

    with open(results_path) as f:
        results = json.load(f)

    return results["best_params"], results["info"]


def _log_optimization_to_mlflow(
    search: RandomizedSearchCV,
    info: dict[str, Any],
    feature_names: list[str] | None,
) -> None:
    """Log optimization results to MLflow."""
    try:
        from src.utils.mlflow_client import _get_mlflow

        mlflow = _get_mlflow()
        if mlflow is None:
            logger.debug("MLflow not available, skipping optimization logging")
            return

        mlflow.set_tracking_uri("mlruns")
        mlflow.set_experiment("fpl_model_training")
        with mlflow.start_run(run_name="hyperparameter_optimization"):
            # Log best parameters
            for key, value in search.best_params_.items():
                mlflow.log_param(f"best_{key}", value)

            # Log optimization info
            for key, value in info.items():
                if isinstance(value, (int, float, str, bool)):
                    mlflow.log_param(key, value)

            # Log CV results summary
            cv_results = search.cv_results_
            mlflow.log_metric(
                "mean_test_score", float(cv_results["mean_test_score"].mean())
            )
            mlflow.log_metric(
                "std_test_score", float(cv_results["std_test_score"].mean())
            )

    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to log optimization to MLflow: %s", e)
