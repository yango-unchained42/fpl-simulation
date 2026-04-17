"""MLflow client for local experiment tracking.

MLflow is used for local development only. Model artefacts
for deployment should be serialized to data/models/*.joblib.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

MLFLOW_TRACKING_URI = "mlruns"
DEFAULT_EXPERIMENTS = {
    "player_predictor": "fpl_player_predictor",
    "starting_xi": "fpl_starting_xi_classifier",
    "match_simulator": "fpl_match_simulator",
}
MODEL_REGISTRY_PATH = Path("data/models/registry.json")


def _get_mlflow() -> Any:
    """Lazily import mlflow to avoid hard dependency.

    Returns:
        The mlflow module if available, otherwise None.
    """
    try:
        import mlflow  # noqa: PLC0415

        return mlflow
    except ImportError:
        return None


def setup_tracking(uri: str = MLFLOW_TRACKING_URI) -> None:
    """Configure MLflow tracking URI.

    Args:
        uri: Tracking URI for MLflow (default: local mlruns directory).
    """
    mlflow = _get_mlflow()
    if mlflow is None:
        logger.warning("mlflow package not installed, tracking disabled")
        return

    mlflow.set_tracking_uri(uri)
    logger.info("MLflow tracking URI set to %s", uri)


def get_or_create_experiment(experiment_name: str) -> str | None:
    """Get or create an MLflow experiment.

    Args:
        experiment_name: Name of the experiment.

    Returns:
        Experiment ID if successful, None otherwise.
    """
    mlflow = _get_mlflow()
    if mlflow is None:
        return None

    try:
        setup_tracking()
        experiment = mlflow.set_experiment(experiment_name)
        return experiment.experiment_id  # type: ignore[no-any-return]
    except Exception as e:
        logger.error("Failed to create experiment '%s': %s", experiment_name, e)
        return None


def list_experiments() -> list[dict[str, str]]:
    """List all MLflow experiments.

    Returns:
        List of dicts with experiment name and ID.
    """
    mlflow = _get_mlflow()
    if mlflow is None:
        return []

    try:
        setup_tracking()
        client = mlflow.MlflowClient()
        experiments = client.search_experiments()
        return [
            {"name": exp.name, "experiment_id": exp.experiment_id}
            for exp in experiments
        ]
    except Exception as e:
        logger.error("Failed to list experiments: %s", e)
        return []


def log_params(run_id: str, params: dict[str, str | int | float]) -> bool:
    """Log parameters to an existing MLflow run.

    Args:
        run_id: ID of the MLflow run.
        params: Dictionary of parameter names and values.

    Returns:
        True if successful, False otherwise.
    """
    mlflow = _get_mlflow()
    if mlflow is None:
        return False

    try:
        setup_tracking()
        client = mlflow.MlflowClient()
        for key, value in params.items():
            client.log_param(run_id, key, str(value))
        return True
    except Exception as e:
        logger.error("Failed to log params for run %s: %s", run_id, e)
        return False


def log_metrics(
    run_id: str, metrics: dict[str, float], step: int | None = None
) -> bool:
    """Log metrics to an existing MLflow run.

    Args:
        run_id: ID of the MLflow run.
        metrics: Dictionary of metric names and values.
        step: Optional step number for the metrics.

    Returns:
        True if successful, False otherwise.
    """
    mlflow = _get_mlflow()
    if mlflow is None:
        return False

    try:
        setup_tracking()
        client = mlflow.MlflowClient()
        timestamp = int(mlflow.utils.time_utils.now_ts() * 1000)
        for key, value in metrics.items():
            client.log_metric(run_id, key, value, timestamp, step or 0)
        return True
    except Exception as e:
        logger.error("Failed to log metrics for run %s: %s", run_id, e)
        return False


def log_artifact(
    run_id: str, artifact_path: str, artifact_folder: str | None = None
) -> bool:
    """Log an artifact file to an MLflow run.

    Args:
        run_id: ID of the MLflow run.
        artifact_path: Path to the artifact file.
        artifact_folder: Optional folder within the run's artifact directory.

    Returns:
        True if successful, False otherwise.
    """
    mlflow = _get_mlflow()
    if mlflow is None:
        return False

    try:
        setup_tracking()
        client = mlflow.MlflowClient()
        client.log_artifact(run_id, artifact_path, artifact_folder)
        return True
    except Exception as e:
        logger.error("Failed to log artifact for run %s: %s", run_id, e)
        return False


def log_experiment(
    experiment_name: str,
    params: dict[str, str | int | float],
    metrics: dict[str, float],
    model_uri: str | None = None,
) -> str | None:
    """Log a complete MLflow experiment run.

    Args:
        experiment_name: Name of the experiment.
        params: Dictionary of parameters.
        metrics: Dictionary of metrics.
        model_uri: Optional path to model artefact.

    Returns:
        Run ID if successful, None otherwise.
    """
    mlflow = _get_mlflow()
    if mlflow is None:
        return None

    try:
        setup_tracking()
        mlflow.set_experiment(experiment_name)
        with mlflow.start_run() as run:
            mlflow.log_params(params)
            mlflow.log_metrics(metrics)
            if model_uri:
                mlflow.log_artifact(model_uri)
        return run.info.run_id  # type: ignore[no-any-return]
    except Exception as e:
        logger.error("Failed to log experiment '%s': %s", experiment_name, e)
        return None


def register_model(
    model_name: str,
    model_path: str,
    metrics: dict[str, float] | None = None,
    params: dict[str, str | int | float] | None = None,
    version: str | None = None,
) -> str | None:
    """Register a model in the local model registry.

    Saves model metadata to a local JSON registry file for tracking
    model versions outside of MLflow.

    Args:
        model_name: Name of the model.
        model_path: Path to the serialized model file (.joblib).
        metrics: Optional metrics dict to store with the model.
        params: Optional parameters dict to store with the model.
        version: Optional explicit version string (auto-incremented if None).

    Returns:
        Version string if successful, None otherwise.
    """
    try:
        registry = _load_registry()

        if model_name not in registry:
            registry[model_name] = []

        # Determine next version
        if version is None:
            existing_versions = [
                int(v["version"].split("v")[1])
                for v in registry[model_name]
                if v["version"].startswith("v")
            ]
            next_version = max(existing_versions, default=0) + 1
            version = f"v{next_version}"

        entry: dict[str, Any] = {
            "version": version,
            "model_path": model_path,
            "metrics": metrics or {},
            "params": params or {},
            "registered_at": __import__("datetime")
            .datetime.now(tz=__import__("datetime").timezone.utc)
            .isoformat(),
        }

        registry[model_name].append(entry)
        _save_registry(registry)

        logger.info("Registered model %s version %s", model_name, version)
        return version

    except Exception as e:
        logger.error("Failed to register model '%s': %s", model_name, e)
        return None


def get_model_versions(model_name: str) -> list[dict[str, Any]]:
    """Get all versions of a registered model.

    Args:
        model_name: Name of the model.

    Returns:
        List of model version entries, or empty list if not found.
    """
    registry = _load_registry()
    return registry.get(model_name, [])


def get_latest_model_version(model_name: str) -> dict[str, Any] | None:
    """Get the latest version of a registered model.

    Args:
        model_name: Name of the model.

    Returns:
        Latest model version entry, or None if not found.
    """
    versions = get_model_versions(model_name)
    return versions[-1] if versions else None


def _load_registry() -> dict[str, list[dict[str, Any]]]:
    """Load the local model registry from disk.

    Returns:
        Registry dictionary mapping model names to version lists.
    """
    if MODEL_REGISTRY_PATH.exists():
        with open(MODEL_REGISTRY_PATH, encoding="utf-8") as f:
            return json.load(f)  # type: ignore[no-any-return]
    return {}


def _save_registry(registry: dict[str, list[dict[str, Any]]]) -> None:
    """Save the local model registry to disk.

    Args:
        registry: Registry dictionary to save.
    """
    MODEL_REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MODEL_REGISTRY_PATH, "w", encoding="utf-8") as f:
        json.dump(registry, f, indent=2)
