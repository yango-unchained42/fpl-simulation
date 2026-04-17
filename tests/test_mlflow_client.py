"""Tests for MLflow client module."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import src.utils.mlflow_client as mlflow_module
from src.utils.mlflow_client import (
    get_latest_model_version,
    get_model_versions,
    get_or_create_experiment,
    list_experiments,
    log_artifact,
    log_experiment,
    log_metrics,
    log_params,
    register_model,
    setup_tracking,
)


class TestGetMlflow:
    """Tests for _get_mlflow lazy import."""

    def test_returns_none_when_not_installed(self) -> None:
        """Test that _get_mlflow returns None when mlflow is not installed."""
        with patch.dict(sys.modules, {"mlflow": None}):
            if "src.utils.mlflow_client" in sys.modules:
                del sys.modules["src.utils.mlflow_client"]

            from src.utils.mlflow_client import _get_mlflow

            result = _get_mlflow()
            assert result is None


class TestSetupTracking:
    """Tests for setup_tracking function."""

    def test_setup_tracking_without_mlflow(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test setup_tracking logs warning when mlflow not installed."""
        with patch("src.utils.mlflow_client._get_mlflow", return_value=None):
            setup_tracking()
            assert "mlflow package not installed" in caplog.text

    def test_setup_tracking_with_mlflow(self) -> None:
        """Test setup_tracking sets tracking URI when mlflow is available."""
        mock_mlflow = MagicMock()
        with patch("src.utils.mlflow_client._get_mlflow", return_value=mock_mlflow):
            setup_tracking("custom_uri")
            mock_mlflow.set_tracking_uri.assert_called_once_with("custom_uri")


class TestGetOrCreateExperiment:
    """Tests for get_or_create_experiment function."""

    def test_returns_none_without_mlflow(self) -> None:
        """Test returns None when mlflow not installed."""
        with patch("src.utils.mlflow_client._get_mlflow", return_value=None):
            result = get_or_create_experiment("test_exp")
            assert result is None

    def test_creates_experiment_with_mlflow(self) -> None:
        """Test experiment creation with mlflow."""
        mock_mlflow = MagicMock()
        mock_mlflow.set_experiment.return_value = MagicMock(experiment_id="123")

        with patch("src.utils.mlflow_client._get_mlflow", return_value=mock_mlflow):
            result = get_or_create_experiment("test_exp")
            assert result == "123"

    def test_handles_exception(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test exception handling during experiment creation."""
        mock_mlflow = MagicMock()
        mock_mlflow.set_experiment.side_effect = Exception("Connection error")

        with patch("src.utils.mlflow_client._get_mlflow", return_value=mock_mlflow):
            result = get_or_create_experiment("test_exp")
            assert result is None
            assert "Failed to create experiment" in caplog.text


class TestListExperiments:
    """Tests for list_experiments function."""

    def test_returns_empty_without_mlflow(self) -> None:
        """Test returns empty list when mlflow not installed."""
        with patch("src.utils.mlflow_client._get_mlflow", return_value=None):
            result = list_experiments()
            assert result == []

    def test_lists_experiments_with_mlflow(self) -> None:
        """Test experiment listing with mlflow."""
        mock_mlflow = MagicMock()
        mock_client = MagicMock()
        mock_exp1 = MagicMock()
        mock_exp1.name = "exp1"
        mock_exp1.experiment_id = "1"
        mock_exp2 = MagicMock()
        mock_exp2.name = "exp2"
        mock_exp2.experiment_id = "2"
        mock_client.search_experiments.return_value = [mock_exp1, mock_exp2]
        mock_mlflow.MlflowClient.return_value = mock_client

        with patch("src.utils.mlflow_client._get_mlflow", return_value=mock_mlflow):
            result = list_experiments()
            assert len(result) == 2
            assert result[0] == {"name": "exp1", "experiment_id": "1"}
            assert result[1] == {"name": "exp2", "experiment_id": "2"}


class TestLogParams:
    """Tests for log_params function."""

    def test_returns_false_without_mlflow(self) -> None:
        """Test returns False when mlflow not installed."""
        with patch("src.utils.mlflow_client._get_mlflow", return_value=None):
            result = log_params("run123", {"param1": "value1"})
            assert result is False

    def test_logs_params_with_mlflow(self) -> None:
        """Test parameter logging with mlflow."""
        mock_mlflow = MagicMock()
        mock_client = MagicMock()
        mock_mlflow.MlflowClient.return_value = mock_client

        with patch("src.utils.mlflow_client._get_mlflow", return_value=mock_mlflow):
            result = log_params("run123", {"param1": "value1", "param2": 42})
            assert result is True
            assert mock_client.log_param.call_count == 2


class TestLogMetrics:
    """Tests for log_metrics function."""

    def test_returns_false_without_mlflow(self) -> None:
        """Test returns False when mlflow not installed."""
        with patch("src.utils.mlflow_client._get_mlflow", return_value=None):
            result = log_metrics("run123", {"accuracy": 0.95})
            assert result is False

    def test_logs_metrics_with_mlflow(self) -> None:
        """Test metric logging with mlflow."""
        mock_mlflow = MagicMock()
        mock_mlflow.utils.time_utils.now_ts.return_value = 1000.0
        mock_client = MagicMock()
        mock_mlflow.MlflowClient.return_value = mock_client

        with patch("src.utils.mlflow_client._get_mlflow", return_value=mock_mlflow):
            result = log_metrics("run123", {"accuracy": 0.95, "loss": 0.05}, step=1)
            assert result is True
            assert mock_client.log_metric.call_count == 2


class TestLogArtifact:
    """Tests for log_artifact function."""

    def test_returns_false_without_mlflow(self) -> None:
        """Test returns False when mlflow not installed."""
        with patch("src.utils.mlflow_client._get_mlflow", return_value=None):
            result = log_artifact("run123", "/path/to/model.joblib")
            assert result is False

    def test_logs_artifact_with_mlflow(self) -> None:
        """Test artifact logging with mlflow."""
        mock_mlflow = MagicMock()
        mock_client = MagicMock()
        mock_mlflow.MlflowClient.return_value = mock_client

        with patch("src.utils.mlflow_client._get_mlflow", return_value=mock_mlflow):
            result = log_artifact("run123", "/path/to/model.joblib", "models")
            assert result is True
            mock_client.log_artifact.assert_called_once_with(
                "run123", "/path/to/model.joblib", "models"
            )


class TestLogExperiment:
    """Tests for log_experiment function."""

    def test_returns_none_without_mlflow(self) -> None:
        """Test returns None when mlflow not installed."""
        with patch("src.utils.mlflow_client._get_mlflow", return_value=None):
            result = log_experiment("test_exp", {}, {})
            assert result is None

    def test_logs_experiment_with_mlflow(self) -> None:
        """Test experiment logging with mlflow."""
        mock_mlflow = MagicMock()
        mock_run = MagicMock()
        mock_run.info.run_id = "run123"
        mock_mlflow.start_run.return_value.__enter__ = MagicMock(return_value=mock_run)
        mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

        with patch("src.utils.mlflow_client._get_mlflow", return_value=mock_mlflow):
            result = log_experiment(
                "test_exp",
                {"learning_rate": 0.01},
                {"accuracy": 0.95},
                "/path/to/model.joblib",
            )
            assert result == "run123"
            mock_mlflow.log_params.assert_called_once()
            mock_mlflow.log_metrics.assert_called_once()
            mock_mlflow.log_artifact.assert_called_once()


class TestRegisterModel:
    """Tests for register_model function."""

    @pytest.fixture(autouse=True)
    def _clean_registry(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary registry file for each test."""
        registry_path = tmp_path / "registry.json"
        monkeypatch.setattr(mlflow_module, "MODEL_REGISTRY_PATH", registry_path)

    def test_register_first_model(self) -> None:
        """Test registering the first version of a model."""
        version = register_model("player_predictor", "/path/to/model.joblib")
        assert version == "v1"

    def test_register_subsequent_versions(self) -> None:
        """Test registering multiple versions of a model."""
        v1 = register_model("player_predictor", "/path/to/v1.joblib")
        v2 = register_model("player_predictor", "/path/to/v2.joblib")
        assert v1 == "v1"
        assert v2 == "v2"

    def test_register_with_explicit_version(self) -> None:
        """Test registering with an explicit version string."""
        version = register_model(
            "player_predictor", "/path/to/model.joblib", version="custom_v1"
        )
        assert version == "custom_v1"

    def test_register_with_metadata(self) -> None:
        """Test registering with metrics and params."""
        version = register_model(
            "player_predictor",
            "/path/to/model.joblib",
            metrics={"accuracy": 0.95},
            params={"learning_rate": 0.01},
        )
        assert version == "v1"
        versions = get_model_versions("player_predictor")
        assert versions[0]["metrics"] == {"accuracy": 0.95}
        assert versions[0]["params"] == {"learning_rate": 0.01}

    def test_register_handles_exception(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test exception handling during registration."""
        with patch.object(
            mlflow_module, "_save_registry", side_effect=Exception("IO error")
        ):
            version = register_model("player_predictor", "/path/to/model.joblib")
            assert version is None
            assert "Failed to register model" in caplog.text


class TestGetModelVersions:
    """Tests for get_model_versions function."""

    @pytest.fixture(autouse=True)
    def _clean_registry(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary registry file for each test."""
        registry_path = tmp_path / "registry.json"
        monkeypatch.setattr(mlflow_module, "MODEL_REGISTRY_PATH", registry_path)

    def test_get_versions_for_existing_model(self) -> None:
        """Test getting versions for a registered model."""
        register_model("player_predictor", "/path/to/v1.joblib")
        register_model("player_predictor", "/path/to/v2.joblib")

        versions = get_model_versions("player_predictor")
        assert len(versions) == 2
        assert versions[0]["version"] == "v1"
        assert versions[1]["version"] == "v2"

    def test_get_versions_for_missing_model(self) -> None:
        """Test getting versions for a non-existent model."""
        versions = get_model_versions("nonexistent_model")
        assert versions == []


class TestGetLatestModelVersion:
    """Tests for get_latest_model_version function."""

    @pytest.fixture(autouse=True)
    def _clean_registry(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary registry file for each test."""
        registry_path = tmp_path / "registry.json"
        monkeypatch.setattr(mlflow_module, "MODEL_REGISTRY_PATH", registry_path)

    def test_get_latest_version(self) -> None:
        """Test getting the latest version of a model."""
        register_model("player_predictor", "/path/to/v1.joblib")
        register_model("player_predictor", "/path/to/v2.joblib")

        latest = get_latest_model_version("player_predictor")
        assert latest is not None
        assert latest["version"] == "v2"

    def test_get_latest_for_missing_model(self) -> None:
        """Test getting latest version for a non-existent model."""
        latest = get_latest_model_version("nonexistent_model")
        assert latest is None
