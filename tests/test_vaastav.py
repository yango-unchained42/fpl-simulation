"""Tests for vaastav historical data ingestion module."""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import requests  # type: ignore[import-untyped]

from src.data.ingest_vaastav import (
    MAX_RETRIES,
    clear_cache,
    fetch_gw_history,
    fetch_season_history,
    load_historical_data,
)


class TestVaastavCache:
    """Tests for vaastav caching mechanism."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory for each test."""
        monkeypatch.setattr("src.data.ingest_vaastav.DATA_DIR", tmp_path / "vaastav")

    def test_cache_saves_and_loads(self) -> None:
        """Test that cache saves DataFrame and loads it back."""
        from src.data.ingest_vaastav import (
            _cache_key,
            _is_cache_valid,
            _load_cache,
            _save_cache,
        )

        cache_path = _cache_key("2023-24", "gws")
        test_df = pl.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        _save_cache(cache_path, test_df)
        assert cache_path.exists()
        loaded = _load_cache(cache_path)
        assert loaded.shape == test_df.shape
        assert _is_cache_valid(cache_path)

    def test_cache_invalid_when_missing(self) -> None:
        """Test that cache is invalid when file doesn't exist."""
        from src.data.ingest_vaastav import _cache_key, _is_cache_valid

        cache_path = _cache_key("2023-24", "nonexistent")
        assert not _is_cache_valid(cache_path)

    def test_cache_invalid_when_expired(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that cache is invalid when TTL expired."""
        import os

        from src.data.ingest_vaastav import _cache_key, _is_cache_valid, _save_cache

        cache_path = _cache_key("2023-24", "expired")
        test_df = pl.DataFrame({"id": [1]})
        _save_cache(cache_path, test_df)
        old_time = time.time() - 172800  # 48 hours ago
        os.utime(cache_path, (old_time, old_time))
        assert not _is_cache_valid(cache_path, ttl=86400)

    def test_clear_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that clear_cache removes all cached files."""
        from src.data.ingest_vaastav import _cache_key, _save_cache

        cache_path1 = _cache_key("2023-24", "gws")
        cache_path2 = _cache_key("2022-23", "players_raw")
        _save_cache(cache_path1, pl.DataFrame({"a": [1]}))
        _save_cache(cache_path2, pl.DataFrame({"b": [2]}))
        clear_cache()
        assert not cache_path1.exists()
        assert not cache_path2.exists()


class TestVaastavRetryLogic:
    """Tests for vaastav retry logic."""

    def test_successful_request_no_retry(self) -> None:
        """Test that successful request doesn't retry."""
        mock_response = MagicMock()
        mock_response.content = b"id,name\n1,A"

        with patch(
            "src.data.ingest_vaastav.requests.get", return_value=mock_response
        ) as mock_get:
            from src.data.ingest_vaastav import _get_with_retry

            result = _get_with_retry("http://test.com")
            assert mock_get.call_count == 1
            assert result == mock_response

    def test_retry_on_failure_then_success(self) -> None:
        """Test that retries on failure then succeeds."""
        mock_response = MagicMock()
        mock_response.content = b"id,name\n1,A"

        with patch("src.data.ingest_vaastav.requests.get") as mock_get:
            mock_get.side_effect = [
                requests.RequestException("Connection error"),
                requests.RequestException("Connection error"),
                mock_response,
            ]
            from src.data.ingest_vaastav import _get_with_retry

            result = _get_with_retry("http://test.com")
            assert mock_get.call_count == 3
            assert result == mock_response

    def test_exhausted_retries_raises(self) -> None:
        """Test that exhausted retries raise an exception."""
        with patch("src.data.ingest_vaastav.requests.get") as mock_get:
            mock_get.side_effect = requests.RequestException("Permanent failure")
            from src.data.ingest_vaastav import _get_with_retry

            with pytest.raises(requests.RequestException):
                _get_with_retry("http://test.com")
            assert mock_get.call_count == MAX_RETRIES


class TestFetchGwHistory:
    """Tests for fetch_gw_history function."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory for each test."""
        monkeypatch.setattr("src.data.ingest_vaastav.DATA_DIR", tmp_path / "vaastav")

    def test_fetches_and_filters_gameweek(self) -> None:
        """Test fetching GW history and filtering by gameweek."""
        csv_content = b"gw,id,points\n1,1,10\n1,2,5\n2,1,8\n2,2,12"
        mock_response = MagicMock()
        mock_response.content = csv_content

        with patch(
            "src.data.ingest_vaastav._get_with_retry", return_value=mock_response
        ):
            result = fetch_gw_history("2023-24", gameweek=1, use_cache=False)

        assert result.shape[0] == 2
        assert result["gw"].to_list() == [1, 1]

    def test_uses_cache_when_available(self) -> None:
        """Test that cached data is used when available."""
        from src.data.ingest_vaastav import _cache_key, _save_cache

        cache_path = _cache_key("2023-24", "gws")
        cached_df = pl.DataFrame({"gw": [1, 2], "id": [1, 2], "points": [10, 8]})
        _save_cache(cache_path, cached_df)

        with patch("src.data.ingest_vaastav._get_with_retry") as mock_get:
            result = fetch_gw_history("2023-24", use_cache=True)
            mock_get.assert_not_called()
            assert result.shape[0] == 2


class TestFetchSeasonHistory:
    """Tests for fetch_season_history function."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory for each test."""
        monkeypatch.setattr("src.data.ingest_vaastav.DATA_DIR", tmp_path / "vaastav")

    def test_fetches_season_data(self) -> None:
        """Test fetching season summary data."""
        csv_content = b"id,web_name,total_points\n1,Saka,200\n2,Salah,250"
        mock_response = MagicMock()
        mock_response.content = csv_content

        with patch(
            "src.data.ingest_vaastav._get_with_retry", return_value=mock_response
        ):
            result = fetch_season_history("2023-24", use_cache=False)

        assert result.shape[0] == 2
        assert "web_name" in result.columns

    def test_uses_cache_when_available(self) -> None:
        """Test that cached season data is used when available."""
        from src.data.ingest_vaastav import _cache_key, _save_cache

        cache_path = _cache_key("2023-24", "players_raw")
        cached_df = pl.DataFrame(
            {"id": [1], "web_name": ["Saka"], "total_points": [200]}
        )
        _save_cache(cache_path, cached_df)

        with patch("src.data.ingest_vaastav._get_with_retry") as mock_get:
            result = fetch_season_history("2023-24", use_cache=True)
            mock_get.assert_not_called()
            assert result.shape[0] == 1


class TestLoadHistoricalData:
    """Tests for load_historical_data function."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory for each test."""
        monkeypatch.setattr("src.data.ingest_vaastav.DATA_DIR", tmp_path / "vaastav")

    def test_loads_multiple_seasons(self) -> None:
        """Test loading data from multiple seasons."""
        csv_content = b"gw,id,points\n1,1,10"
        mock_response = MagicMock()
        mock_response.content = csv_content

        with patch(
            "src.data.ingest_vaastav._get_with_retry", return_value=mock_response
        ):
            result = load_historical_data(
                seasons=["2021-22", "2022-23"], use_cache=False
            )

        assert result.shape[0] == 2  # One row per season

    def test_returns_empty_on_all_failures(self) -> None:
        """Test that empty DataFrame is returned when all seasons fail."""
        with patch(
            "src.data.ingest_vaastav._get_with_retry",
            side_effect=requests.RequestException("Fail"),
        ):
            result = load_historical_data(seasons=["2021-22"], use_cache=False)
            assert result.shape[0] == 0

    def test_partial_failure_loads_available(self) -> None:
        """Test that available seasons are loaded even if some fail."""
        csv_content = b"gw,id,points\n1,1,10"
        mock_response = MagicMock()
        mock_response.content = csv_content

        call_count = [0]

        def side_effect(*args: object, **kwargs: object) -> MagicMock:
            call_count[0] += 1
            if call_count[0] == 1:
                raise requests.RequestException("Fail")
            return mock_response

        with patch("src.data.ingest_vaastav._get_with_retry", side_effect=side_effect):
            result = load_historical_data(
                seasons=["2021-22", "2022-23"], use_cache=False
            )

        assert result.shape[0] == 1  # Only one season succeeded


class TestLogErrorsToMlflow:
    """Tests for MLflow error logging."""

    def test_logs_errors_when_mlflow_available(self) -> None:
        """Test that errors are logged to MLflow when available."""
        mock_mlflow = MagicMock()
        mock_mlflow.start_run.return_value.__enter__ = MagicMock()
        mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

        errors = [
            {"season": "2021-22", "error": "Network error", "timestamp": 1234567890.0}
        ]

        with patch("src.utils.mlflow_client._get_mlflow", return_value=mock_mlflow):
            from src.data.ingest_vaastav import _log_errors_to_mlflow

            _log_errors_to_mlflow(errors)

        mock_mlflow.log_param.assert_called()

    def test_skips_when_mlflow_unavailable(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that error logging is skipped when MLflow is not available."""
        errors = [
            {"season": "2021-22", "error": "Network error", "timestamp": 1234567890.0}
        ]

        with (
            patch("src.utils.mlflow_client._get_mlflow", return_value=None),
            caplog.at_level("DEBUG"),
        ):
            from src.data.ingest_vaastav import _log_errors_to_mlflow

            _log_errors_to_mlflow(errors)

        assert "MLflow not available" in caplog.text
