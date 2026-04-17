"""Tests for Understat data ingestion module."""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.data.ingest_understat import (
    clear_cache,
    ingest_understat,
    ingest_understat_match_stats,
    ingest_understat_player_match_stats,
    ingest_understat_player_season_stats,
    ingest_understat_shots,
)


class TestUnderstatCache:
    """Tests for Understat caching mechanism."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory for each test."""
        monkeypatch.setattr(
            "src.data.ingest_understat.DATA_DIR", tmp_path / "understat"
        )

    def test_cache_saves_and_loads(self) -> None:
        """Test that cache saves DataFrame and loads it back."""
        from src.data.ingest_understat import (
            _cache_key,
            _is_cache_valid,
            _load_cache,
            _save_cache,
        )

        cache_path = _cache_key("2023/24", "shots")
        test_df = pl.DataFrame({"player": ["Saka"], "xg": [0.5]})
        _save_cache(cache_path, test_df)
        assert cache_path.exists()
        loaded = _load_cache(cache_path)
        assert loaded.shape == test_df.shape
        assert _is_cache_valid(cache_path)

    def test_cache_invalid_when_missing(self) -> None:
        """Test that cache is invalid when file doesn't exist."""
        from src.data.ingest_understat import _cache_key, _is_cache_valid

        cache_path = _cache_key("2023/24", "nonexistent")
        assert not _is_cache_valid(cache_path)

    def test_cache_invalid_when_expired(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that cache is invalid when TTL expired."""
        import os

        from src.data.ingest_understat import _cache_key, _is_cache_valid, _save_cache

        cache_path = _cache_key("2023/24", "expired")
        test_df = pl.DataFrame({"xg": [0.5]})
        _save_cache(cache_path, test_df)
        old_time = time.time() - 172800  # 48 hours ago
        os.utime(cache_path, (old_time, old_time))
        assert not _is_cache_valid(cache_path, ttl=86400)

    def test_clear_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that clear_cache removes all cached files."""
        from src.data.ingest_understat import _cache_key, _save_cache

        cache_path1 = _cache_key("2023/24", "shots")
        cache_path2 = _cache_key("2022/23", "player_match_stats")
        _save_cache(cache_path1, pl.DataFrame({"a": [1]}))
        _save_cache(cache_path2, pl.DataFrame({"b": [2]}))
        clear_cache()
        assert not cache_path1.exists()
        assert not cache_path2.exists()


class TestIngestUnderstatShots:
    """Tests for ingest_understat_shots function."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory for each test."""
        monkeypatch.setattr(
            "src.data.ingest_understat.DATA_DIR", tmp_path / "understat"
        )

    def test_returns_empty_when_soccerdata_missing(self) -> None:
        """Test that empty DataFrame is returned when soccerdata not installed."""
        with patch(
            "src.data.ingest_understat._fetch_season_table",
            return_value=pl.DataFrame(),
        ) as mock_fetch:
            result = ingest_understat_shots(seasons=["2023/24"], use_cache=False)
            assert result.is_empty()
            mock_fetch.assert_called_once_with("2023/24", "shots", "read_shot_events")

    def test_returns_data_when_available(self) -> None:
        """Test that data is returned when fetch succeeds."""
        shot_df = pl.DataFrame(
            {"player": ["Saka"], "xg": [0.5], "shot_type": ["RightFoot"]}
        )

        with patch(
            "src.data.ingest_understat._fetch_season_table",
            return_value=shot_df,
        ):
            result = ingest_understat_shots(seasons=["2023/24"], use_cache=False)

        assert result.shape[0] == 1
        assert "season" in result.columns
        assert result["season"].to_list() == ["2023/24"]

    def test_uses_cache_when_available(self) -> None:
        """Test that cached data is used when available."""
        from src.data.ingest_understat import _cache_key, _save_cache

        cache_path = _cache_key("2023/24", "shots")
        cached_df = pl.DataFrame({"player": ["Salah"], "xg": [0.8]})
        _save_cache(cache_path, cached_df)

        with patch("src.data.ingest_understat._fetch_season_table") as mock_fetch:
            result = ingest_understat_shots(seasons=["2023/24"], use_cache=True)
            mock_fetch.assert_not_called()
            assert result.shape[0] == 1


class TestIngestUnderstatMatchStats:
    """Tests for ingest_understat_match_stats function."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory for each test."""
        monkeypatch.setattr(
            "src.data.ingest_understat.DATA_DIR", tmp_path / "understat"
        )

    def test_returns_empty_when_soccerdata_missing(self) -> None:
        """Test that empty DataFrame is returned when soccerdata not installed."""
        with patch(
            "src.data.ingest_understat._fetch_season_table",
            return_value=pl.DataFrame(),
        ) as mock_fetch:
            result = ingest_understat_match_stats(seasons=["2023/24"], use_cache=False)
            assert result.is_empty()
            mock_fetch.assert_called_once_with(
                "2023/24", "match_stats", "read_team_match_stats"
            )

    def test_returns_data_when_available(self) -> None:
        """Test that data is returned when fetch succeeds."""
        match_df = pl.DataFrame(
            {
                "home_team": ["Arsenal"],
                "away_team": ["Chelsea"],
                "xg_home": [1.5],
                "xg_away": [0.8],
            }
        )

        with patch(
            "src.data.ingest_understat._fetch_season_table",
            return_value=match_df,
        ):
            result = ingest_understat_match_stats(seasons=["2023/24"], use_cache=False)

        assert result.shape[0] == 1
        assert "season" in result.columns

    def test_uses_cache_when_available(self) -> None:
        """Test that cached data is used when available."""
        from src.data.ingest_understat import _cache_key, _save_cache

        cache_path = _cache_key("2023/24", "match_stats")
        cached_df = pl.DataFrame({"home_team": ["Arsenal"], "xg": [1.5]})
        _save_cache(cache_path, cached_df)

        with patch("src.data.ingest_understat._fetch_season_table") as mock_fetch:
            result = ingest_understat_match_stats(seasons=["2023/24"], use_cache=True)
            mock_fetch.assert_not_called()
            assert result.shape[0] == 1


class TestIngestUnderstatPlayerMatchStats:
    """Tests for ingest_understat_player_match_stats function."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory for each test."""
        monkeypatch.setattr(
            "src.data.ingest_understat.DATA_DIR", tmp_path / "understat"
        )

    def test_returns_data_when_available(self) -> None:
        """Test that player match stats are returned when fetch succeeds."""
        player_df = pl.DataFrame(
            {
                "player": ["Saka"],
                "xg": [0.5],
                "xa": [0.3],
                "shots": [3],
                "key_passes": [2],
                "minutes": [90],
            }
        )

        with patch(
            "src.data.ingest_understat._fetch_season_table",
            return_value=player_df,
        ):
            result = ingest_understat_player_match_stats(
                seasons=["2023/24"], use_cache=False
            )

        assert result.shape[0] == 1
        assert "season" in result.columns

    def test_uses_cache_when_available(self) -> None:
        """Test that cached data is used when available."""
        from src.data.ingest_understat import _cache_key, _save_cache

        cache_path = _cache_key("2023/24", "player_match_stats")
        cached_df = pl.DataFrame({"player": ["Salah"], "xg": [0.8], "xa": [0.4]})
        _save_cache(cache_path, cached_df)

        with patch("src.data.ingest_understat._fetch_season_table") as mock_fetch:
            result = ingest_understat_player_match_stats(
                seasons=["2023/24"], use_cache=True
            )
            mock_fetch.assert_not_called()
            assert result.shape[0] == 1


class TestIngestUnderstatPlayerSeasonStats:
    """Tests for ingest_understat_player_season_stats function."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory for each test."""
        monkeypatch.setattr(
            "src.data.ingest_understat.DATA_DIR", tmp_path / "understat"
        )

    def test_returns_data_when_available(self) -> None:
        """Test that player season stats are returned when fetch succeeds."""
        season_df = pl.DataFrame(
            {"player": ["Saka"], "xg": [12.5], "xa": [8.3], "shots": [80]}
        )

        with patch(
            "src.data.ingest_understat._fetch_season_table",
            return_value=season_df,
        ):
            result = ingest_understat_player_season_stats(
                seasons=["2023/24"], use_cache=False
            )

        assert result.shape[0] == 1
        assert "season" in result.columns

    def test_uses_cache_when_available(self) -> None:
        """Test that cached data is used when available."""
        from src.data.ingest_understat import _cache_key, _save_cache

        cache_path = _cache_key("2023/24", "player_season_stats")
        cached_df = pl.DataFrame({"player": ["Salah"], "xg": [22.0], "xa": [12.0]})
        _save_cache(cache_path, cached_df)

        with patch("src.data.ingest_understat._fetch_season_table") as mock_fetch:
            result = ingest_understat_player_season_stats(
                seasons=["2023/24"], use_cache=True
            )
            mock_fetch.assert_not_called()
            assert result.shape[0] == 1


class TestIngestUnderstat:
    """Tests for the main ingest_understat function."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary cache directory for each test."""
        monkeypatch.setattr(
            "src.data.ingest_understat.DATA_DIR", tmp_path / "understat"
        )

    def test_returns_all_four_dataframes(self) -> None:
        """Test that ingest_understat returns all four tables."""
        sample_df = pl.DataFrame({"player": ["Saka"], "xg": [0.5]})

        with patch(
            "src.data.ingest_understat._fetch_season_table",
            return_value=sample_df,
        ):
            result = ingest_understat(seasons=["2023/24"], use_cache=False)

        assert "shots" in result
        assert "match_stats" in result
        assert "player_match_stats" in result
        assert "player_season_stats" in result
        assert result["shots"].shape[0] == 1
        assert result["player_match_stats"].shape[0] == 1


class TestFetchSeasonTable:
    """Tests for _fetch_season_table generic helper."""

    def test_returns_empty_on_import_error(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that empty DataFrame is returned on ImportError."""
        with patch.dict("sys.modules", {"soccerdata": None}):
            from src.data.ingest_understat import _fetch_season_table

            result = _fetch_season_table("2023/24", "shots", "read_shot_events")
            assert result.is_empty()
            assert "soccerdata not installed" in caplog.text

    def test_returns_empty_on_general_error(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that empty DataFrame is returned on general exception."""
        mock_us_instance = MagicMock()
        mock_us_instance.read_shot_events.side_effect = Exception("Network error")
        mock_soccerdata = MagicMock()
        mock_soccerdata.Understat.return_value = mock_us_instance

        with patch.dict("sys.modules", {"soccerdata": mock_soccerdata}):
            from src.data.ingest_understat import _fetch_season_table

            result = _fetch_season_table("2023/24", "shots", "read_shot_events")
            assert result.is_empty()
            assert "Failed to fetch Understat shots" in caplog.text

    def test_calls_correct_reader_method(self) -> None:
        """Test that the correct reader method is called."""
        mock_us_instance = MagicMock()
        mock_us_instance.read_player_match_stats.return_value = pl.DataFrame(
            {"player": ["Saka"], "xg": [0.5]}
        )
        mock_soccerdata = MagicMock()
        mock_soccerdata.Understat.return_value = mock_us_instance

        with patch.dict("sys.modules", {"soccerdata": mock_soccerdata}):
            from src.data.ingest_understat import _fetch_season_table

            result = _fetch_season_table(
                "2023/24", "player_match_stats", "read_player_match_stats"
            )
            assert not result.is_empty()
            mock_us_instance.read_player_match_stats.assert_called_once()
