"""Tests for data source integration (crosswalk + unified merge)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.data.crosswalk import build_understat_fpl_crosswalk
from src.data.merge_unified import (
    _standardize_fpl_history,
    _standardize_understat_pms,
    _standardize_vaastav_gw,
    create_unified_player_gw,
)


class TestCrosswalk:
    """Tests for Understat→FPL player ID crosswalk."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary crosswalk file for each test."""
        monkeypatch.setattr(
            "src.data.crosswalk.CROSSWALK_FILE",
            tmp_path / "crosswalk.parquet",
        )
        monkeypatch.setattr(
            "src.data.crosswalk.CROSSWALK_DIR",
            tmp_path,
        )

    def test_exact_name_match(self) -> None:
        """Test exact name matching."""
        us_data = pl.DataFrame({"player_id": [100, 200], "player": ["Salah", "Saka"]})
        fpl_players = pl.DataFrame({"id": [1, 2], "web_name": ["Salah", "Saka"]})
        crosswalk = build_understat_fpl_crosswalk(
            us_data, fpl_players, use_cache=False, log_to_mlflow=False
        )
        assert crosswalk.shape[0] == 2
        assert (
            crosswalk.filter(pl.col("understat_name") == "Salah")[
                "fpl_player_id"
            ].to_list()[0]
            == 1
        )

    def test_fuzzy_name_match(self) -> None:
        """Test fuzzy name matching."""
        us_data = pl.DataFrame({"player_id": [100], "player": ["M Salah"]})
        fpl_players = pl.DataFrame({"id": [1], "web_name": ["Salah"]})
        crosswalk = build_understat_fpl_crosswalk(
            us_data,
            fpl_players,
            threshold=0.5,
            use_cache=False,
            log_to_mlflow=False,
        )
        assert crosswalk.shape[0] == 1
        assert crosswalk["fpl_player_id"].to_list()[0] == 1

    def test_unmatched_player(self) -> None:
        """Test that unmatched players get null fpl_player_id."""
        us_data = pl.DataFrame({"player_id": [999], "player": ["UnknownPlayer"]})
        fpl_players = pl.DataFrame({"id": [1], "web_name": ["Salah"]})
        crosswalk = build_understat_fpl_crosswalk(
            us_data,
            fpl_players,
            threshold=0.9,
            use_cache=False,
            log_to_mlflow=False,
        )
        assert crosswalk.shape[0] == 1
        assert crosswalk["fpl_player_id"].to_list()[0] is None

    def test_caching(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that crosswalk is cached and reused."""
        monkeypatch.setattr(
            "src.data.crosswalk.CROSSWALK_FILE",
            tmp_path / "crosswalk.parquet",
        )
        monkeypatch.setattr(
            "src.data.crosswalk.CROSSWALK_DIR",
            tmp_path,
        )

        us_data = pl.DataFrame({"player_id": [100], "player": ["Salah"]})
        fpl_players = pl.DataFrame({"id": [1], "web_name": ["Salah"]})

        cw1 = build_understat_fpl_crosswalk(
            us_data, fpl_players, use_cache=True, log_to_mlflow=False
        )
        assert cw1.shape[0] == 1

        cw2 = build_understat_fpl_crosswalk(
            us_data, fpl_players, use_cache=True, log_to_mlflow=False
        )
        assert cw2.shape[0] == 1

    def test_logs_to_mlflow(self) -> None:
        """Test that crosswalk stats are logged to MLflow."""
        mock_mlflow = MagicMock()
        mock_mlflow.start_run.return_value.__enter__ = MagicMock()
        mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

        us_data = pl.DataFrame({"player_id": [100, 200], "player": ["Salah", "Saka"]})
        fpl_players = pl.DataFrame({"id": [1, 2], "web_name": ["Salah", "Saka"]})

        with patch("src.utils.mlflow_client._get_mlflow", return_value=mock_mlflow):
            build_understat_fpl_crosswalk(
                us_data, fpl_players, use_cache=False, log_to_mlflow=True
            )

        mock_mlflow.log_param.assert_called()
        mock_mlflow.log_metric.assert_called()


class TestStandardizeFPLHistory:
    """Tests for FPL history column standardization."""

    def test_renames_element_to_player_id(self) -> None:
        """Test that 'element' is renamed to 'player_id'."""
        df = pl.DataFrame({"element": [1, 2], "round": [1, 2]})
        result = _standardize_fpl_history(df)
        assert "player_id" in result.columns
        assert "gameweek" in result.columns
        assert "element" not in result.columns

    def test_no_rename_needed(self) -> None:
        """Test that already-standard columns are unchanged."""
        df = pl.DataFrame({"player_id": [1, 2], "gameweek": [1, 2]})
        result = _standardize_fpl_history(df)
        assert result.columns == ["player_id", "gameweek"]


class TestStandardizeVaastavGW:
    """Tests for Vaastav GW column standardization."""

    def test_renames_element_and_gw(self) -> None:
        """Test that 'element' and 'GW' are renamed."""
        df = pl.DataFrame({"element": [1, 2], "GW": [1, 2]})
        result = _standardize_vaastav_gw(df)
        assert "player_id" in result.columns
        assert "gameweek" in result.columns

    def test_no_rename_needed(self) -> None:
        """Test that already-standard columns are unchanged."""
        df = pl.DataFrame({"player_id": [1, 2], "gameweek": [1, 2]})
        result = _standardize_vaastav_gw(df)
        assert result.columns == ["player_id", "gameweek"]


class TestStandardizeUnderstatPMS:
    """Tests for Understat player match stats standardization."""

    def test_maps_understat_id_to_fpl_id(self) -> None:
        """Test that Understat player_id is mapped to FPL player_id."""
        us_data = pl.DataFrame(
            {"player_id": [100, 200], "xg": [0.5, 0.8], "goals": [1, 0]}
        )
        crosswalk = pl.DataFrame(
            {
                "understat_player_id": [100, 200],
                "fpl_player_id": [1, 2],
            }
        )
        result = _standardize_understat_pms(us_data, crosswalk)
        assert "player_id" in result.columns
        assert result["player_id"].to_list() == [1, 2]

    def test_filters_unmatched_players(self) -> None:
        """Test that unmatched players are filtered out."""
        us_data = pl.DataFrame({"player_id": [100, 999], "xg": [0.5, 0.8]})
        crosswalk = pl.DataFrame(
            {
                "understat_player_id": [100],
                "fpl_player_id": [1],
            }
        )
        result = _standardize_understat_pms(us_data, crosswalk)
        assert result.shape[0] == 1
        assert result["player_id"].to_list()[0] == 1

    def test_returns_empty_for_no_crosswalk(self) -> None:
        """Test that empty crosswalk returns empty DataFrame."""
        us_data = pl.DataFrame({"player_id": [100], "xg": [0.5]})
        crosswalk = pl.DataFrame(
            {"understat_player_id": [], "fpl_player_id": []},
            schema={"understat_player_id": pl.Int64, "fpl_player_id": pl.Int64},
        )
        result = _standardize_understat_pms(us_data, crosswalk)
        assert result.is_empty()


class TestCreateUnifiedPlayerGW:
    """Tests for unified player-gameweek table creation."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary unified file for each test."""
        monkeypatch.setattr(
            "src.data.merge_unified.UNIFIED_FILE",
            tmp_path / "unified.parquet",
        )
        monkeypatch.setattr(
            "src.data.merge_unified.UNIFIED_DIR",
            tmp_path,
        )

    def test_merges_fpl_and_vaastav(self) -> None:
        """Test that FPL and Vaastav data are merged correctly."""
        fpl_history = pl.DataFrame(
            {"element": [1, 1], "round": [1, 2], "total_points": [6, 8]}
        )
        vaastav_gw = pl.DataFrame(
            {"element": [1, 1], "GW": [1, 2], "minutes": [90, 90]}
        )
        understat_pms = pl.DataFrame()
        fpl_players = pl.DataFrame(
            {
                "id": [1],
                "web_name": ["Salah"],
                "team": [1],
                "element_type": [3],
                "now_cost": [130],
                "status": ["a"],
            }
        )
        crosswalk = pl.DataFrame(
            {"understat_player_id": [], "fpl_player_id": []},
            schema={"understat_player_id": pl.Int64, "fpl_player_id": pl.Int64},
        )

        result = create_unified_player_gw(
            fpl_history,
            vaastav_gw,
            understat_pms,
            fpl_players,
            crosswalk,
            use_cache=False,
        )

        assert result.shape[0] == 2
        assert "player_id" in result.columns
        assert "gameweek" in result.columns
        assert "total_points" in result.columns
        assert "minutes" in result.columns
        assert "web_name" in result.columns

    def test_merges_understat_via_crosswalk(self) -> None:
        """Test that Understat data is merged via crosswalk."""
        fpl_history = pl.DataFrame({"element": [1], "round": [1], "total_points": [6]})
        vaastav_gw = pl.DataFrame({"element": [1], "GW": [1], "minutes": [90]})
        understat_pms = pl.DataFrame({"player_id": [100], "xg": [0.5], "goals": [1]})
        fpl_players = pl.DataFrame(
            {
                "id": [1],
                "web_name": ["Salah"],
                "team": [1],
                "element_type": [3],
                "now_cost": [130],
                "status": ["a"],
            }
        )
        crosswalk = pl.DataFrame({"understat_player_id": [100], "fpl_player_id": [1]})

        result = create_unified_player_gw(
            fpl_history,
            vaastav_gw,
            understat_pms,
            fpl_players,
            crosswalk,
            use_cache=False,
        )

        assert result.shape[0] >= 1
        assert "xg" in result.columns
