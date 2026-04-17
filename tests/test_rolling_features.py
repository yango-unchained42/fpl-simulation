"""Tests for rolling features module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.features.rolling_features import (
    compute_rolling_features,
)
from src.features.team_rolling_features import (
    compute_team_rolling_features,
)


class TestRollingFeatures:
    """Tests for compute_rolling_features function."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary features file for each test."""
        monkeypatch.setattr(
            "src.features.rolling_features.FEATURES_FILE",
            tmp_path / "features.parquet",
        )
        monkeypatch.setattr(
            "src.features.rolling_features.FEATURES_DIR",
            tmp_path,
        )

    def test_basic_rolling_mean(self) -> None:
        """Test rolling mean calculation for a single player."""
        df = pl.DataFrame(
            {
                "player_id": [1, 1, 1, 1, 1],
                "gameweek": [1, 2, 3, 4, 5],
                "total_points": [6, 8, 10, 4, 12],
            }
        )
        result = compute_rolling_features(
            df, windows=[3], use_cache=False, log_to_mlflow=False
        )

        # GW1: mean of [6] = 6.0
        # GW2: mean of [6, 8] = 7.0
        # GW3: mean of [6, 8, 10] = 8.0
        # GW4: mean of [8, 10, 4] = 7.333...
        # GW5: mean of [10, 4, 12] = 8.666...
        col = "total_points_rolling_mean_3"
        assert col in result.columns
        vals = result[col].to_list()
        assert vals[0] == pytest.approx(6.0)
        assert vals[1] == pytest.approx(7.0)
        assert vals[2] == pytest.approx(8.0)

    def test_rolling_sum(self) -> None:
        """Test rolling sum calculation for cumulative metrics."""
        df = pl.DataFrame(
            {
                "player_id": [1, 1, 1, 1],
                "gameweek": [1, 2, 3, 4],
                "goals_scored": [1, 0, 2, 1],
            }
        )
        result = compute_rolling_features(
            df, windows=[3], use_cache=False, log_to_mlflow=False
        )

        col = "goals_scored_rolling_sum_3"
        assert col in result.columns
        vals = result[col].to_list()
        assert vals[0] == 1  # [1]
        assert vals[1] == 1  # [1, 0]
        assert vals[2] == 3  # [1, 0, 2]
        assert vals[3] == 3  # [0, 2, 1]

    def test_multiple_windows(self) -> None:
        """Test that multiple window sizes create separate columns."""
        df = pl.DataFrame(
            {
                "player_id": [1] * 15,
                "gameweek": list(range(1, 16)),
                "total_points": list(range(1, 16)),
            }
        )
        result = compute_rolling_features(
            df, windows=[3, 5, 10], use_cache=False, log_to_mlflow=False
        )

        for w in [3, 5, 10]:
            col = f"total_points_rolling_mean_{w}"
            assert col in result.columns

    def test_multiple_players(self) -> None:
        """Test that rolling is computed per player independently."""
        df = pl.DataFrame(
            {
                "player_id": [1, 1, 1, 2, 2, 2],
                "gameweek": [1, 2, 3, 1, 2, 3],
                "total_points": [6, 8, 10, 2, 4, 6],
            }
        )
        result = compute_rolling_features(
            df, windows=[3], use_cache=False, log_to_mlflow=False
        )

        col = "total_points_rolling_mean_3"
        # Player 1: mean of [6, 8, 10] = 8.0
        p1_val = result.filter(pl.col("player_id") == 1)[col].to_list()[-1]
        assert p1_val == pytest.approx(8.0)
        # Player 2: mean of [2, 4, 6] = 4.0
        p2_val = result.filter(pl.col("player_id") == 2)[col].to_list()[-1]
        assert p2_val == pytest.approx(4.0)

    def test_excluded_columns_not_rolled(self) -> None:
        """Test that excluded columns don't get rolling features."""
        df = pl.DataFrame(
            {
                "player_id": [1, 1, 1],
                "gameweek": [1, 2, 3],
                "total_points": [6, 8, 10],
                "web_name": ["Saka", "Saka", "Saka"],
            }
        )
        result = compute_rolling_features(
            df, windows=[3], use_cache=False, log_to_mlflow=False
        )

        # Should have rolling feature for total_points
        assert "total_points_rolling_mean_3" in result.columns
        # Should NOT have rolling feature for web_name
        assert "web_name_rolling_mean_3" not in result.columns

    def test_empty_dataframe(self) -> None:
        """Test that empty DataFrame returns empty DataFrame."""
        df = pl.DataFrame(
            {"player_id": [], "gameweek": [], "total_points": []},
            schema={
                "player_id": pl.Int64,
                "gameweek": pl.Int64,
                "total_points": pl.Int64,
            },
        )
        result = compute_rolling_features(df, use_cache=False, log_to_mlflow=False)
        assert result.is_empty()

    def test_caching(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that features are cached and reused."""
        monkeypatch.setattr(
            "src.features.rolling_features.FEATURES_FILE",
            tmp_path / "features.parquet",
        )
        monkeypatch.setattr(
            "src.features.rolling_features.FEATURES_DIR",
            tmp_path,
        )

        df = pl.DataFrame(
            {
                "player_id": [1, 1],
                "gameweek": [1, 2],
                "total_points": [6, 8],
            }
        )

        # First call computes and caches
        result1 = compute_rolling_features(df, use_cache=True, log_to_mlflow=False)
        assert "total_points_rolling_mean_3" in result1.columns

        # Second call loads from cache
        result2 = compute_rolling_features(df, use_cache=True, log_to_mlflow=False)
        assert result2.shape == result1.shape

    def test_logs_to_mlflow(self) -> None:
        """Test that feature stats are logged to MLflow."""
        mock_mlflow = MagicMock()
        mock_mlflow.start_run.return_value.__enter__ = MagicMock()
        mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

        df = pl.DataFrame(
            {
                "player_id": [1, 1, 1],
                "gameweek": [1, 2, 3],
                "total_points": [6, 8, 10],
            }
        )

        with patch("src.utils.mlflow_client._get_mlflow", return_value=mock_mlflow):
            compute_rolling_features(df, use_cache=False, log_to_mlflow=True)

        mock_mlflow.log_param.assert_called()
        mock_mlflow.log_metric.assert_called()

    def test_defensive_metrics_rolled(self) -> None:
        """Test that defensive metrics get rolling features."""
        df = pl.DataFrame(
            {
                "player_id": [1, 1, 1],
                "gameweek": [1, 2, 3],
                "tackles": [3, 5, 2],
                "clean_sheets": [1, 0, 1],
                "recoveries": [10, 8, 12],
            }
        )
        result = compute_rolling_features(
            df, windows=[3], use_cache=False, log_to_mlflow=False
        )

        for metric in ["tackles", "clean_sheets", "recoveries"]:
            col = f"{metric}_rolling_mean_3"
            assert col in result.columns

    def test_advanced_metrics_rolled(self) -> None:
        """Test that advanced metrics get rolling features."""
        df = pl.DataFrame(
            {
                "player_id": [1, 1, 1],
                "gameweek": [1, 2, 3],
                "xg": [0.5, 0.8, 0.3],
                "xa": [0.2, 0.4, 0.1],
                "key_passes": [2, 3, 1],
            }
        )
        result = compute_rolling_features(
            df, windows=[3], use_cache=False, log_to_mlflow=False
        )

        for metric in ["xg", "xa", "key_passes"]:
            col = f"{metric}_rolling_mean_3"
            assert col in result.columns

    def test_ict_metrics_rolled(self) -> None:
        """Test that ICT components get rolling features."""
        df = pl.DataFrame(
            {
                "player_id": [1, 1, 1],
                "gameweek": [1, 2, 3],
                "influence": [50.0, 60.0, 45.0],
                "creativity": [30.0, 40.0, 25.0],
                "threat": [80.0, 90.0, 70.0],
            }
        )
        result = compute_rolling_features(
            df, windows=[3], use_cache=False, log_to_mlflow=False
        )

        for metric in ["influence", "creativity", "threat"]:
            col = f"{metric}_rolling_mean_3"
            assert col in result.columns


class TestTeamRollingFeatures:
    """Tests for team rolling features."""

    @pytest.fixture(autouse=True)
    def _clean_cache(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Use a temporary team features file for each test."""
        monkeypatch.setattr(
            "src.features.team_rolling_features.TEAM_FEATURES_FILE",
            tmp_path / "team_features.parquet",
        )
        monkeypatch.setattr(
            "src.features.team_rolling_features.FEATURES_DIR",
            tmp_path,
        )

    def test_team_rolling_mean(self) -> None:
        """Test team rolling mean calculation."""
        df = pl.DataFrame(
            {
                "team_id": [1, 1, 1, 1, 1],
                "gameweek": [1, 2, 3, 4, 5],
                "xg": [1.5, 2.0, 1.0, 2.5, 1.8],
            }
        )
        result = compute_team_rolling_features(
            df, windows=[3], use_cache=False, log_to_mlflow=False
        )

        col = "team_xg_rolling_mean_3"
        assert col in result.columns
        vals = result[col].to_list()
        assert vals[0] == pytest.approx(1.5)
        assert vals[1] == pytest.approx(1.75)
        assert vals[2] == pytest.approx(1.5)

    def test_team_rolling_sum(self) -> None:
        """Test team rolling sum calculation."""
        df = pl.DataFrame(
            {
                "team_id": [1, 1, 1, 1],
                "gameweek": [1, 2, 3, 4],
                "goals_scored": [2, 1, 3, 0],
            }
        )
        result = compute_team_rolling_features(
            df, windows=[3], use_cache=False, log_to_mlflow=False
        )

        col = "team_goals_scored_rolling_sum_3"
        assert col in result.columns
        vals = result[col].to_list()
        assert vals[0] == 2
        assert vals[1] == 3
        assert vals[2] == 6
        assert vals[3] == 4

    def test_multiple_teams(self) -> None:
        """Test that rolling is computed per team independently."""
        df = pl.DataFrame(
            {
                "team_id": [1, 1, 1, 2, 2, 2],
                "gameweek": [1, 2, 3, 1, 2, 3],
                "xg": [1.0, 2.0, 3.0, 0.5, 1.0, 1.5],
            }
        )
        result = compute_team_rolling_features(
            df, windows=[3], use_cache=False, log_to_mlflow=False
        )

        col = "team_xg_rolling_mean_3"
        t1_val = result.filter(pl.col("team_id") == 1)[col].to_list()[-1]
        assert t1_val == pytest.approx(2.0)
        t2_val = result.filter(pl.col("team_id") == 2)[col].to_list()[-1]
        assert t2_val == pytest.approx(1.0)

    def test_home_away_splits(self) -> None:
        """Test home/away split rolling features."""
        df = pl.DataFrame(
            {
                "team_id": [1, 1, 1, 1],
                "gameweek": [1, 2, 3, 4],
                "was_home": [True, False, True, False],
                "xg": [2.0, 1.0, 2.5, 0.8],
            }
        )
        result = compute_team_rolling_features(
            df, windows=[3], use_cache=False, log_to_mlflow=False
        )

        assert "team_xg_home_rolling_mean_3" in result.columns
        assert "team_xg_away_rolling_mean_3" in result.columns

    def test_excluded_columns_not_rolled(self) -> None:
        """Test that excluded columns don't get rolling features."""
        df = pl.DataFrame(
            {
                "team_id": [1, 1, 1],
                "gameweek": [1, 2, 3],
                "xg": [1.0, 2.0, 1.5],
                "name": ["Arsenal", "Arsenal", "Arsenal"],
            }
        )
        result = compute_team_rolling_features(
            df, windows=[3], use_cache=False, log_to_mlflow=False
        )

        assert "team_xg_rolling_mean_3" in result.columns
        assert "team_name_rolling_mean_3" not in result.columns

    def test_empty_team_data(self) -> None:
        """Test that empty team data returns empty DataFrame."""
        df = pl.DataFrame(
            {"team_id": [], "gameweek": [], "xg": []},
            schema={
                "team_id": pl.Int64,
                "gameweek": pl.Int64,
                "xg": pl.Float64,
            },
        )
        result = compute_team_rolling_features(df, use_cache=False, log_to_mlflow=False)
        assert result.is_empty()

    def test_caching(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that team features are cached and reused."""
        monkeypatch.setattr(
            "src.features.team_rolling_features.TEAM_FEATURES_FILE",
            tmp_path / "team_features.parquet",
        )
        monkeypatch.setattr(
            "src.features.team_rolling_features.FEATURES_DIR",
            tmp_path,
        )

        df = pl.DataFrame(
            {
                "team_id": [1, 1],
                "gameweek": [1, 2],
                "xg": [1.5, 2.0],
            }
        )

        result1 = compute_team_rolling_features(df, use_cache=True, log_to_mlflow=False)
        assert "team_xg_rolling_mean_3" in result1.columns

        result2 = compute_team_rolling_features(df, use_cache=True, log_to_mlflow=False)
        assert result2.shape == result1.shape

    def test_logs_to_mlflow(self) -> None:
        """Test that team feature stats are logged to MLflow."""
        mock_mlflow = MagicMock()
        mock_mlflow.start_run.return_value.__enter__ = MagicMock()
        mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

        df = pl.DataFrame(
            {
                "team_id": [1, 1, 1],
                "gameweek": [1, 2, 3],
                "xg": [1.5, 2.0, 1.0],
            }
        )

        with patch("src.utils.mlflow_client._get_mlflow", return_value=mock_mlflow):
            compute_team_rolling_features(df, use_cache=False, log_to_mlflow=True)

        mock_mlflow.log_param.assert_called()
        mock_mlflow.log_metric.assert_called()
