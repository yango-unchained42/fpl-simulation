"""Tests for feature engineering modules."""

from __future__ import annotations

import polars as pl

from src.features.engineer import engineer_features
from src.features.h2h_metrics import compute_player_vs_team, compute_team_h2h
from src.features.rolling_features import compute_rolling_features


class TestRollingFeatures:
    """Tests for rolling feature computation."""

    def test_computes_rolling_averages(self) -> None:
        df = pl.DataFrame(
            {
                "player_id": [1, 1, 1, 1, 1],
                "gameweek": [1, 2, 3, 4, 5],
                "total_points": [5, 10, 15, 20, 25],
                "goals_scored": [0, 1, 0, 2, 1],
                "assists": [1, 0, 1, 0, 1],
                "minutes": [90, 90, 90, 90, 90],
            }
        )
        result = compute_rolling_features(
            df, windows=[3], use_cache=False, log_to_mlflow=False
        )
        assert "total_points_rolling_mean_3" in result.columns
        assert "goals_scored_rolling_sum_3" in result.columns

    def test_default_windows(self) -> None:
        df = pl.DataFrame(
            {
                "player_id": [1] * 15,
                "gameweek": list(range(1, 16)),
                "total_points": list(range(1, 16)),
                "goals_scored": [0] * 15,
                "assists": [0] * 15,
                "minutes": [90] * 15,
            }
        )
        result = compute_rolling_features(df, use_cache=False, log_to_mlflow=False)
        for w in [3, 5, 10]:
            assert f"total_points_rolling_mean_{w}" in result.columns

    def test_missing_metric_columns(self) -> None:
        df = pl.DataFrame(
            {
                "player_id": [1, 1],
                "gameweek": [1, 2],
                "total_points": [5, 10],
            }
        )
        result = compute_rolling_features(
            df, windows=[2], use_cache=False, log_to_mlflow=False
        )
        assert "total_points_rolling_mean_2" in result.columns
        assert "goals_scored_rolling_mean_2" not in result.columns


class TestTeamH2H:
    """Tests for team H2H metrics."""

    def test_basic_h2h(self) -> None:
        matches = pl.DataFrame(
            {
                "home_team_id": [1, 1, 2],
                "away_team_id": [2, 2, 1],
                "home_goals": [2, 1, 0],
                "away_goals": [1, 1, 3],
                "season": ["2023-24", "2023-24", "2023-24"],
            }
        )
        result = compute_team_h2h(matches, use_cache=False, log_to_mlflow=False)
        assert "avg_home_goals" in result.columns
        assert "avg_away_goals" in result.columns

    def test_h2h_groups_by_team_pair(self) -> None:
        matches = pl.DataFrame(
            {
                "home_team_id": [1, 1],
                "away_team_id": [2, 2],
                "home_goals": [2, 4],
                "away_goals": [1, 0],
                "season": ["2023-24", "2023-24"],
            }
        )
        result = compute_team_h2h(matches, use_cache=False)
        assert result.shape[0] == 1


class TestPlayerVsTeam:
    """Tests for player vs team defense metrics."""

    def test_basic_pvt(self) -> None:
        stats = pl.DataFrame(
            {
                "player_id": [1, 1, 2],
                "opponent_team_id": [2, 2, 1],
                "points": [5, 10, 3],
                "xg": [0.5, 1.0, 0.2],
                "goals": [0, 1, 0],
                "shots": [3, 4, 1],
            }
        )
        result = compute_player_vs_team(stats, use_cache=False)
        assert "avg_points" in result.columns
        assert "avg_xg" in result.columns
        assert "avg_goals" in result.columns

    def test_pvt_aggregation(self) -> None:
        stats = pl.DataFrame(
            {
                "player_id": [1, 1],
                "opponent_team_id": [2, 2],
                "points": [5, 15],
                "xg": [0.5, 1.5],
                "goals": [0, 2],
                "shots": [3, 4],
            }
        )
        result = compute_player_vs_team(stats, use_cache=False)
        assert result.shape[0] == 1
        row = result.row(0, named=True)
        assert row["avg_points"] == 10.0  # type: ignore[comparison-overlap]


class TestEngineerFeatures:
    """Tests for full feature engineering pipeline."""

    def test_engineer_features_returns_dataframe(self) -> None:
        player_stats = pl.DataFrame(
            {
                "player_id": [1, 1],
                "gameweek": [1, 2],
                "total_points": [5, 10],
                "goals_scored": [0, 1],
                "assists": [1, 0],
                "minutes": [90, 90],
                "opponent_team_id": [2, 2],
                "xg": [0.5, 1.0],
            }
        )
        matches = pl.DataFrame(
            {
                "home_team_id": [1],
                "away_team_id": [2],
                "home_goals": [2],
                "away_goals": [1],
                "season": ["2023-24"],
            }
        )
        fixtures = pl.DataFrame(
            {
                "fixture_id": [1],
                "home_team_id": [1],
                "away_team_id": [2],
                "gameweek": [3],
            }
        )
        result = engineer_features(player_stats, matches, fixtures)
        assert isinstance(result, pl.DataFrame)
        assert result.shape[0] > 0
