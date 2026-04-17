"""Integration tests for the full pipeline."""

from __future__ import annotations

import polars as pl

from src.data.clean import clean_data
from src.data.merge import merge_player_data
from src.features.rolling_features import compute_rolling_features


class TestFullPipeline:
    """End-to-end pipeline integration tests."""

    def test_ingest_clean_merge_flow(self) -> None:
        """Test data flows through ingestion, cleaning, and merging."""
        fpl_players = pl.DataFrame(
            {
                "player_id": [1, 2],
                "name": ["saka", "salah"],
                "team_id": [1, 12],
            }
        )
        vaastav_stats = pl.DataFrame(
            {
                "player_id": [1, 2],
                "total_points": [150, 200],
                "minutes": [2700, 3000],
            }
        )

        merged = merge_player_data(fpl_players, vaastav_stats)
        assert merged.shape[0] == 2
        assert "name" in merged.columns
        assert "total_points" in merged.columns

    def test_cleaning_preserves_data(self) -> None:
        """Test that cleaning doesn't drop valid rows."""
        df = pl.DataFrame(
            {
                "player_id": [1, 2, 3],
                "name": ["saka", "salah", "haaland"],
                "gameweek": [1, 2, 3],
                "minutes": [90, 0, 45],
                "goals": [1, 0, 2],
                "assists": [0, 1, 0],
                "points": [6, 2, 11],
            }
        )
        cleaned = clean_data(df)
        assert cleaned.shape[0] == df.shape[0]

    def test_rolling_features_pipeline(self) -> None:
        """Test rolling feature computation on multi-player data."""
        rows = []
        for pid in [1, 2]:
            for gw in range(1, 11):
                rows.append(
                    {
                        "player_id": pid,
                        "gameweek": gw,
                        "total_points": gw * 2,
                        "goals_scored": gw % 3,
                        "assists": (gw + 1) % 2,
                        "minutes": 90,
                    }
                )
        df = pl.DataFrame(rows)
        result = compute_rolling_features(df, use_cache=False, log_to_mlflow=False)
        assert "total_points_rolling_mean_3" in result.columns
        assert "total_points_rolling_mean_5" in result.columns
        assert "total_points_rolling_mean_10" in result.columns
