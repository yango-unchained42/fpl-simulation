"""Tests for granular Understat shot feature engineering."""

from __future__ import annotations

import polars as pl

from src.models.shot_features import (
    engineer_shot_quality_features,
    merge_shot_features_with_dataset,
)


class TestEngineerShotQualityFeatures:
    """Tests for engineer_shot_quality_features function."""

    def test_basic_shot_features(self) -> None:
        """Test basic shot feature engineering."""
        shot_data = pl.DataFrame(
            {
                "player_id": [1, 1, 1, 2, 2],
                "gameweek": [1, 1, 2, 1, 1],
                "xg": [0.5, 0.8, 0.3, 0.2, 0.1],
                "goal": [1, 0, 0, 0, 0],
                "situation": [
                    "OpenPlay",
                    "OpenPlay",
                    "OpenPlay",
                    "OpenPlay",
                    "OpenPlay",
                ],
                "body_part": [
                    "RightFoot",
                    "LeftFoot",
                    "RightFoot",
                    "Head",
                    "RightFoot",
                ],
                "location": [
                    "Centre of penalty area",
                    "Left side of penalty area",
                    "Outside box",
                    "Six yard box",
                    "Centre of penalty area",
                ],
            }
        )
        result = engineer_shot_quality_features(shot_data, use_cache=False)
        assert result.shape[0] == 3  # 3 player-gameweek pairs
        assert "avg_shot_xg" in result.columns
        assert "shot_frequency" in result.columns
        assert "conversion_rate" in result.columns
        assert "box_entry_rate" in result.columns

    def test_penalty_involvement(self) -> None:
        """Test penalty involvement detection."""
        shot_data = pl.DataFrame(
            {
                "player_id": [1, 1],
                "gameweek": [1, 1],
                "xg": [0.79, 0.5],
                "goal": [1, 0],
                "situation": ["Penalty", "OpenPlay"],
                "body_part": ["RightFoot", "RightFoot"],
                "location": ["Penalty spot", "Centre of penalty area"],
            }
        )
        result = engineer_shot_quality_features(shot_data, use_cache=False)
        assert result["penalty_involvement"].to_list()[0] == 1

    def test_set_piece_taking(self) -> None:
        """Test set piece taking detection."""
        shot_data = pl.DataFrame(
            {
                "player_id": [1, 1],
                "gameweek": [1, 1],
                "xg": [0.05, 0.3],
                "goal": [0, 0],
                "situation": ["Direct Freekick", "OpenPlay"],
                "body_part": ["RightFoot", "RightFoot"],
                "location": ["Outside box", "Centre of penalty area"],
            }
        )
        result = engineer_shot_quality_features(shot_data, use_cache=False)
        assert result["set_piece_taking"].to_list()[0] == 1

    def test_empty_shot_data(self) -> None:
        """Test that empty shot data returns empty DataFrame."""
        shot_data = pl.DataFrame(
            {
                "player_id": [],
                "gameweek": [],
                "xg": [],
                "goal": [],
                "situation": [],
                "body_part": [],
                "location": [],
            },
            schema={
                "player_id": pl.Int64,
                "gameweek": pl.Int64,
                "xg": pl.Float64,
                "goal": pl.Int64,
                "situation": pl.String,
                "body_part": pl.String,
                "location": pl.String,
            },
        )
        result = engineer_shot_quality_features(shot_data, use_cache=False)
        assert result.is_empty()

    def test_player_mapping(self) -> None:
        """Test player mapping from Understat to FPL IDs."""
        shot_data = pl.DataFrame(
            {
                "player_id": [100, 100],
                "gameweek": [1, 1],
                "xg": [0.5, 0.8],
                "goal": [1, 0],
                "situation": ["OpenPlay", "OpenPlay"],
                "body_part": ["RightFoot", "LeftFoot"],
                "location": ["Centre of penalty area", "Left side of penalty area"],
            }
        )
        player_mapping = pl.DataFrame(
            {
                "understat_player_id": [100, 200],
                "fpl_player_id": [1, 2],
            }
        )
        result = engineer_shot_quality_features(
            shot_data, player_mapping=player_mapping, use_cache=False
        )
        assert "player_id" in result.columns
        assert result["player_id"].to_list()[0] == 1


class TestMergeShotFeaturesWithDataset:
    """Tests for merge_shot_features_with_dataset function."""

    def test_basic_merge(self) -> None:
        """Test basic merge of shot features with main dataset."""
        main_dataset = pl.DataFrame(
            {
                "player_id": [1, 1, 2],
                "gameweek": [1, 2, 1],
                "total_points": [6, 8, 2],
            }
        )
        shot_features = pl.DataFrame(
            {
                "player_id": [1, 2],
                "gameweek": [1, 1],
                "avg_shot_xg": [0.65, 0.15],
                "shot_frequency": [2, 1],
                "conversion_rate": [0.5, 0.0],
                "box_entry_rate": [1.0, 0.0],
                "penalty_involvement": [0, 0],
                "set_piece_taking": [0, 0],
                "body_part_diversity": [2, 1],
            }
        )
        result = merge_shot_features_with_dataset(main_dataset, shot_features)
        assert result.shape[0] == 3
        assert "avg_shot_xg" in result.columns
        # Player 1 GW2 has no shot data, should be filled with 0
        gw2_row = result.filter((pl.col("player_id") == 1) & (pl.col("gameweek") == 2))
        assert gw2_row["avg_shot_xg"].to_list()[0] == 0.0

    def test_missing_shot_features(self) -> None:
        """Test that missing shot features are handled gracefully."""
        main_dataset = pl.DataFrame(
            {
                "player_id": [1, 2],
                "gameweek": [1, 1],
                "total_points": [6, 2],
            }
        )
        shot_features = pl.DataFrame(
            {
                "player_id": [],
                "gameweek": [],
                "avg_shot_xg": [],
            },
            schema={
                "player_id": pl.Int64,
                "gameweek": pl.Int64,
                "avg_shot_xg": pl.Float64,
            },
        )
        result = merge_shot_features_with_dataset(main_dataset, shot_features)
        assert result.shape[0] == 2
        assert result["avg_shot_xg"].to_list() == [0.0, 0.0]

    def test_custom_join_columns(self) -> None:
        """Test merge with custom join columns."""
        main_dataset = pl.DataFrame(
            {
                "fpl_id": [1, 2],
                "gw": [1, 1],
                "total_points": [6, 2],
            }
        )
        shot_features = pl.DataFrame(
            {
                "fpl_id": [1, 2],
                "gw": [1, 1],
                "avg_shot_xg": [0.65, 0.15],
            }
        )
        result = merge_shot_features_with_dataset(
            main_dataset, shot_features, on_cols=["fpl_id", "gw"]
        )
        assert result.shape[0] == 2
        assert "avg_shot_xg" in result.columns
