"""Integration tests for the full data cleaning pipeline."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.data.clean import impute_missing_minutes, standardize_names
from src.data.database import write_to_supabase
from src.data.impute import run_imputation
from src.data.validate import run_validation
from src.features.h2h_metrics import compute_h2h_features
from src.utils.name_resolver import resolve_names, standardize_name


class TestNameFormatValidation:
    """Tests for name format validation across the pipeline."""

    def test_standardize_name_produces_first_last(self) -> None:
        """Test that standardize_name produces 'First Last' format."""
        assert standardize_name("saka, bukayo") == "Bukayo Saka"
        assert standardize_name("  SALAH  ") == "Salah"
        assert standardize_name("haaland (Captain)") == "Haaland"

    def test_dataframe_standardization(self) -> None:
        """Test DataFrame-level name standardization."""
        df = pl.DataFrame({"name": ["saka", "  SALAH  ", "haaland"]})
        result = standardize_names(df, name_col="name")
        names = result["name"].to_list()
        assert "Saka" in names
        assert "Salah" in names
        assert "Haaland" in names

    def test_name_resolution_accuracy(self) -> None:
        """Test name resolution accuracy with known variations."""
        source = [
            "Erling Braut Haaland",
            "Mohamed Salah Hamed Mahrous Ghaly",
            "Saka",
        ]
        target = ["Erling Haaland", "Mohamed Salah", "Bukayo Saka"]
        resolved, confidence = resolve_names(
            source, target, threshold=0.8, log_to_mlflow=False
        )
        assert resolved["Erling Braut Haaland"] == "Erling Haaland"
        assert resolved["Mohamed Salah Hamed Mahrous Ghaly"] == "Mohamed Salah"
        assert confidence["Erling Braut Haaland"] == 1.0
        assert confidence["Mohamed Salah Hamed Mahrous Ghaly"] == 1.0


class TestImputationQuality:
    """Tests for imputation quality across the pipeline."""

    def test_mean_imputation_preserves_distribution(self) -> None:
        """Test that mean imputation doesn't drastically shift distribution."""
        df = pl.DataFrame({"value": [10.0, 12.0, 11.0, None, 13.0, 10.0, 12.0]})
        original_mean = df["value"].drop_nulls().mean()
        result, _ = run_imputation(df, log_to_mlflow=False)
        imputed_mean = result["value"].mean()
        # Mean should be very close since we fill with mean
        assert abs(imputed_mean - original_mean) < 0.01  # type: ignore[operator]

    def test_forward_fill_preserves_trend(self) -> None:
        """Test that forward fill preserves time-series trends."""
        df = pl.DataFrame(
            {
                "player_id": [1, 1, 1, 1, 1],
                "gameweek": [1, 2, 3, 4, 5],
                "cumulative_points": [6, None, 14, None, 26],
            }
        )
        result, _ = run_imputation(
            df, strategies={"cumulative_points": "forward_fill"}, log_to_mlflow=False
        )
        # Forward fill should propagate the last known value
        values = result["cumulative_points"].to_list()
        assert values[1] == 6.0  # Filled from GW 1
        assert values[3] == 14.0  # Filled from GW 3

    def test_over_imputation_prevention(self) -> None:
        """Test that columns with too many nulls are not imputed."""
        df = pl.DataFrame(
            {
                "good_col": [1.0, 2.0, None, 4.0],  # 25% nulls
                "bad_col": [None, None, None, 1.0],  # 75% nulls
            }
        )
        result, report = run_imputation(df, max_null_ratio=0.5, log_to_mlflow=False)
        assert "good_col" in report.columns_imputed
        assert "bad_col" in report.columns_rejected


class TestValidationRules:
    """Tests for validation rule enforcement."""

    def test_full_validation_on_clean_data(self) -> None:
        """Test that clean data passes all validation rules."""
        df = pl.DataFrame(
            {
                "player_id": [1, 2, 3],
                "gameweek": [1, 2, 3],
                "minutes": [90, 0, 45],
                "goals": [1, 0, 0],
                "points": [6, 2, 3],
            }
        )
        config = {
            "required_columns": ["player_id", "gameweek", "minutes"],
            "range_checks": {
                "gameweek": (1, 38),
                "minutes": (0, 120),
                "goals": (0, None),
                "points": (0, None),
            },
            "consistency_rules": [
                {"type": "non_negative", "columns": ["minutes", "goals", "points"]}
            ],
            "critical_columns": ["player_id", "gameweek"],
            "composite_keys": [["player_id", "gameweek"]],
        }
        report = run_validation(df, config, log_to_mlflow=False)
        assert report.is_valid is True
        assert report.error_count == 0

    def test_validation_catches_bad_data(self) -> None:
        """Test that validation catches invalid data."""
        df = pl.DataFrame(
            {
                "player_id": [1, 1, 3],  # duplicate composite key
                "gameweek": [1, 1, 40],  # duplicate + out of range
                "minutes": [90, 0, -10],  # negative
                "goals": [1, 0, 0],
                "points": [6, 2, 3],
            }
        )
        config = {
            "required_columns": ["player_id", "gameweek", "minutes"],
            "range_checks": {
                "gameweek": (1, 38),
                "minutes": (0, 120),
            },
            "consistency_rules": [{"type": "non_negative", "columns": ["minutes"]}],
            "composite_keys": [["player_id", "gameweek"]],
        }
        report = run_validation(df, config, log_to_mlflow=False)
        assert report.is_valid is False
        assert report.error_count >= 1


class TestH2HCalculationAccuracy:
    """Tests for H2H calculation accuracy."""

    def test_team_h2h_goals_calculation(self) -> None:
        """Test that team H2H goals are calculated correctly."""
        matches = pl.DataFrame(
            {
                "home_team_id": [1, 1, 1],
                "away_team_id": [2, 2, 2],
                "home_goals": [2, 3, 1],
                "away_goals": [1, 0, 2],
                "season": ["2023-24"] * 3,
            }
        )
        result = compute_h2h_features(
            matches,
            pl.DataFrame(
                {
                    "player_id": [1],
                    "opponent_team_id": [2],
                    "points": [6],
                    "xg": [0.5],
                    "goals": [1],
                    "shots": [3],
                    "season": ["2023-24"],
                }
            ),
            use_cache=False,
            log_to_mlflow=False,
        )
        team_h2h = result["team_h2h"]
        # Avg goals scored by home team 1 vs away team 2: (2+3+1)/3 = 2.0
        assert team_h2h["avg_home_goals"].to_list()[0] == pytest.approx(2.0)

    def test_player_vs_team_points_calculation(self) -> None:
        """Test that player vs team points are calculated correctly."""
        stats = pl.DataFrame(
            {
                "player_id": [1, 1, 1],
                "opponent_team_id": [2, 2, 2],
                "points": [6, 8, 10],
                "xg": [0.5, 0.8, 1.0],
                "goals": [1, 1, 2],
                "shots": [3, 4, 5],
                "season": ["2023-24"] * 3,
            }
        )
        result = compute_h2h_features(
            pl.DataFrame(
                {
                    "home_team_id": [1],
                    "away_team_id": [2],
                    "home_goals": [2],
                    "away_goals": [1],
                    "season": ["2023-24"],
                }
            ),
            stats,
            use_cache=False,
            log_to_mlflow=False,
        )
        player_vs_team = result["player_vs_team"]
        # Avg points: (6+8+10)/3 = 8.0
        assert player_vs_team["avg_points"].to_list()[0] == pytest.approx(8.0)


class TestFullCleaningPipeline:
    """Integration tests for the full cleaning pipeline."""

    def test_end_to_end_cleaning(self) -> None:
        """Test the full cleaning pipeline end-to-end."""
        # Raw data with issues
        raw_df = pl.DataFrame(
            {
                "player_id": [1, 2, 3],
                "gameweek": [1, 2, 3],
                "name": ["saka, bukayo", "  SALAH  ", "haaland"],
                "minutes": [90, None, 45],
                "goals": [1, 0, 0],
                "assists": [0, 1, 0],
                "points": [6, 2, 3],
            }
        )

        # Step 1: Name standardization (basic strip+titlecase)
        df = standardize_names(raw_df, name_col="name")
        names = df["name"].to_list()
        assert "Salah" in names
        assert "Haaland" in names

        # Step 2: Missing data imputation
        df = impute_missing_minutes(df)
        assert df["minutes"].null_count() == 0

        # Step 3: Validation
        config = {
            "required_columns": ["player_id", "gameweek", "minutes"],
            "range_checks": {"gameweek": (1, 38), "minutes": (0, 120)},
            "consistency_rules": [
                {"type": "non_negative", "columns": ["minutes", "goals", "points"]}
            ],
        }
        report = run_validation(df, config, log_to_mlflow=False)
        assert report.is_valid is True

    def test_pipeline_with_h2h_features(self) -> None:
        """Test cleaning pipeline with H2H feature generation."""
        # Clean player data
        pl.DataFrame(
            {
                "player_id": [1, 2, 3],
                "gameweek": [1, 2, 3],
                "name": ["Saka", "Salah", "Haaland"],
                "minutes": [90, 90, 90],
                "goals": [1, 2, 1],
                "assists": [0, 1, 0],
                "points": [6, 12, 6],
            }
        )

        # Match data for H2H
        matches = pl.DataFrame(
            {
                "home_team_id": [1, 2],
                "away_team_id": [2, 1],
                "home_goals": [2, 1],
                "away_goals": [1, 0],
                "season": ["2023-24", "2023-24"],
            }
        )

        # Player stats for H2H
        stats = pl.DataFrame(
            {
                "player_id": [1, 1, 2],
                "opponent_team_id": [2, 2, 1],
                "points": [6, 8, 2],
                "xg": [0.5, 0.8, 0.1],
                "goals": [1, 1, 0],
                "shots": [3, 4, 1],
                "season": ["2023-24", "2023-24", "2023-24"],
            }
        )

        h2h_result = compute_h2h_features(
            matches, stats, use_cache=False, write_db=False, log_to_mlflow=False
        )
        assert "team_h2h" in h2h_result
        assert "player_vs_team" in h2h_result
        assert h2h_result["team_h2h"].shape[0] == 2
        assert h2h_result["player_vs_team"].shape[0] == 2


class TestSupabaseIntegration:
    """Tests for Supabase integration in cleaning pipeline."""

    def test_write_to_supabase_calls_client(self) -> None:
        """Test that write_to_supabase calls the Supabase client."""
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_insert = MagicMock()
        mock_table.insert.return_value = mock_insert
        mock_client.table.return_value = mock_table

        df = pl.DataFrame({"id": [1, 2], "name": ["A", "B"]})

        with patch("src.data.database.get_supabase_client", return_value=mock_client):
            result = write_to_supabase("test_table", df, client=mock_client)

        assert result is True
        mock_client.table.assert_called_once_with("test_table")

    def test_write_to_supabase_upsert(self) -> None:
        """Test that write_to_supabase uses upsert when requested."""
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_upsert = MagicMock()
        mock_table.upsert.return_value = mock_upsert
        mock_client.table.return_value = mock_table

        df = pl.DataFrame({"id": [1], "name": ["A"]})

        with patch("src.data.database.get_supabase_client", return_value=mock_client):
            result = write_to_supabase(
                "test_table", df, client=mock_client, upsert=True
            )

        assert result is True
        mock_table.upsert.assert_called_once()

    def test_write_to_supabase_handles_connection_error(self) -> None:
        """Test that write_to_supabase handles connection errors gracefully."""
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.insert.side_effect = RuntimeError("Connection refused")
        mock_client.table.return_value = mock_table

        df = pl.DataFrame({"id": [1]})

        with patch("src.data.database.get_supabase_client", return_value=mock_client):
            result = write_to_supabase("test_table", df, client=mock_client)

        assert result is False

    def test_write_to_supabase_no_client(self) -> None:
        """Test that write_to_supabase returns False without client."""
        df = pl.DataFrame({"id": [1]})

        with patch("src.data.database.get_supabase_client", return_value=None):
            result = write_to_supabase("test_table", df)

        assert result is False
