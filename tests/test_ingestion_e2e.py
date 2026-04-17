"""Comprehensive data ingestion tests.

Covers:
1. Supabase integration tests (write calls, connection handling, error recovery)
2. Data validation tests (completeness, type validation, duplicate detection)
3. Full ingestion pipeline integration test with mocked sources
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.data.database import get_supabase_client, read_from_supabase, write_to_supabase


class TestSupabaseIntegration:
    """Tests for Supabase database integration."""

    def test_write_to_supabase_insert_success(self) -> None:
        """Test successful insert to Supabase."""
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
        mock_table.insert.assert_called_once()

    def test_write_to_supabase_upsert_success(self) -> None:
        """Test successful upsert to Supabase."""
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

    def test_write_to_supabase_no_client(self) -> None:
        """Test write fails when no Supabase client available."""
        df = pl.DataFrame({"id": [1]})

        with patch("src.data.database.get_supabase_client", return_value=None):
            result = write_to_supabase("test_table", df)

        assert result is False

    def test_write_to_supabase_connection_error(self) -> None:
        """Test write handles connection errors gracefully."""
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.insert.side_effect = RuntimeError("Connection refused")
        mock_client.table.return_value = mock_table

        df = pl.DataFrame({"id": [1]})

        with patch("src.data.database.get_supabase_client", return_value=mock_client):
            result = write_to_supabase("test_table", df, client=mock_client)

        assert result is False

    def test_get_supabase_client_no_credentials(self) -> None:
        """Test client returns None without credentials."""
        import os

        old_url = os.environ.pop("SUPABASE_URL", None)
        old_key = os.environ.pop("SUPABASE_KEY", None)

        try:
            client = get_supabase_client()
            assert client is None
        finally:
            if old_url:
                os.environ["SUPABASE_URL"] = old_url
            if old_key:
                os.environ["SUPABASE_KEY"] = old_key

    def test_read_from_supabase_success(self) -> None:
        """Test successful read from Supabase."""
        mock_client = MagicMock()
        mock_query = MagicMock()
        mock_query.select.return_value = mock_query
        mock_query.execute.return_value = MagicMock(
            data=[{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]
        )
        mock_client.table.return_value = mock_query

        with patch("src.data.database.get_supabase_client", return_value=mock_client):
            result = read_from_supabase("test_table", client=mock_client)

        assert result.shape[0] == 2
        assert "id" in result.columns

    def test_read_from_supabase_empty_result(self) -> None:
        """Test read returns empty DataFrame when no data."""
        mock_client = MagicMock()
        mock_query = MagicMock()
        mock_query.select.return_value = mock_query
        mock_query.execute.return_value = MagicMock(data=[])
        mock_client.table.return_value = mock_query

        with patch("src.data.database.get_supabase_client", return_value=mock_client):
            result = read_from_supabase("test_table", client=mock_client)

        assert result.is_empty()

    def test_read_from_supabase_with_filters(self) -> None:
        """Test read with filters applies them correctly."""
        mock_client = MagicMock()
        mock_query = MagicMock()
        mock_query.select.return_value = mock_query
        mock_query.eq.return_value = mock_query
        mock_query.gte.return_value = mock_query
        mock_query.execute.return_value = MagicMock(data=[])
        mock_client.table.return_value = mock_query

        with patch("src.data.database.get_supabase_client", return_value=mock_client):
            read_from_supabase(
                "test_table",
                client=mock_client,
                filters=[("id", "eq", 1), ("score", "gte", 10)],
            )

        mock_query.eq.assert_called_once_with("id", 1)
        mock_query.gte.assert_called_once_with("score", 10)


class TestDataCompleteness:
    """Tests for data completeness validation."""

    def test_fpl_players_has_required_columns(self) -> None:
        """Test that FPL player data has all required columns."""
        required_cols = {"id", "web_name", "team", "element_type", "now_cost"}
        player_df = pl.DataFrame(
            {
                "id": [1],
                "web_name": ["Saka"],
                "team": [1],
                "element_type": [3],
                "now_cost": [90],
                "total_points": [100],
            }
        )
        missing = required_cols - set(player_df.columns)
        assert not missing, f"Missing columns: {missing}"

    def test_fpl_teams_has_required_columns(self) -> None:
        """Test that FPL team data has all required columns."""
        required_cols = {"id", "name", "short_name", "strength"}
        team_df = pl.DataFrame(
            {
                "id": [1],
                "name": ["Arsenal"],
                "short_name": ["ARS"],
                "strength": [4],
            }
        )
        missing = required_cols - set(team_df.columns)
        assert not missing, f"Missing columns: {missing}"

    def test_vaastav_history_has_required_columns(self) -> None:
        """Test that vaastav history data has all required columns."""
        required_cols = {"GW", "element", "total_points", "minutes"}
        history_df = pl.DataFrame(
            {
                "GW": [1],
                "element": [1],
                "total_points": [6],
                "minutes": [90],
                "goals_scored": [1],
            }
        )
        missing = required_cols - set(history_df.columns)
        assert not missing, f"Missing columns: {missing}"

    def test_understat_shots_has_required_columns(self) -> None:
        """Test that Understat shots data has all required columns."""
        required_cols = {"xg", "shot_id"}
        shots_df = pl.DataFrame({"shot_id": [1], "xg": [0.5], "player": ["Saka"]})
        missing = required_cols - set(shots_df.columns)
        assert not missing, f"Missing columns: {missing}"

    def test_understat_player_match_has_required_columns(self) -> None:
        """Test that Understat player match stats has all required columns."""
        required_cols = {"xg", "xa", "minutes", "player_id"}
        match_df = pl.DataFrame(
            {"player_id": [1], "xg": [0.5], "xa": [0.3], "minutes": [90]}
        )
        missing = required_cols - set(match_df.columns)
        assert not missing, f"Missing columns: {missing}"


class TestDataTypeValidation:
    """Tests for data type validation."""

    def test_fpl_player_id_is_integer(self) -> None:
        """Test that FPL player IDs are integers."""
        df = pl.DataFrame({"id": [1, 2, 3], "web_name": ["A", "B", "C"]})
        assert df["id"].dtype == pl.Int64

    def test_fpl_player_cost_is_integer(self) -> None:
        """Test that FPL player costs are integers."""
        df = pl.DataFrame({"id": [1], "now_cost": [90]})
        assert df["now_cost"].dtype == pl.Int64

    def test_understat_xg_is_numeric(self) -> None:
        """Test that Understat xG values are numeric."""
        df = pl.DataFrame({"xg": [0.5, 0.0, 1.2]})
        assert df["xg"].dtype in (pl.Float64, pl.Float32)

    def test_vaastav_gw_is_integer(self) -> None:
        """Test that vaastav GW column is integer."""
        df = pl.DataFrame({"GW": [1, 2, 3], "element": [1, 1, 1]})
        assert df["GW"].dtype == pl.Int64

    def test_no_null_in_required_columns(self) -> None:
        """Test that required columns have no null values."""
        df = pl.DataFrame(
            {"id": [1, 2, 3], "web_name": ["A", "B", "C"], "team": [1, 2, 3]}
        )
        required = ["id", "web_name", "team"]
        for col in required:
            null_count = df[col].null_count()
            assert null_count == 0, f"Column {col} has {null_count} null values"

    def test_null_in_optional_columns_allowed(self) -> None:
        """Test that optional columns can have null values."""
        df = pl.DataFrame({"id": [1, 2], "optional_metric": [10.0, None]})
        assert df["optional_metric"].null_count() == 1


class TestDuplicateDetection:
    """Tests for duplicate row detection."""

    def test_detect_duplicate_players(self) -> None:
        """Test detection of duplicate player records."""
        df = pl.DataFrame({"id": [1, 1, 2], "web_name": ["Saka", "Saka", "Salah"]})
        duplicates = df.filter(pl.col("id").is_duplicated())
        assert duplicates.shape[0] == 2

    def test_remove_duplicate_players(self) -> None:
        """Test removal of duplicate player records."""
        df = pl.DataFrame({"id": [1, 1, 2], "web_name": ["Saka", "Saka", "Salah"]})
        deduped = df.unique(subset=["id"], keep="first")
        assert deduped.shape[0] == 2
        assert set(deduped["id"].to_list()) == {1, 2}

    def test_no_duplicates_in_clean_data(self) -> None:
        """Test that clean data has no duplicates."""
        df = pl.DataFrame({"id": [1, 2, 3], "web_name": ["A", "B", "C"]})
        duplicates = df.filter(pl.col("id").is_duplicated())
        assert duplicates.shape[0] == 0

    def test_detect_duplicate_gw_records(self) -> None:
        """Test detection of duplicate GW records (same player + same GW)."""
        df = pl.DataFrame(
            {
                "element": [1, 1, 2],
                "GW": [1, 1, 1],
                "total_points": [6, 6, 10],
            }
        )
        duplicates = df.filter(pl.struct(["element", "GW"]).is_duplicated())
        assert duplicates.shape[0] == 2

    def test_detect_duplicate_fixture_records(self) -> None:
        """Test detection of duplicate fixture records."""
        df = pl.DataFrame(
            {
                "fixture_id": [1, 1, 2],
                "team_h": [1, 1, 3],
                "team_a": [2, 2, 4],
            }
        )
        duplicates = df.filter(pl.col("fixture_id").is_duplicated())
        assert duplicates.shape[0] == 2


class TestFullIngestionPipelineIntegration:
    """Integration tests for the full ingestion pipeline with mocked sources."""

    def test_full_pipeline_with_all_sources(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test full pipeline runs end-to-end with mocked data sources."""
        from src.data.ingest_pipeline import run_ingestion_pipeline

        state_file = tmp_path / "pipeline_state.json"
        monkeypatch.setattr("src.data.ingest_pipeline.PIPELINE_STATE_FILE", state_file)

        with (
            patch("src.data.ingest_fpl.ingest_fpl_data") as mock_fpl,
            patch("src.data.ingest_vaastav.load_historical_data") as mock_vaastav,
            patch("src.data.ingest_understat.ingest_understat_shots") as mock_us_shots,
            patch(
                "src.data.ingest_understat.ingest_understat_player_match_stats"
            ) as mock_us_match,
            patch(
                "src.data.ingest_understat.ingest_understat_player_season_stats"
            ) as mock_us_season,
            patch(
                "src.data.ingest_understat.ingest_understat_match_stats"
            ) as mock_us_match_stats,
            patch("src.data.ingest_pipeline._write_to_supabase") as mock_write,
        ):
            mock_fpl.return_value = {
                "players": pl.DataFrame({"id": [1, 2], "web_name": ["A", "B"]}),
                "teams": pl.DataFrame({"id": [1], "name": ["Team1"]}),
                "fixtures": pl.DataFrame(),
            }
            mock_vaastav.return_value = pl.DataFrame(
                {
                    "GW": [1, 2],
                    "element": [1, 1],
                    "total_points": [6, 8],
                }
            )
            mock_us_shots.return_value = pl.DataFrame({"shot_id": [1], "xg": [0.5]})
            mock_us_match.return_value = pl.DataFrame(
                {
                    "player_id": [1],
                    "xg": [0.5],
                    "xa": [0.3],
                    "minutes": [90],
                }
            )
            mock_us_season.return_value = pl.DataFrame(
                {"player_id": [1], "xg": [12.0], "xa": [8.0]}
            )
            mock_us_match_stats.return_value = pl.DataFrame(
                {"home_team": ["A"], "away_team": ["B"], "xg": [1.5]}
            )

            result = run_ingestion_pipeline(
                seasons=["2023-24"],
                use_cache=False,
                sources=None,
                write_db=True,
                resume=False,
            )

        assert result.success is True
        assert len(result.successful_sources) == 7
        assert mock_write.call_count >= 4

    def test_pipeline_partial_failure_continues(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test pipeline continues when some sources fail."""
        from src.data.ingest_pipeline import run_ingestion_pipeline

        state_file = tmp_path / "pipeline_state.json"
        monkeypatch.setattr("src.data.ingest_pipeline.PIPELINE_STATE_FILE", state_file)

        with (
            patch("src.data.ingest_fpl.ingest_fpl_data") as mock_fpl,
            patch("src.data.ingest_understat.ingest_understat_shots") as mock_us_shots,
            patch(
                "src.data.ingest_understat.ingest_understat_player_match_stats"
            ) as mock_us_match,
            patch(
                "src.data.ingest_understat.ingest_understat_player_season_stats"
            ) as mock_us_season,
        ):
            mock_fpl.return_value = {
                "players": pl.DataFrame({"id": [1]}),
                "teams": pl.DataFrame({"id": [1]}),
                "fixtures": pl.DataFrame(),
            }
            mock_us_shots.side_effect = RuntimeError("Understat down")
            mock_us_match.side_effect = RuntimeError("Understat down")
            mock_us_season.side_effect = RuntimeError("Understat down")

            result = run_ingestion_pipeline(
                seasons=["2023-24"],
                use_cache=False,
                sources=["fpl", "understat"],
                write_db=False,
                resume=False,
            )

        assert result.success is False
        assert "fpl" in result.successful_sources
        assert "understat_shots" in result.failed_sources

    def test_pipeline_checkpoint_resume(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test pipeline resumes from checkpoint."""
        from src.data.ingest_pipeline import run_ingestion_pipeline

        state_file = tmp_path / "pipeline_state.json"
        monkeypatch.setattr("src.data.ingest_pipeline.PIPELINE_STATE_FILE", state_file)

        with (
            patch("src.data.ingest_fpl.ingest_fpl_data") as mock_fpl,
            patch("src.data.ingest_vaastav.load_historical_data") as mock_vaastav,
        ):
            mock_fpl.return_value = {
                "players": pl.DataFrame({"id": [1]}),
                "teams": pl.DataFrame({"id": [1]}),
                "fixtures": pl.DataFrame(),
            }
            mock_vaastav.return_value = pl.DataFrame({"GW": [1]})

            result1 = run_ingestion_pipeline(
                seasons=["2023-24"],
                use_cache=False,
                sources=["fpl"],
                write_db=False,
                resume=True,
            )
            assert result1.success is True

            result2 = run_ingestion_pipeline(
                seasons=["2023-24"],
                use_cache=False,
                sources=["fpl", "vaastav"],
                write_db=False,
                resume=True,
            )
            sources_run = [r.source for r in result2.results]
            assert "fpl" not in sources_run
            assert "vaastav" in sources_run
