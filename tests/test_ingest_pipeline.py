"""Tests for data ingestion pipeline orchestration."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import polars as pl

from src.data.ingest_pipeline import (
    IngestionResult,
    PipelineResult,
    _check_data_freshness,
    _run_ingestion_step_with_retry,
    create_parser,
    run_ingestion_pipeline,
)


class TestIngestionResult:
    """Tests for IngestionResult dataclass."""

    def test_successful_result(self) -> None:
        """Test creating a successful ingestion result."""
        result = IngestionResult(
            source="fpl", success=True, row_count=100, duration_seconds=1.5
        )
        assert result.source == "fpl"
        assert result.success is True
        assert result.row_count == 100
        assert result.error_message is None

    def test_failed_result(self) -> None:
        """Test creating a failed ingestion result."""
        result = IngestionResult(
            source="fpl",
            success=False,
            row_count=0,
            duration_seconds=0.5,
            error_message="Connection error",
        )
        assert result.success is False
        assert result.error_message == "Connection error"


class TestPipelineResult:
    """Tests for PipelineResult dataclass."""

    def test_successful_sources(self) -> None:
        """Test successful_sources property."""
        results = [
            IngestionResult("fpl", True, 100, 1.0),
            IngestionResult("vaastav", False, 0, 0.5, error_message="Error"),
            IngestionResult("understat", True, 50, 2.0),
        ]
        pipeline = PipelineResult(results=results, total_duration_seconds=3.5)
        assert pipeline.successful_sources == ["fpl", "understat"]
        assert pipeline.failed_sources == ["vaastav"]
        assert pipeline.success is False

    def test_all_successful(self) -> None:
        """Test pipeline with all successful sources."""
        results = [
            IngestionResult("fpl", True, 100, 1.0),
            IngestionResult("vaastav", True, 200, 2.0),
        ]
        pipeline = PipelineResult(results=results, total_duration_seconds=3.0)
        assert pipeline.success is True
        assert pipeline.failed_sources == []

    def test_summary_format(self) -> None:
        """Test summary output format."""
        results = [
            IngestionResult("fpl", True, 100, 1.0),
        ]
        pipeline = PipelineResult(results=results, total_duration_seconds=1.0)
        summary = pipeline.summary()
        assert "fpl" in summary
        assert "OK" in summary
        assert "100 rows" in summary


class TestDataFreshness:
    """Tests for _check_data_freshness function."""

    def test_empty_dataframe_fails(self) -> None:
        """Test that empty DataFrame fails freshness check."""
        df = pl.DataFrame()
        is_fresh, msg = _check_data_freshness(df, ["id"])
        assert is_fresh is False
        assert "empty" in msg.lower()

    def test_missing_columns_fails(self) -> None:
        """Test that missing required columns fail freshness check."""
        df = pl.DataFrame({"name": ["Saka"]})
        is_fresh, msg = _check_data_freshness(df, ["id", "name"])
        assert is_fresh is False
        assert "id" in msg

    def test_insufficient_rows_fails(self) -> None:
        """Test that insufficient rows fail freshness check."""
        df = pl.DataFrame({"id": [1]})
        is_fresh, msg = _check_data_freshness(df, ["id"], min_rows=5)
        assert is_fresh is False
        assert "Insufficient" in msg

    def test_valid_data_passes(self) -> None:
        """Test that valid data passes freshness check."""
        df = pl.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        is_fresh, msg = _check_data_freshness(df, ["id", "name"], min_rows=2)
        assert is_fresh is True
        assert "passes" in msg.lower()


class TestRunIngestionStep:
    """Tests for _run_ingestion_step_with_retry function."""

    def test_successful_step(self) -> None:
        """Test successful ingestion step."""
        mock_fn = MagicMock(return_value=pl.DataFrame({"id": [1, 2, 3]}))
        result, data = _run_ingestion_step_with_retry(
            "test", mock_fn, required_columns=["id"]
        )
        assert result.success is True
        assert result.row_count == 3
        assert result.error_message is None
        assert data is not None
        assert data.shape[0] == 3

    def test_step_with_exception(self) -> None:
        """Test ingestion step that raises an exception."""
        mock_fn = MagicMock(side_effect=RuntimeError("Connection failed"))
        result, data = _run_ingestion_step_with_retry("test", mock_fn, max_retries=0)
        assert result.success is False
        assert result.row_count == 0
        assert "Connection failed" in result.error_message  # type: ignore[operator]
        assert data is None

    def test_step_freshness_check_failure(self) -> None:
        """Test ingestion step that fails freshness check."""
        mock_fn = MagicMock(return_value=pl.DataFrame({"name": ["A"]}))
        result, data = _run_ingestion_step_with_retry(
            "test", mock_fn, required_columns=["id", "name"], max_retries=0
        )
        assert result.success is False
        assert "Freshness check" in result.error_message  # type: ignore[operator]
        assert data is None


class TestRunIngestionPipeline:
    """Tests for run_ingestion_pipeline function."""

    def test_runs_all_sources_by_default(self) -> None:
        """Test that all sources are run by default."""
        with (
            patch("src.data.ingest_fpl.ingest_fpl_data") as mock_fpl,
            patch("src.data.ingest_vaastav.load_historical_data") as mock_vaastav,
            patch(
                "src.data.ingest_understat.ingest_understat_shots"
            ) as mock_understat_shots,
            patch(
                "src.data.ingest_understat.ingest_understat_player_match_stats"
            ) as mock_understat_player_match,
            patch(
                "src.data.ingest_understat.ingest_understat_player_season_stats"
            ) as mock_understat_player_season,
            patch(
                "src.data.ingest_understat.ingest_understat_match_stats"
            ) as mock_understat_match,
        ):
            mock_fpl.return_value = {"players": pl.DataFrame({"id": [1]})}
            mock_vaastav.return_value = pl.DataFrame({"GW": [1]})
            mock_understat_shots.return_value = pl.DataFrame()
            mock_understat_player_match.return_value = pl.DataFrame()
            mock_understat_player_season.return_value = pl.DataFrame()
            mock_understat_match.return_value = pl.DataFrame()

            result = run_ingestion_pipeline(
                seasons=["2023-24"], use_cache=False, sources=None, resume=False
            )

        sources_run = [r.source for r in result.results]
        assert "fpl" in sources_run
        assert "vaastav" in sources_run
        assert "understat_shots" in sources_run
        assert "understat_player_match" in sources_run
        assert "understat_player_season" in sources_run
        assert "understat_match_stats" in sources_run

    def test_runs_only_specified_sources(self) -> None:
        """Test that only specified sources are run."""
        with (
            patch("src.data.ingest_fpl.ingest_fpl_data") as mock_fpl,
            patch("src.data.ingest_vaastav.load_historical_data") as mock_vaastav,
        ):
            mock_fpl.return_value = {"players": pl.DataFrame({"id": [1]})}
            mock_vaastav.return_value = pl.DataFrame({"GW": [1]})

            result = run_ingestion_pipeline(
                seasons=["2023-24"], use_cache=False, sources=["fpl"], resume=False
            )

        assert len(result.results) == 1
        assert result.results[0].source == "fpl"
        mock_vaastav.assert_not_called()

    def test_continues_on_fpl_failure(self) -> None:
        """Test that pipeline continues when FPL fails."""
        with (
            patch("src.data.ingest_fpl.ingest_fpl_data") as mock_fpl,
            patch("src.data.ingest_vaastav.load_historical_data") as mock_vaastav,
            patch(
                "src.data.ingest_vaastav.fetch_season_history"
            ) as mock_vaastav_season,
        ):
            mock_fpl.side_effect = RuntimeError("FPL API down")
            mock_vaastav.return_value = pl.DataFrame({"GW": [1]})
            mock_vaastav_season.return_value = pl.DataFrame({"id": [1]})

            result = run_ingestion_pipeline(
                seasons=["2023-24"],
                use_cache=False,
                sources=["fpl", "vaastav"],
                resume=False,
            )

        # FPL failed, vaastav + vaastav_season succeeded
        assert len(result.results) == 3
        assert result.results[0].success is False
        assert result.results[1].success is True


class TestCreateParser:
    """Tests for create_parser function."""

    def test_parser_has_required_arguments(self) -> None:
        """Test that parser has all required arguments."""
        parser = create_parser()
        args = parser.parse_args([])
        assert args.sources is None
        assert args.seasons is None
        assert args.no_cache is False
        assert args.verbose is False

    def test_parser_accepts_sources(self) -> None:
        """Test that parser accepts sources argument."""
        parser = create_parser()
        args = parser.parse_args(["--sources", "fpl", "vaastav"])
        assert args.sources == ["fpl", "vaastav"]

    def test_parser_accepts_seasons(self) -> None:
        """Test that parser accepts seasons argument."""
        parser = create_parser()
        args = parser.parse_args(["--seasons", "2022/23", "2023/24"])
        assert args.seasons == ["2022/23", "2023/24"]

    def test_parser_no_cache_flag(self) -> None:
        """Test that parser accepts no-cache flag."""
        parser = create_parser()
        args = parser.parse_args(["--no-cache"])
        assert args.no_cache is True

    def test_parser_verbose_flag(self) -> None:
        """Test that parser accepts verbose flag."""
        parser = create_parser()
        args = parser.parse_args(["--verbose"])
        assert args.verbose is True
