"""Data ingestion pipeline orchestration.

Coordinates data ingestion from all sources (FPL API, vaastav, Understat),
manages dependencies between sources, handles error recovery with retries and
checkpoints, and writes results to Supabase.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)

PIPELINE_STATE_FILE = Path("data/processed/pipeline_state.json")
MAX_RETRIES = 2


@dataclass
class IngestionResult:
    """Result of a single ingestion step."""

    source: str
    success: bool
    row_count: int
    duration_seconds: float
    retries: int = 0
    error_message: str | None = None


@dataclass
class PipelineResult:
    """Result of the full ingestion pipeline."""

    results: list[IngestionResult] = field(default_factory=list)
    total_duration_seconds: float = 0.0

    @property
    def success(self) -> bool:
        """Whether all ingestion steps succeeded."""
        return all(r.success for r in self.results)

    @property
    def successful_sources(self) -> list[str]:
        """List of sources that were ingested successfully."""
        return [r.source for r in self.results if r.success]

    @property
    def failed_sources(self) -> list[str]:
        """List of sources that failed to ingest."""
        return [r.source for r in self.results if not r.success]

    def summary(self) -> str:
        """Generate a human-readable summary of the pipeline run."""
        lines = ["Pipeline Execution Summary", "=" * 40]
        for r in self.results:
            status = "OK" if r.success else "FAILED"
            retry_info = f" ({r.retries} retries)" if r.retries > 0 else ""
            lines.append(
                f"  {r.source}: {status}{retry_info} "
                f"({r.row_count} rows, {r.duration_seconds:.1f}s)"
            )
            if r.error_message:
                lines.append(f"    Error: {r.error_message}")
        lines.append(f"\nTotal duration: {self.total_duration_seconds:.1f}s")
        lines.append(f"Successful: {len(self.successful_sources)}")
        lines.append(f"Failed: {len(self.failed_sources)}")
        return "\n".join(lines)


def _load_pipeline_state() -> dict[str, Any]:
    """Load the last pipeline state from disk for checkpoint/resume."""
    if PIPELINE_STATE_FILE.exists():
        with open(PIPELINE_STATE_FILE, encoding="utf-8") as f:
            return json.load(f)  # type: ignore[no-any-return]
    return {}


def _save_pipeline_state(state: dict[str, Any]) -> None:
    """Save pipeline state to disk for checkpoint/resume."""
    PIPELINE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(PIPELINE_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def _check_data_freshness(
    data: pl.DataFrame,
    required_columns: list[str],
    min_rows: int = 1,
) -> tuple[bool, str]:
    """Check if ingested data meets freshness requirements."""
    if data.is_empty():
        return False, "DataFrame is empty"

    missing_cols = [c for c in required_columns if c not in data.columns]
    if missing_cols:
        return False, f"Missing required columns: {missing_cols}"

    if data.shape[0] < min_rows:
        return False, f"Insufficient rows: {data.shape[0]} < {min_rows}"

    return True, "Data passes freshness checks"


def _run_ingestion_step_with_retry(
    step_name: str,
    ingest_fn: Any,
    required_columns: list[str] | None = None,
    min_rows: int = 1,
    max_retries: int = MAX_RETRIES,
    **kwargs: Any,
) -> tuple[IngestionResult, pl.DataFrame | None]:
    """Run a single ingestion step with retry, timing, and validation.

    Returns:
        Tuple of (IngestionResult, DataFrame or None if failed).
    """
    last_error: str | None = None
    for attempt in range(max_retries + 1):
        start_time = time.time()
        try:
            data = ingest_fn(**kwargs)
            duration = time.time() - start_time

            if required_columns is not None:
                is_fresh, msg = _check_data_freshness(data, required_columns, min_rows)
                if not is_fresh:
                    logger.warning(
                        "Data freshness check failed for %s: %s", step_name, msg
                    )
                    last_error = f"Freshness check: {msg}"
                    if attempt < max_retries:
                        logger.info(
                            "Retrying %s (attempt %d/%d)",
                            step_name,
                            attempt + 1,
                            max_retries,
                        )
                        continue
                    return (
                        IngestionResult(
                            source=step_name,
                            success=False,
                            row_count=data.shape[0],
                            duration_seconds=duration,
                            retries=attempt,
                            error_message=last_error,
                        ),
                        None,
                    )

            logger.info(
                "Ingested %d rows for %s in %.1fs",
                data.shape[0],
                step_name,
                duration,
            )
            return (
                IngestionResult(
                    source=step_name,
                    success=True,
                    row_count=data.shape[0],
                    duration_seconds=duration,
                    retries=attempt,
                ),
                data,
            )

        except Exception as e:  # noqa: BLE001
            duration = time.time() - start_time
            last_error = str(e)
            logger.error("Ingestion failed for %s: %s", step_name, e)
            if attempt < max_retries:
                logger.info(
                    "Retrying %s (attempt %d/%d)",
                    step_name,
                    attempt + 1,
                    max_retries,
                )
                continue
            return (
                IngestionResult(
                    source=step_name,
                    success=False,
                    row_count=0,
                    duration_seconds=duration,
                    retries=attempt,
                    error_message=last_error,
                ),
                None,
            )

    return (
        IngestionResult(
            source=step_name,
            success=False,
            row_count=0,
            duration_seconds=0.0,
            retries=max_retries,
            error_message=last_error or "Unknown error",
        ),
        None,
    )


def _write_to_supabase(table: str, df: pl.DataFrame) -> bool:
    """Write DataFrame to Supabase with graceful fallback."""
    try:
        from src.data.database import write_to_supabase

        return write_to_supabase(table, df, upsert=True)
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "Supabase write failed for %s: %s (data cached locally)", table, e
        )
        return False


def run_ingestion_pipeline(
    seasons: list[str] | None = None,
    use_cache: bool = True,
    sources: list[str] | None = None,
    write_db: bool = True,
    resume: bool = True,
) -> PipelineResult:
    """Run the full data ingestion pipeline.

    Args:
        seasons: List of season strings for historical sources.
        use_cache: Whether to use cached data if available.
        sources: List of sources to ingest. Defaults to all sources.
        write_db: Whether to write results to Supabase.
        resume: Whether to resume from last checkpoint.

    Returns:
        PipelineResult with per-source results and summary.
    """
    if seasons is None:
        seasons = ["2021-22", "2022-23", "2023-24", "2024-25"]
    if sources is None:
        sources = ["fpl", "vaastav", "understat"]

    pipeline_start = time.time()
    results: list[IngestionResult] = []

    # Load checkpoint state for resume
    completed_sources: set[str] = set()
    if resume:
        state = _load_pipeline_state()
        completed_sources = set(state.keys())
        if completed_sources:
            logger.info("Resuming pipeline, skipping completed: %s", completed_sources)

    logger.info("Starting ingestion pipeline with sources: %s", sources)

    # Understat uses ALL_SEASONS by default — cache handles deduplication
    # so re-fetching existing seasons just loads from cache (no network)
    from src.data.ingest_understat import ALL_SEASONS as UNDERSTAT_ALL_SEASONS  # type: ignore[attr-defined]

    # Use all Understat seasons if user didn't specify custom ones
    understat_seasons = UNDERSTAT_ALL_SEASONS if seasons is None else seasons

    # ── Step 1: FPL API ─────────────────────────────────────────────────────
    if "fpl" in sources and "fpl" not in completed_sources:
        logger.info("Step 1: Ingesting FPL API data")
        from src.data.ingest_fpl import ingest_fpl_data

        fpl_start = time.time()
        try:
            fpl_data = ingest_fpl_data(use_cache=use_cache)
            fpl_duration = time.time() - fpl_start
            total_rows = sum(df.shape[0] for df in fpl_data.values())
            logger.info(
                "Ingested FPL data: players=%d, teams=%d, fixtures=%d",
                fpl_data["players"].shape[0],
                fpl_data["teams"].shape[0],
                fpl_data["fixtures"].shape[0],
            )
            result = IngestionResult(
                source="fpl",
                success=True,
                row_count=total_rows,
                duration_seconds=fpl_duration,
            )

            if write_db:
                _write_to_supabase("fpl_players", fpl_data["players"])
                _write_to_supabase("fpl_teams", fpl_data["teams"])
                if not fpl_data["fixtures"].is_empty():
                    _write_to_supabase("fpl_fixtures", fpl_data["fixtures"])
                if (
                    "player_history" in fpl_data
                    and not fpl_data["player_history"].is_empty()
                ):
                    _write_to_supabase("fpl_player_history", fpl_data["player_history"])

        except Exception as e:  # noqa: BLE001
            fpl_duration = time.time() - fpl_start
            logger.error("Ingestion failed for fpl: %s", e)
            result = IngestionResult(
                source="fpl",
                success=False,
                row_count=0,
                duration_seconds=fpl_duration,
                error_message=str(e),
            )
            logger.warning("FPL ingestion failed, continuing with other sources")

        results.append(result)
        _save_pipeline_state({**_load_pipeline_state(), "fpl": asdict(result)})

    # ── Step 2: Vaastav ──────────────────────────────────────────────────────
    if "vaastav" in sources and "vaastav" not in completed_sources:
        logger.info("Step 2: Ingesting vaastav historical data")
        from src.data.ingest_vaastav import (
            fetch_season_history,
            load_historical_data,
        )

        vaastav_result, vaastav_data = _run_ingestion_step_with_retry(
            "vaastav",
            load_historical_data,
            required_columns=["GW"],
            min_rows=1,
            seasons=seasons,
            use_cache=use_cache,
        )
        results.append(vaastav_result)

        if vaastav_result.success and write_db and vaastav_data is not None:
            _write_to_supabase("player_history", vaastav_data)

        # Vaastav season summary (official FPL season-aggregated metrics)
        for season in seasons:
            season_result, season_data = _run_ingestion_step_with_retry(
                f"vaastav_season_{season}",
                fetch_season_history,
                required_columns=[],
                min_rows=0,
                season=season,
                use_cache=use_cache,
            )
            results.append(season_result)
            if season_result.success and write_db and season_data is not None:
                _write_to_supabase("vaastav_season_summary", season_data)

        _save_pipeline_state(
            {**_load_pipeline_state(), "vaastav": asdict(vaastav_result)}
        )

    # ── Step 3: Understat ────────────────────────────────────────────────────
    if "understat" in sources:
        logger.info("Step 3: Ingesting Understat data")
        from src.data.ingest_understat import (
            ingest_understat_match_stats,
            ingest_understat_player_match_stats,
            ingest_understat_player_season_stats,
            ingest_understat_shots,
        )

        for table_name, ingest_fn, db_table in [
            ("understat_shots", ingest_understat_shots, "understat_shots"),
            (
                "understat_player_match",
                ingest_understat_player_match_stats,
                "understat_player_match",
            ),
            (
                "understat_player_season",
                ingest_understat_player_season_stats,
                "understat_player_season",
            ),
            (
                "understat_match_stats",
                ingest_understat_match_stats,
                "understat_match_stats",
            ),
        ]:
            if table_name in completed_sources:
                continue

            step_result, step_data = _run_ingestion_step_with_retry(
                table_name,
                ingest_fn,
                required_columns=[],
                min_rows=0,
                seasons=understat_seasons,
                use_cache=use_cache,
            )
            results.append(step_result)

            if step_result.success and write_db and step_data is not None:
                _write_to_supabase(db_table, step_data)

            _save_pipeline_state(
                {**_load_pipeline_state(), table_name: asdict(step_result)}
            )

    # ── Step 4: FBRef — removed (anti-bot protection makes scraping unreliable)

    total_duration = time.time() - pipeline_start

    pipeline_result = PipelineResult(
        results=results,
        total_duration_seconds=total_duration,
    )

    logger.info("\n%s", pipeline_result.summary())
    return pipeline_result


def create_parser() -> argparse.ArgumentParser:
    """Create CLI argument parser for the ingestion pipeline."""
    parser = argparse.ArgumentParser(
        description="FPL Data Ingestion Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.data.ingest_pipeline --sources fpl vaastav
  python -m src.data.ingest_pipeline --seasons 2022/23 2023/24 --no-cache
  python -m src.data.ingest_pipeline --sources fpl --verbose
  python -m src.data.ingest_pipeline --no-db  # skip Supabase writes
  python -m src.data.ingest_pipeline --daily  # FPL + Understat current season
        """,
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=["fpl", "vaastav", "understat"],
        default=None,
        help="Data sources to ingest (default: all)",
    )
    parser.add_argument(
        "--seasons",
        nargs="+",
        default=None,
        help="Seasons to ingest (default: last 3)",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable caching and force fresh API calls",
    )
    parser.add_argument(
        "--no-db",
        action="store_true",
        help="Skip Supabase writes (data still cached locally)",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Ignore checkpoint state and re-run all sources",
    )
    parser.add_argument(
        "--daily",
        action="store_true",
        help="Daily GW refresh mode: FPL (current) + Understat (current) only",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )
    return parser


def main() -> None:
    """Entry point for CLI execution."""
    parser = create_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Daily mode: FPL + current Understat only, skip Vaastav (historical/static)
    if args.daily:
        from src.data.ingest_understat import CURRENT_SEASON  # type: ignore[attr-defined]

        args.sources = ["fpl", "understat"]
        args.seasons = [CURRENT_SEASON]
        args.no_resume = True  # Always refresh in daily mode

    result = run_ingestion_pipeline(
        seasons=args.seasons,
        use_cache=not args.no_cache,
        sources=args.sources,
        write_db=not args.no_db,
        resume=not args.no_resume,
    )

    print(result.summary())

    if result.failed_sources:
        logger.warning("Pipeline completed with errors from: %s", result.failed_sources)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
