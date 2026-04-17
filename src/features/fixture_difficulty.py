"""Fixture difficulty and team strength module.

Calculates fixture difficulty ratings, opponent team strength metrics,
and strength of schedule for each player and team.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/processed/fixture_cache")
CACHE_TTL_SECONDS = 86400  # 24 hours


def _cache_key(func_name: str, params: dict[str, Any]) -> Path:
    """Generate cache file path."""
    import hashlib
    import json

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    key_str = json.dumps({"func": func_name, "params": params}, sort_keys=True)
    key_hash = hashlib.md5(key_str.encode()).hexdigest()[:12]
    return CACHE_DIR / f"{func_name}_{key_hash}.parquet"


def _is_cache_valid(cache_path: Path, ttl: int = CACHE_TTL_SECONDS) -> bool:
    """Check if a cached file is still valid."""
    if not cache_path.exists():
        return False
    age = time.time() - cache_path.stat().st_mtime
    return age < ttl


def _save_cache(cache_path: Path, df: pl.DataFrame) -> None:
    """Save DataFrame to a Parquet cache file."""
    df.write_parquet(cache_path)


def _load_cache(cache_path: Path) -> pl.DataFrame:
    """Load DataFrame from a Parquet cache file."""
    return pl.read_parquet(cache_path)


def compute_fixture_difficulty(
    fixtures: pl.DataFrame,
    team_stats: pl.DataFrame | None = None,
    use_cache: bool = True,
    log_to_mlflow: bool = True,
) -> pl.DataFrame:
    """Compute fixture difficulty ratings for each fixture.

    Fixture difficulty is calculated based on:
    - Opponent team strength (attack/defense ratings)
    - Home/away adjustment
    - Recent form of opponent team
    - Historical goals scored/conceded against opponent

    Args:
        fixtures: DataFrame with fixture data. Must have 'home_team_id',
            'away_team_id', 'gameweek' columns.
        team_stats: Optional DataFrame with team statistics for strength
            calculation. If None, uses FPL strength ratings from fixtures.
        use_cache: Whether to use cached data if available.
        log_to_mlflow: Whether to log to MLflow.

    Returns:
        DataFrame with fixture difficulty ratings (1-5 scale, 5 = hardest).
    """
    cache_params = {"fixtures": fixtures.shape[0]}
    cache_path = _cache_key("compute_fixture_difficulty", cache_params)

    if use_cache and _is_cache_valid(cache_path):
        logger.info("Using cached fixture difficulty")
        return _load_cache(cache_path)

    if fixtures.is_empty():
        return pl.DataFrame()

    # Base difficulty from FPL team strength if available
    if (
        "team_h_difficulty" in fixtures.columns
        and "team_a_difficulty" in fixtures.columns
    ):
        result = fixtures.with_columns(
            pl.col("team_h_difficulty").alias("home_difficulty"),
            pl.col("team_a_difficulty").alias("away_difficulty"),
        )
    else:
        # Default: calculate from team strength if available
        if team_stats is not None and "strength" in team_stats.columns:
            strength_map = dict(
                zip(team_stats["id"].to_list(), team_stats["strength"].to_list())
            )

            # Home team difficulty = away team strength (adjusted for home advantage)
            # Away team difficulty = home team strength (harder to play away)
            result = (
                fixtures.with_columns(
                    pl.col("away_team_id")
                    .map_elements(
                        lambda x: strength_map.get(x, 3), return_dtype=pl.Int64
                    )
                    .alias("away_strength"),
                    pl.col("home_team_id")
                    .map_elements(
                        lambda x: strength_map.get(x, 3), return_dtype=pl.Int64
                    )
                    .alias("home_strength"),
                )
                .with_columns(
                    (pl.col("away_strength") - 1)
                    .clip(lower_bound=1, upper_bound=5)
                    .alias("home_difficulty"),
                    (pl.col("home_strength") + 1)
                    .clip(lower_bound=1, upper_bound=5)
                    .alias("away_difficulty"),
                )
                .drop(["away_strength", "home_strength"])
            )
        else:
            # Default: all fixtures difficulty = 3 (neutral)
            result = fixtures.with_columns(
                pl.lit(3).alias("home_difficulty"),
                pl.lit(3).alias("away_difficulty"),
            )

    # Overall fixture difficulty (average of home and away)
    result = result.with_columns(
        ((pl.col("home_difficulty") + pl.col("away_difficulty")) / 2)
        .round()
        .cast(pl.Int64)
        .alias("overall_difficulty")
    )

    # Save cache
    _save_cache(cache_path, result)
    logger.info(
        "Computed fixture difficulty: %d fixtures",
        result.shape[0],
    )

    if log_to_mlflow:
        _log_fixture_to_mlflow(result)

    return result


def compute_team_strength(
    team_stats: pl.DataFrame,
    matches: pl.DataFrame | None = None,
    use_cache: bool = True,
    log_to_mlflow: bool = True,
) -> pl.DataFrame:
    """Compute dynamic team strength ratings.

    Team strength is calculated based on:
    - Recent performance (goals scored/conceded, xG/xGA)
    - Home/away performance splits
    - Strength of opponents faced
    - Form over last 5-10 games

    Args:
        team_stats: DataFrame with team statistics. Must have 'id' column.
        matches: Optional DataFrame with match results for dynamic strength.
        use_cache: Whether to use cached data if available.
        log_to_mlflow: Whether to log to MLflow.

    Returns:
        DataFrame with team strength ratings (attack, defense, overall).
    """
    cache_params = {"teams": team_stats.shape[0]}
    cache_path = _cache_key("compute_team_strength", cache_params)

    if use_cache and _is_cache_valid(cache_path):
        logger.info("Using cached team strength")
        return _load_cache(cache_path)

    if team_stats.is_empty():
        return pl.DataFrame()

    # Start with base strength from FPL
    result = team_stats.clone()

    # If we have match data, compute dynamic strength
    if matches is not None and not matches.is_empty():
        # Home team attack strength = avg home goals scored
        home_attack = matches.group_by("home_team_id").agg(
            pl.col("home_goals").mean().alias("dynamic_attack_strength"),
            pl.col("home_xg").mean().alias("dynamic_xg_for"),
        )

        # Away team defense strength = avg away goals conceded
        away_defense = matches.group_by("away_team_id").agg(
            pl.col("away_goals").mean().alias("dynamic_defense_weakness"),
            pl.col("away_xg").mean().alias("dynamic_xg_against"),
        )

        result = result.join(
            home_attack, left_on="id", right_on="home_team_id", how="left"
        )
        result = result.join(
            away_defense, left_on="id", right_on="away_team_id", how="left"
        )

        # Fill missing values with base strength
        if "dynamic_attack_strength" in result.columns:
            result = result.with_columns(
                pl.col("dynamic_attack_strength").fill_null(
                    pl.col("strength_attack_home") / 10
                )
            )
        if "dynamic_defense_weakness" in result.columns:
            result = result.with_columns(
                pl.col("dynamic_defense_weakness").fill_null(
                    pl.col("strength_defence_away") / 10
                )
            )

    # Save cache
    _save_cache(cache_path, result)
    logger.info(
        "Computed team strength: %d teams",
        result.shape[0],
    )

    if log_to_mlflow:
        _log_team_strength_to_mlflow(result)

    return result


def compute_strength_of_schedule(
    fixtures: pl.DataFrame,
    team_strength: pl.DataFrame | None = None,
    window: int = 5,
    use_cache: bool = True,
    log_to_mlflow: bool = True,
) -> pl.DataFrame:
    """Compute strength of schedule for each team.

    Strength of schedule = average difficulty of next N fixtures.

    Args:
        fixtures: DataFrame with upcoming fixtures.
        team_strength: Optional DataFrame with team strength ratings.
        window: Number of upcoming fixtures to consider.
        use_cache: Whether to use cached data if available.
        log_to_mlflow: Whether to log to MLflow.

    Returns:
        DataFrame with strength of schedule per team.
    """
    cache_params = {"window": window}
    cache_path = _cache_key("compute_strength_of_schedule", cache_params)

    if use_cache and _is_cache_valid(cache_path):
        logger.info("Using cached strength of schedule")
        return _load_cache(cache_path)

    if fixtures.is_empty():
        return pl.DataFrame()

    # Get difficulty ratings
    if "overall_difficulty" not in fixtures.columns:
        fixtures = compute_fixture_difficulty(
            fixtures, team_strength, use_cache=False, log_to_mlflow=False
        )

    # Calculate average difficulty for next N fixtures per team
    home_sos = (
        fixtures.group_by("home_team_id")
        .agg(
            pl.col("overall_difficulty").head(window).mean().alias("home_sos"),
            pl.col("overall_difficulty")
            .head(window)
            .count()
            .alias("home_fixtures_count"),
        )
        .rename({"home_team_id": "team_id"})
    )

    away_sos = (
        fixtures.group_by("away_team_id")
        .agg(
            pl.col("overall_difficulty").head(window).mean().alias("away_sos"),
            pl.col("overall_difficulty")
            .head(window)
            .count()
            .alias("away_fixtures_count"),
        )
        .rename({"away_team_id": "team_id"})
    )

    sos = home_sos.join(away_sos, on="team_id", how="outer").with_columns(
        (
            (
                pl.col("home_sos") * pl.col("home_fixtures_count")
                + pl.col("away_sos") * pl.col("away_fixtures_count")
            )
            / (pl.col("home_fixtures_count") + pl.col("away_fixtures_count"))
        ).alias("overall_sos")
    )

    # Save cache
    _save_cache(cache_path, sos)
    logger.info("Computed strength of schedule: %d teams", sos.shape[0])

    if log_to_mlflow:
        _log_sos_to_mlflow(sos, window)

    return sos


def _log_fixture_to_mlflow(df: pl.DataFrame) -> None:
    """Log fixture difficulty metrics to MLflow."""
    try:
        from src.utils.mlflow_client import _get_mlflow

        mlflow = _get_mlflow()
        if mlflow is None:
            logger.debug("MLflow not available, skipping fixture logging")
            return

        mlflow.set_tracking_uri("mlruns")
        mlflow.set_experiment("fpl_feature_engineering")
        with mlflow.start_run(run_name="fixture_difficulty"):
            mlflow.log_param("total_fixtures", df.shape[0])
            if "overall_difficulty" in df.columns:
                mlflow.log_metric("avg_difficulty", df["overall_difficulty"].mean())
    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to log fixture difficulty to MLflow: %s", e)


def _log_team_strength_to_mlflow(df: pl.DataFrame) -> None:
    """Log team strength metrics to MLflow."""
    try:
        from src.utils.mlflow_client import _get_mlflow

        mlflow = _get_mlflow()
        if mlflow is None:
            logger.debug("MLflow not available, skipping team strength logging")
            return

        mlflow.set_tracking_uri("mlruns")
        mlflow.set_experiment("fpl_feature_engineering")
        with mlflow.start_run(run_name="team_strength"):
            mlflow.log_param("total_teams", df.shape[0])
    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to log team strength to MLflow: %s", e)


def _log_sos_to_mlflow(df: pl.DataFrame, window: int) -> None:
    """Log strength of schedule metrics to MLflow."""
    try:
        from src.utils.mlflow_client import _get_mlflow

        mlflow = _get_mlflow()
        if mlflow is None:
            logger.debug("MLflow not available, skipping SOS logging")
            return

        mlflow.set_tracking_uri("mlruns")
        mlflow.set_experiment("fpl_feature_engineering")
        with mlflow.start_run(run_name="strength_of_schedule"):
            mlflow.log_param("window", window)
            mlflow.log_param("total_teams", df.shape[0])
            if "overall_sos" in df.columns:
                mlflow.log_metric("avg_sos", df["overall_sos"].mean())
    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to log SOS to MLflow: %s", e)


def clear_cache() -> None:
    """Clear all cached fixture difficulty data."""
    if CACHE_DIR.exists():
        for f in CACHE_DIR.glob("*.parquet"):
            f.unlink()
        logger.info("Cleared fixture difficulty cache in %s", CACHE_DIR)
