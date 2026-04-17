"""Player ID crosswalk between Understat and FPL.

Maps Understat player IDs to FPL element IDs using fuzzy name matching
with confidence scoring.
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

CROSSWALK_DIR = Path("data/processed")
CROSSWALK_FILE = CROSSWALK_DIR / "understat_fpl_crosswalk.parquet"


def build_understat_fpl_crosswalk(
    understat_data: pl.DataFrame,
    fpl_players: pl.DataFrame,
    threshold: float = 0.75,
    use_cache: bool = True,
    log_to_mlflow: bool = True,
) -> pl.DataFrame:
    """Build a crosswalk mapping Understat player IDs to FPL element IDs.

    Uses fuzzy name matching with Levenshtein distance to connect
    Understat's internal player IDs to FPL's element IDs.

    Args:
        understat_data: Understat player data with 'player' and 'player_id' columns.
        fpl_players: FPL player data with 'id' and 'web_name' columns.
        threshold: Minimum confidence score to accept a match.
        use_cache: Whether to use cached crosswalk if available.
        log_to_mlflow: Whether to log matching statistics to MLflow.

    Returns:
        DataFrame with columns: understat_player_id, fpl_player_id,
        understat_name, fpl_name, confidence.
    """
    if use_cache and CROSSWALK_FILE.exists():
        logger.info("Using cached Understat→FPL crosswalk")
        return pl.read_parquet(CROSSWALK_FILE)

    # Get unique Understat players
    us_players = (
        understat_data.select(["player_id", "player"])
        .unique(subset=["player_id"])
        .rename({"player": "understat_name"})
    )

    # Get FPL player names
    fpl_cols = ["id", "web_name"]
    if "first_name" in fpl_players.columns:
        fpl_cols.append("first_name")
    if "second_name" in fpl_players.columns:
        fpl_cols.append("second_name")
    fpl_names = fpl_players.select(fpl_cols).rename(
        {"id": "fpl_player_id", "web_name": "fpl_name"}
    )

    # Build mapping
    from src.utils.name_resolver import build_name_mapping

    us_name_list = us_players["understat_name"].to_list()
    fpl_name_list = fpl_names["fpl_name"].to_list()

    name_mapping = build_name_mapping(us_name_list, fpl_name_list, threshold)

    # Build crosswalk DataFrame
    rows = []
    for us_id, us_name in zip(
        us_players["player_id"].to_list(),
        us_players["understat_name"].to_list(),
    ):
        fpl_name, confidence = name_mapping.get(us_name, (us_name, 0.0))
        fpl_row = fpl_names.filter(pl.col("fpl_name") == fpl_name)
        fpl_id = (
            fpl_row["fpl_player_id"].to_list()[0] if not fpl_row.is_empty() else None
        )

        rows.append(
            {
                "understat_player_id": us_id,
                "fpl_player_id": fpl_id,
                "understat_name": us_name,
                "fpl_name": fpl_name,
                "confidence": round(confidence, 3),
            }
        )

    crosswalk = pl.DataFrame(rows)

    # Save cache
    CROSSWALK_DIR.mkdir(parents=True, exist_ok=True)
    crosswalk.write_parquet(CROSSWALK_FILE)
    logger.info("Saved Understat→FPL crosswalk to %s", CROSSWALK_FILE)

    # Log statistics
    matched = crosswalk.filter(pl.col("fpl_player_id").is_not_null())
    unmatched = crosswalk.filter(pl.col("fpl_player_id").is_null())
    avg_conf = (
        float(matched["confidence"].mean())  # type: ignore[arg-type]
        if not matched.is_empty()
        else 0.0
    )

    logger.info(
        "Crosswalk: %d matched (%.1f%%), %d unmatched, avg confidence: %.3f",
        matched.shape[0],
        matched.shape[0] / max(crosswalk.shape[0], 1) * 100,
        unmatched.shape[0],
        avg_conf,
    )

    if log_to_mlflow:
        _log_crosswalk_to_mlflow(crosswalk, matched, unmatched, avg_conf)

    return crosswalk


def _log_crosswalk_to_mlflow(
    crosswalk: pl.DataFrame,
    matched: pl.DataFrame,
    unmatched: pl.DataFrame,
    avg_conf: float,
) -> None:
    """Log crosswalk statistics to MLflow."""
    try:
        from src.utils.mlflow_client import _get_mlflow

        mlflow = _get_mlflow()
        if mlflow is None:
            logger.debug("MLflow not available, skipping crosswalk logging")
            return

        mlflow.set_tracking_uri("mlruns")
        mlflow.set_experiment("fpl_data_cleaning")
        with mlflow.start_run(run_name="understat_fpl_crosswalk"):
            mlflow.log_param("total_understat_players", crosswalk.shape[0])
            mlflow.log_param("matched_players", matched.shape[0])
            mlflow.log_param("unmatched_players", unmatched.shape[0])
            mlflow.log_metric(
                "match_rate", matched.shape[0] / max(crosswalk.shape[0], 1)
            )
            mlflow.log_metric("avg_confidence", avg_conf)
            if not unmatched.is_empty():
                mlflow.log_param(
                    "unmatched_sample",
                    str(unmatched.head(10).select(["understat_name"]).to_dict()),
                )
    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to log crosswalk to MLflow: %s", e)
