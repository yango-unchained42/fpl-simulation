"""Pipeline entry point.

Orchestrates the full FPL prediction pipeline:
data ingestion → cleaning → feature engineering → model prediction → output.
"""

from __future__ import annotations

import argparse
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def run_pipeline(gameweek: int | None = None) -> None:
    """Run the full prediction pipeline.

    Args:
        gameweek: Target gameweek for predictions. Auto-detected if None.
    """
    logger.info("Starting FPL prediction pipeline")

    # Step 1: Data Ingestion
    logger.info("Step 1: Data Ingestion")
    from src.data.ingest_fpl import ingest_fpl_data
    from src.data.ingest_vaastav import load_historical_data

    fpl_data = ingest_fpl_data()
    historical = load_historical_data()

    # Step 2: Data Cleaning
    logger.info("Step 2: Data Cleaning")
    from src.data.clean import clean_data

    _players_clean = clean_data(fpl_data["players"])

    # Step 3: Feature Engineering
    logger.info("Step 3: Feature Engineering")
    from src.features.engineer import engineer_features

    _features = engineer_features(
        player_stats=historical,
        matches=historical,  # placeholder
        fixtures=fpl_data["fixtures"],
    )

    # Step 4: Model Predictions
    logger.info("Step 4: Model Predictions")
    # TODO: Load trained models and generate predictions

    # Step 5: Output to Supabase
    logger.info("Step 5: Output to Supabase")
    # TODO: Write predictions to Supabase

    logger.info("Pipeline completed successfully")


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="FPL Prediction Pipeline")
    parser.add_argument(
        "--gameweek",
        type=int,
        default=None,
        help="Target gameweek for predictions",
    )
    args = parser.parse_args()
    run_pipeline(gameweek=args.gameweek)


if __name__ == "__main__":
    main()
