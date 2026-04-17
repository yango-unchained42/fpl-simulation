"""Daily update pipeline for FPL Simulation.

This script runs daily to:
1. Ingest fresh FPL API data.
2. Upsert raw data to Supabase.
3. Calculate features (rolling stats, H2H, form).
4. Upsert features to Supabase.
5. Generate predictions for the next gameweek.
6. Upsert predictions to Supabase.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import polars as pl
from dotenv import load_dotenv

from src.data.ingest_fpl import fetch_bootstrap_static

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_supabase():
    """Get Supabase client."""
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in .env")

    from supabase import create_client

    return create_client(url, key)


def upsert_table(
    supabase, table_name: str, df: pl.DataFrame, batch_size: int = 500
) -> None:
    """Upsert a DataFrame to Supabase in batches."""
    if df.is_empty():
        logger.info("⏭️ Skipping %s (empty)", table_name)
        return

    records = df.to_dicts()
    total = len(records)
    logger.info("📤 Upserting %s: %d rows...", table_name, total)

    for i in range(0, total, batch_size):
        batch = records[i : i + batch_size]
        try:
            supabase.table(table_name).upsert(batch).execute()
        except Exception as e:
            logger.error("   ❌ Error at batch %d: %s", i, e)

    logger.info("✅ %s complete!", table_name)


def ingest_fpl_data() -> dict[str, Any]:
    """Fetch fresh FPL data."""
    logger.info("🔍 Fetching FPL API data...")
    data = fetch_bootstrap_static(use_cache=False)
    return data


def calculate_features(history_df: pl.DataFrame, current_gw: int) -> pl.DataFrame:
    """Calculate rolling stats, H2H, and form features."""
    logger.info("🧮 Calculating features...")

    # Example: Rolling average points (last 3 GWs)
    # In a real scenario, this would use the full feature engineering pipeline
    features = history_df.group_by("player_id").agg(
        pl.col("total_points").tail(3).mean().alias("rolling_points_3"),
        pl.col("expected_goals").tail(3).mean().alias("rolling_xg_3"),
        pl.col("expected_assists").tail(3).mean().alias("rolling_xa_3"),
    )

    # Add current gameweek and season
    features = features.with_columns(
        pl.lit(current_gw).alias("gameweek"),
        pl.lit("2025-26").alias("season"),
    )

    # Add dummy columns for now (to be filled by full pipeline later)
    features = features.with_columns(
        pl.lit(0.0).alias("rolling_points_5"),
        pl.lit(0.0).alias("rolling_points_10"),
        pl.lit(0.0).alias("h2h_avg_points_vs_opponent"),
        pl.lit(0.0).alias("h2h_avg_xg_vs_opponent"),
        pl.lit(0.0).alias("form_score"),
        pl.lit(3).alias("fixture_difficulty"),
        pl.lit(0.0).alias("xi_probability"),
    )

    return features


def generate_predictions(features_df: pl.DataFrame) -> pl.DataFrame:
    """Generate predictions for the next gameweek."""
    logger.info("🔮 Generating predictions...")

    # For now, use a simple heuristic: Expected Points + XI Probability
    # In the future, this will use the trained ML model
    predictions = features_df.select(
        [
            "player_id",
            "gameweek",
            "season",
            "rolling_points_3",
            "rolling_xg_3",
            "rolling_xa_3",
        ]
    ).with_columns(
        (pl.col("rolling_points_3") * 0.6 + pl.col("rolling_xg_3") * 0.4).alias(
            "expected_points"
        ),
        pl.lit(0.8).alias("xi_probability"),
    )

    return predictions


def main() -> None:
    """Main daily update function."""
    logger.info("🚀 Starting daily update pipeline...")
    start_time = time.time()

    try:
        supabase = get_supabase()
    except Exception as e:
        logger.error("❌ Failed to connect to Supabase: %s", e)
        return

    # 1. Ingest FPL Data
    try:
        fpl_data = ingest_fpl_data()
    except Exception as e:
        logger.error("❌ Failed to ingest FPL data: %s", e)
        return

    # 2. Upsert Raw Data
    players_df = pl.DataFrame(fpl_data["elements"])
    teams_df = pl.DataFrame(fpl_data["teams"])

    # Fixtures might be nested in events
    fixtures_list = []
    for event in fpl_data.get("events", []):
        fixtures_list.extend(event.get("fixtures", []))
    fixtures_df = pl.DataFrame(fixtures_list) if fixtures_list else pl.DataFrame()

    upsert_table(supabase, "fpl_players", players_df)
    upsert_table(supabase, "fpl_teams", teams_df)
    if not fixtures_df.is_empty():
        upsert_table(supabase, "fpl_fixtures", fixtures_df)

    # 3. Calculate Features
    # Fetch latest history from DB
    try:
        history_result = supabase.table("player_history").select("*").execute()
        history_df = pl.DataFrame(history_result.data)
        current_gw = history_df["gameweek"].max() + 1
        features_df = calculate_features(history_df, current_gw)
        upsert_table(supabase, "player_features", features_df)
    except Exception as e:
        logger.error("❌ Failed to calculate features: %s", e)

    # 4. Generate Predictions
    try:
        predictions_df = generate_predictions(features_df)
        predictions_df = predictions_df.with_columns(
            pl.lit("v1.0").alias("model_version"),
            pl.lit(False).alias("is_captain_pick"),
        )
        upsert_table(supabase, "predictions", predictions_df)
    except Exception as e:
        logger.error("❌ Failed to generate predictions: %s", e)

    elapsed = time.time() - start_time
    logger.info("✅ Daily update complete in %.2f seconds!", elapsed)


if __name__ == "__main__":
    main()
