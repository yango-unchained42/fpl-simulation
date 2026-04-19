"""Daily Silver layer update.

Orchestrates the transformation of Bronze data into Silver tables:
1. Team mapping (unified team UUIDs)
2. Player mapping (unified player UUIDs)
3. Match mapping (unified fixture UUIDs)
4. FPL stats (player performance + fantasy data)
5. Understat stats (xG/xA/shots)
6. Fixtures (cleaned fixture schedule)
7. Unified player stats (merged FPL + Understat)

Usage:
    python scripts/daily_silver_update.py --season 2025-26
    python scripts/daily_silver_update.py --skip-fpl
    python scripts/daily_silver_update.py --skip-understat
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time

from dotenv import load_dotenv

from src.config import CURRENT_SEASON, get_supabase

# Ensure scripts/ is on path for sibling imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> None:
    """Run the daily Silver layer update."""
    parser = argparse.ArgumentParser(description="Daily Silver Layer Update")
    parser.add_argument(
        "--season",
        type=str,
        default=CURRENT_SEASON,
        help=f"Season to process (default: {CURRENT_SEASON})",
    )
    parser.add_argument("--skip-fpl", action="store_true", help="Skip FPL data updates")
    parser.add_argument(
        "--skip-understat", action="store_true", help="Skip Understat updates"
    )
    parser.add_argument(
        "--skip-fixtures", action="store_true", help="Skip fixtures updates"
    )
    parser.add_argument(
        "--skip-match-mapping", action="store_true", help="Skip match mapping"
    )
    parser.add_argument(
        "--skip-team-mapping", action="store_true", help="Skip team mapping"
    )
    args = parser.parse_args()

    load_dotenv()
    if not os.getenv("SUPABASE_URL"):
        logger.error("SUPABASE_URL not set")
        sys.exit(1)

    season = args.season
    logger.info(f"Starting daily Silver layer update for season {season}...")
    start_time = time.time()

    client = get_supabase()
    updated = False

    # Step 1: Team mapping
    if not args.skip_team_mapping:
        logger.info("[1/7] Team mapping")
        from scripts.daily_team_mapping_update import run as run_team_mapping

        run_team_mapping()
        logger.info("  ✓ Team mapping updated")

        # Player mapping
        logger.info("[2/7] Player mapping")
        from src.silver.player_mapping import build_all_season_mappings

        mappings = build_all_season_mappings()
        if not mappings.is_empty():
            records = mappings.to_dicts()
            for i in range(0, len(records), 500):
                client.table("silver_player_mapping").upsert(
                    records[i : i + 500]
                ).execute()
            logger.info(f"  ✓ Player mappings updated ({mappings.height} entries)")
        else:
            logger.warning("  ⚠ No player mappings generated")
    else:
        logger.info("[1-2/7] Skipping team + player mapping")

    # Step 2: Match mapping
    if not args.skip_match_mapping:
        logger.info("[3/7] Match mapping")
        from scripts.populate_silver_match_mapping import populate_match_mapping

        populate_match_mapping(client)
        logger.info("  ✓ Match mapping updated")
    else:
        logger.info("[3/7] Skipping match mapping")

    # Step 3: FPL data
    if not args.skip_fpl:
        logger.info("[4/7] FPL stats")
        from src.silver.fpl_stats import (
            update_fpl_fantasy_stats,
            update_fpl_player_stats,
        )

        if update_fpl_player_stats(client, season):
            updated = True
        if update_fpl_fantasy_stats(client, season):
            updated = True
        logger.info("  ✓ FPL stats updated")
    else:
        logger.info("[4/7] Skipping FPL data")

    # Step 4: Fixtures
    if not args.skip_fixtures:
        logger.info("[5/7] Fixtures")
        from src.silver.fixtures import update_fixtures

        if update_fixtures(client, season):
            updated = True
        logger.info("  ✓ Fixtures updated")
    else:
        logger.info("[5/7] Skipping fixtures")

    # Step 5: Understat data
    if not args.skip_understat:
        logger.info("[6/7] Understat stats")
        from src.silver.understat_stats import (
            update_understat_match_stats,
            update_understat_player_stats,
        )

        if update_understat_player_stats(client, season):
            updated = True
        if update_understat_match_stats(client, season):
            updated = True
        logger.info("  ✓ Understat stats updated")
    else:
        logger.info("[6/7] Skipping Understat")

    # Step 6: Unified player stats
    logger.info("[7/7] Unified player stats")
    from src.silver.unified_stats import update_unified_player_stats

    if update_unified_player_stats(client, season):
        updated = True
    logger.info("  ✓ Unified player stats updated")

    elapsed = time.time() - start_time
    if updated:
        logger.info(f"\n✓ Silver layer update complete in {elapsed:.1f}s")
    else:
        logger.info(f"\n✓ Silver layer update complete in {elapsed:.1f}s — no changes")


if __name__ == "__main__":
    main()
