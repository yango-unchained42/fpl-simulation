"""Daily FPL Pipeline Workflow.

This script runs all daily tasks in sequence:
1. Update Bronze layer (FPL + Understat)
2. Update Silver player state from FPL API (pre-GW snapshot)
3. Merge player state into silver_fpl_fantasy_stats
4. Log results

Usage: python scripts/daily_pipeline.py --gw 32
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_script(script_path: str, args: list[str] | None = None) -> bool:
    """Run a Python script and return success status."""
    cmd = [sys.executable, script_path]
    if args:
        cmd.extend(args)

    logger.info(f"Running: {' '.join(cmd)}")
    start = datetime.now()

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,  # 10 min timeout
        )
        duration = (datetime.now() - start).total_seconds()

        if result.returncode == 0:
            logger.info(f"✓ {script_path} completed in {duration:.1f}s")
            return True
        else:
            logger.error(f"✗ {script_path} failed: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        logger.error(f"✗ {script_path} timed out")
        return False
    except Exception as e:
        logger.error(f"✗ {script_path} error: {e}")
        return False


def main() -> None:
    """Run the daily FPL pipeline."""
    parser = argparse.ArgumentParser(description="Daily FPL Pipeline")
    parser.add_argument(
        "--gw",
        type=int,
        help="Current gameweek number (required for player state update)",
    )
    parser.add_argument(
        "--season",
        type=str,
        default="2025-26",
        help="Current season",
    )
    parser.add_argument(
        "--skip-bronze",
        action="store_true",
        help="Skip Bronze layer update",
    )
    parser.add_argument(
        "--skip-silver",
        action="store_true",
        help="Skip Silver layer update",
    )
    args = parser.parse_args()

    logger.info("=" * 50)
    logger.info(f"FPL Daily Pipeline - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    logger.info("=" * 50)

    results = {}

    # Step 1: Bronze layer update
    if not args.skip_bronze:
        logger.info("\n[1/3] Running Bronze layer update...")
        results["bronze"] = run_script("scripts/daily_bronze_update.py")
    else:
        logger.info("\n[1/3] Skipping Bronze layer update")
        results["bronze"] = True

    # Step 2: Silver layer update (FPL + Understat + Fixtures)
    if not args.skip_silver:
        logger.info(f"\n[2/3] Running Silver layer update...")
        silver_args = ["--season", args.season]
        if args.gw:
            silver_args.extend(["--gw", str(args.gw)])
        results["silver"] = run_script("scripts/daily_silver_update.py", silver_args)
    else:
        logger.info("\n[2/3] Skipping Silver layer update")
        results["silver"] = True

    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("Pipeline Summary:")
    logger.info(f"  Bronze: {'✓' if results.get('bronze') else '✗'}")
    logger.info(f"  Silver: {'✓' if results.get('silver') else '✗'}")

    all_passed = all(results.values())
    if all_passed:
        logger.info("✓ All steps completed successfully!")
        sys.exit(0)
    else:
        logger.error("✗ Some steps failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
