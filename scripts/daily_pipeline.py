"""Daily FPL Pipeline Workflow.

Runs all daily tasks in sequence:
1. Update Bronze layer (FPL API + Understat for current season)
2. Update Silver layer (cleaning, UUID mapping, aggregations)

Usage:
    python scripts/daily_pipeline.py --season 2025-26
    python scripts/daily_pipeline.py --season 2025-26 --skip-bronze
    python scripts/daily_pipeline.py --skip-silver
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from datetime import datetime

from src.config import CURRENT_SEASON

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
            logger.info(f"  ✓ completed in {duration:.1f}s")
            return True
        else:
            logger.error(f"  ✗ failed:\n{result.stderr[-500:]}")
            return False
    except subprocess.TimeoutExpired:
        logger.error("  ✗ timed out")
        return False
    except Exception as e:
        logger.error(f"  ✗ error: {e}")
        return False


def main() -> None:
    """Run the daily FPL pipeline."""
    parser = argparse.ArgumentParser(description="Daily FPL Pipeline")
    parser.add_argument(
        "--season",
        type=str,
        default=CURRENT_SEASON,
        help=f"Season to process (default: {CURRENT_SEASON})",
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
    logger.info(f"Season: {args.season}")
    logger.info("=" * 50)

    results: dict[str, bool] = {}

    # Step 1: Bronze layer update
    if not args.skip_bronze:
        logger.info("\n[1/2] Bronze layer update")
        results["bronze"] = run_script(
            "scripts/daily_bronze_update.py", ["--season", args.season]
        )
    else:
        logger.info("\n[1/2] Skipping Bronze layer update")
        results["bronze"] = True

    # Step 2: Silver layer update
    if not args.skip_silver:
        logger.info("\n[2/2] Silver layer update")
        results["silver"] = run_script(
            "scripts/daily_silver_update.py", ["--season", args.season]
        )
    else:
        logger.info("\n[2/2] Skipping Silver layer update")
        results["silver"] = True

    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("Pipeline Summary:")
    for step, ok in results.items():
        logger.info(f"  {step}: {'✓' if ok else '✗'}")

    if all(results.values()):
        logger.info("✓ All steps completed successfully!")
        sys.exit(0)
    else:
        logger.error("✗ Some steps failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
