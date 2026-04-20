"""Pipeline metrics report.

Usage:
    python scripts/pipeline_metrics.py
    python scripts/pipeline_metrics.py --season 2025-26
    python scripts/pipeline_metrics.py --json
"""

from __future__ import annotations

import argparse
import json

from src.monitoring.metrics import collect_all_metrics, format_metrics_report


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline metrics report")
    parser.add_argument("--season", type=str, default=None, help="Filter by season")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    metrics = collect_all_metrics(args.season)

    if args.json:
        print(json.dumps(metrics, indent=2, default=str))
    else:
        print(format_metrics_report(metrics))


if __name__ == "__main__":
    main()
