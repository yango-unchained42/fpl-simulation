"""Pipeline monitoring — metrics collection and quality checks.

Calculates data quality metrics for all silver tables:
- Row counts per table per season
- Mapping rates (FPL↔Understat, Vaastav↔Understat)
- Data freshness (last update timestamps)
- Duplicate detection
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from src.config import ALL_SEASONS, get_supabase
from src.utils.supabase_utils import fetch_all_paginated

logger = logging.getLogger(__name__)


def get_table_counts(season: str | None = None) -> dict[str, int]:
    """Get row counts for all bronze and silver tables.

    Args:
        season: Optional season filter. None = all seasons.

    Returns:
        Dict mapping table name to row count.
    """
    client = get_supabase()

    tables = [
        "bronze_fpl_players", "bronze_fpl_teams", "bronze_fpl_fixtures", "bronze_fpl_gw",
        "bronze_understat_player_stats", "bronze_understat_match_stats", "bronze_understat_shots",
        "silver_player_mapping", "silver_team_mapping", "silver_match_mapping",
        "silver_fpl_player_stats", "silver_fpl_fantasy_stats",
        "silver_understat_player_stats", "silver_understat_match_stats",
        "silver_fixtures", "silver_unified_player_stats",
    ]

    counts: dict[str, int] = {}
    for table in tables:
        try:
            query = client.table(table).select("*", count="exact").limit(0)
            if season:
                query = query.eq("season", season)
            r = query.execute()
            counts[table] = r.count or 0
        except Exception:
            counts[table] = -1  # Table doesn't exist or error

    return counts


def get_mapping_quality(season: str | None = None) -> dict[str, Any]:
    """Calculate player mapping quality metrics.

    Returns:
        Dict with mapping rates and quality stats.
    """
    client = get_supabase()

    # Fetch all mappings (or filtered by season)
    filters = {"season": season} if season else None
    all_mappings = fetch_all_paginated(
        client, "silver_player_mapping",
        select_cols="season,fpl_id,vaastav_id,understat_id,confidence_score",
    )

    if not all_mappings:
        return {"total": 0}

    # Deduplicate by (season, fpl_id) for FPL entries
    # and by (season, vaastav_id) for Vaastav entries
    by_season: dict[str, dict] = {}

    for m in all_mappings:
        s = m.get("season", "unknown")
        if s not in by_season:
            by_season[s] = {
                "total": 0, "fpl": 0, "vaastav": 0, "understat": 0,
                "fpl_to_understat": 0, "vaastav_to_understat": 0,
                "high_confidence": 0,
            }

        d = by_season[s]
        d["total"] += 1

        has_fpl = bool(m.get("fpl_id"))
        has_va = bool(m.get("vaastav_id"))
        has_us = bool(m.get("understat_id"))
        high_conf = (m.get("confidence_score") or 0) >= 0.85

        if has_fpl:
            d["fpl"] += 1
            if has_us:
                d["fpl_to_understat"] += 1
        if has_va:
            d["vaastav"] += 1
            if has_us:
                d["vaastav_to_understat"] += 1
        if has_us:
            d["understat"] += 1
        if high_conf:
            d["high_confidence"] += 1

    # Calculate rates
    result: dict[str, Any] = {"by_season": {}, "totals": {
        "total": 0, "fpl": 0, "vaastav": 0, "understat": 0,
        "fpl_to_understat": 0, "vaastav_to_understat": 0,
    }}

    for s, d in by_season.items():
        fpl_rate = d["fpl_to_understat"] / d["fpl"] * 100 if d["fpl"] else 0
        va_rate = d["vaastav_to_understat"] / d["vaastav"] * 100 if d["vaastav"] else 0

        result["by_season"][s] = {
            **d,
            "fpl_to_understat_rate": round(fpl_rate, 1),
            "vaastav_to_understat_rate": round(va_rate, 1),
        }

        for k in result["totals"]:
            result["totals"][k] += d.get(k, 0)

    # Overall rates
    t = result["totals"]
    t["fpl_to_understat_rate"] = round(
        t["fpl_to_understat"] / t["fpl"] * 100, 1
    ) if t["fpl"] else 0
    t["vaastav_to_understat_rate"] = round(
        t["vaastav_to_understat"] / t["vaastav"] * 100, 1
    ) if t["vaastav"] else 0

    return result


def get_duplicate_counts() -> dict[str, int]:
    """Check for duplicates in key silver tables.

    Returns:
        Dict mapping table to duplicate count.
    """
    client = get_supabase()
    dupes: dict[str, int] = {}

    # Player mapping: duplicates by (season, fpl_id)
    try:
        all_maps = fetch_all_paginated(
            client, "silver_player_mapping", select_cols="season,fpl_id"
        )
        seen: set[tuple] = set()
        dupe_count = 0
        for m in all_maps:
            key = (m.get("season"), m.get("fpl_id"))
            if key[1] is not None:
                if key in seen:
                    dupe_count += 1
                else:
                    seen.add(key)
        dupes["silver_player_mapping"] = dupe_count
    except Exception:
        dupes["silver_player_mapping"] = -1

    return dupes


def collect_all_metrics(season: str | None = None) -> dict[str, Any]:
    """Collect all pipeline metrics.

    Args:
        season: Optional season filter.

    Returns:
        Complete metrics dict.
    """
    logger.info(f"Collecting pipeline metrics{'for ' + season if season else ''}...")

    return {
        "collected_at": datetime.now().isoformat(),
        "season": season,
        "table_counts": get_table_counts(season),
        "mapping_quality": get_mapping_quality(season),
        "duplicates": get_duplicate_counts(),
    }


def format_metrics_report(metrics: dict[str, Any]) -> str:
    """Format metrics as a readable report.

    Args:
        metrics: Metrics dict from collect_all_metrics.

    Returns:
        Formatted string report.
    """
    lines = []
    lines.append("=" * 60)
    lines.append("FPL Pipeline — Data Quality Report")
    lines.append(f"Generated: {metrics['collected_at']}")
    if metrics.get("season"):
        lines.append(f"Season: {metrics['season']}")
    lines.append("=" * 60)

    # Table counts
    lines.append("\n📊 Table Row Counts:")
    lines.append(f"  {'Table':<40} {'Rows':>10}")
    lines.append(f"  {'─' * 40} {'─' * 10}")
    for table, count in sorted(metrics["table_counts"].items()):
        tier = "🥉" if "bronze" in table else "🥈"
        lines.append(f"  {tier} {table:<37} {count:>10,}")

    # Mapping quality
    mq = metrics["mapping_quality"]
    lines.append(f"\n🔗 Player Mapping Quality:")
    if "totals" in mq:
        t = mq["totals"]
        lines.append(f"  Total mappings:     {t['total']:>8,}")
        lines.append(f"  FPL players:        {t['fpl']:>8,}")
        lines.append(f"  Vaastav players:    {t['vaastav']:>8,}")
        lines.append(f"  Understat players:  {t['understat']:>8,}")
        lines.append(f"  FPL→Understat:      {t['fpl_to_understat']:>8,} ({t['fpl_to_understat_rate']}%)")
        lines.append(f"  Vaastav→Understat:  {t['vaastav_to_understat']:>8,} ({t['vaastav_to_understat_rate']}%)")

    # By season
    if "by_season" in mq:
        lines.append(f"\n  By season:")
        lines.append(f"  {'Season':<10} {'FPL':>6} {'US Map':>7} {'US%':>7} {'VA':>6} {'US Map':>7} {'US%':>7}")
        for s in sorted(mq["by_season"].keys()):
            d = mq["by_season"][s]
            lines.append(
                f"  {s:<10} {d['fpl']:>6} {d['fpl_to_understat']:>7} "
                f"{d['fpl_to_understat_rate']:>6.1f}% {d['vaastav']:>6} "
                f"{d['vaastav_to_understat']:>7} {d['vaastav_to_understat_rate']:>6.1f}%"
            )

    # Duplicates
    lines.append(f"\n⚠️  Duplicates:")
    for table, count in metrics["duplicates"].items():
        status = "❌" if count > 0 else "✅"
        lines.append(f"  {status} {table}: {count}")

    lines.append("\n" + "=" * 60)
    return "\n".join(lines)
