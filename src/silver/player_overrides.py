"""Manual player mapping overrides.

For players that can't be matched automatically by name + team fuzzy matching.
Add entries here when you discover mismatches in the silver_player_mapping table.

Format: (season, source, source_id, target_source, target_id)
- source/target_source: "fpl", "vaastav", "understat"
- IDs: the player ID in the respective source

These are applied BEFORE fuzzy matching in the player mapping pipeline.
"""

from __future__ import annotations

# Manual overrides: (season, source, source_id, target_source, target_id)
MANUAL_PLAYER_OVERRIDES: list[tuple[str, str, int, str, int]] = [
    # Example (uncomment and fill in when needed):
    # ("2025-26", "fpl", 123, "understat", 456),
    # ("2024-25", "vaastav", 789, "understat", 456),
]


def get_overrides_for_season(
    season: str,
) -> list[tuple[str, int, str, int]]:
    """Get manual overrides for a specific season.

    Args:
        season: Season string (e.g., "2025-26").

    Returns:
        List of (source, source_id, target_source, target_id) tuples.
    """
    return [
        (src, sid, tgt, tid)
        for (s, src, sid, tgt, tid) in MANUAL_PLAYER_OVERRIDES
        if s == season
    ]


def get_override_lookup(
    season: str,
    source: str,
    target: str,
) -> dict[int, int]:
    """Get a lookup dict for a specific source→target direction.

    Args:
        season: Season string.
        source: Source system ("fpl", "vaastav", "understat").
        target: Target system.

    Returns:
        Dict mapping source_id → target_id.
    """
    lookup: dict[int, int] = {}
    for s, src, sid, tgt, tid in MANUAL_PLAYER_OVERRIDES:
        if s == season and src == source and tgt == target:
            lookup[sid] = tid
    return lookup
