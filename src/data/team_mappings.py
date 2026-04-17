"""Team mapping utilities for cross-source team ID resolution."""

from __future__ import annotations

from pathlib import Path

import polars as pl

TEAM_MAPPINGS_PATH = Path("data/raw/team_mappings.csv")


def load_team_mappings() -> pl.DataFrame:
    """Load team mappings from the CSV file.

    Returns:
        Polars DataFrame with columns: season, source, source_team_id,
        source_team_name, fpl_team_id, fpl_team_name
    """
    if not TEAM_MAPPINGS_PATH.exists():
        raise FileNotFoundError(f"Team mappings not found at {TEAM_MAPPINGS_PATH}")

    return pl.read_csv(TEAM_MAPPINGS_PATH)


def get_fpl_team_id(season: str, source: str, source_value: int | str) -> int | None:
    """Resolve a team ID from any source to the canonical FPL team ID.

    Args:
        season: Season string (e.g., "2024-25")
        source: Data source ("fpl", "vaastav", "understat")
        source_value: The team's ID (int) or name (str) from the source

    Returns:
        FPL team ID (1-20) or None if not found
    """
    mappings = load_team_mappings()

    # Determine which column to match against
    if source == "fpl":
        match_col = "source_team_id"
        # source_team_id is stored as string in CSV, so compare as string
        source_value_str = str(int(source_value))
    elif source == "understat":
        match_col = "source_team_id"
        source_value_str = str(int(source_value))
    elif source == "vaastav":
        match_col = "source_team_name"
        source_value_str = str(source_value)
    else:
        raise ValueError(f"Unknown source: {source}")

    result = mappings.filter(
        (pl.col("season") == season)
        & (pl.col("source") == source)
        & (pl.col(match_col) == source_value_str)
    )

    if result.is_empty():
        return None

    return result["fpl_team_id"][0]


def get_understat_team_id(season: str, fpl_team_id: int) -> int | None:
    """Get Understat team ID for a given FPL team ID and season.

    Args:
        season: Season string (e.g., "2024-25")
        fpl_team_id: FPL team ID (1-20)

    Returns:
        Understat team ID or None if not found
    """
    mappings = load_team_mappings()

    result = mappings.filter(
        (pl.col("season") == season)
        & (pl.col("source") == "understat")
        & (pl.col("fpl_team_id") == pl.lit(fpl_team_id))
    )

    if result.is_empty():
        return None

    # source_team_id is stored as string, convert to int
    return int(result["source_team_id"][0])


def get_vaastav_team_name(fpl_team_id: int) -> str | None:
    """Get the Vaastav team name for a given FPL team ID.

    Note: This returns the FPL canonical name for the most recent season.

    Args:
        fpl_team_id: FPL team ID (1-20)

    Returns:
        Vaastav team name (e.g., "Man City") or None if not found
    """
    mappings = load_team_mappings()

    result = mappings.filter(
        (pl.col("source") == "vaastav") & (pl.col("fpl_team_id") == pl.lit(fpl_team_id))
    )

    if result.is_empty():
        return None

    # Return most recent season's FPL canonical name
    return result.sort("season", descending=True)["fpl_team_name"][0]


def append_mappings(new_mappings: pl.DataFrame, allow_updates: bool = True) -> None:
    """Append new team mappings to the CSV file.

    Args:
        new_mappings: DataFrame with columns: season, source, source_team_id,
            source_team_name, fpl_team_id, fpl_team_name
        allow_updates: If True, update existing rows; if False, skip duplicates
    """
    if not TEAM_MAPPINGS_PATH.exists():
        # Create new file with headers
        TEAM_MAPPINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
        new_mappings.write_csv(TEAM_MAPPINGS_PATH)
        return

    # Load existing mappings
    existing = load_team_mappings()

    # Filter out duplicates if not allowing updates
    if not allow_updates:
        # Remove rows that already exist (same season + source + source_team_id)
        new_mappings = new_mappings.join(
            existing.select(["season", "source", "source_team_id"]),
            on=["season", "source", "source_team_id"],
            how="anti",
        )

    if new_mappings.is_empty():
        return

    # Append new rows
    combined = pl.concat([existing, new_mappings])
    combined.write_csv(TEAM_MAPPINGS_PATH)


def create_fpl_mappings(season: str, teams_df: pl.DataFrame) -> pl.DataFrame:
    """Create team mappings from FPL teams data.

    Args:
        season: Season string (e.g., "2024-25")
        teams_df: DataFrame with FPL team data (must have id, name columns)

    Returns:
        DataFrame with mapping columns ready for append_mappings
    """
    return pl.DataFrame(
        {
            "season": season,
            "source": "fpl",
            "source_team_id": teams_df["id"].to_list(),
            "source_team_name": teams_df["name"].to_list(),
            "fpl_team_id": teams_df["id"].to_list(),
            "fpl_team_name": teams_df["name"].to_list(),
        }
    )


def create_understat_mappings(
    season: str,
    teams_df: pl.DataFrame,
    understat_team_names: dict[int, str],
    fpl_team_id_map: dict[int, int],
) -> pl.DataFrame:
    """Create team mappings from Understat data.

    Args:
        season: Season string (e.g., "2024-25")
        teams_df: DataFrame with unique Understat team IDs
        understat_team_names: Dict mapping Understat ID to team name
        fpl_team_id_map: Dict mapping Understat ID to FPL team ID

    Returns:
        DataFrame with mapping columns ready for append_mappings
    """
    rows = []
    for understat_id in teams_df["team_id"].to_list():
        fpl_id = fpl_team_id_map.get(understat_id)
        if fpl_id is None:
            continue
        rows.append(
            {
                "season": season,
                "source": "understat",
                "source_team_id": understat_id,
                "source_team_name": understat_team_names.get(
                    understat_id, f"Team_{understat_id}"
                ),
                "fpl_team_id": fpl_id,
                "fpl_team_name": "",
            }
        )

    df = pl.DataFrame(rows)

    # Fill FPL names from existing mappings
    try:
        existing = load_team_mappings()
        fpl_names = existing.filter(
            (pl.col("season") == season) & (pl.col("source") == "fpl")
        )
        if not fpl_names.is_empty():
            name_map = dict(zip(fpl_names["fpl_team_id"], fpl_names["fpl_team_name"]))
            df = df.with_columns(
                pl.col("fpl_team_id")
                .map_elements(lambda x: name_map.get(x, ""), return_dtype=pl.String)
                .alias("fpl_team_name")
            )
    except Exception:
        pass

    return df


def create_vaastav_mappings(
    season: str,
    vaastav_team_names: list[str],
    fpl_team_id_map: dict[str, int],
) -> pl.DataFrame:
    """Create team mappings from Vaastav team names.

    Args:
        season: Season string (e.g., "2024-25")
        vaastav_team_names: List of Vaastav team names
        fpl_team_id_map: Dict mapping Vaastav name to FPL team ID

    Returns:
        DataFrame with mapping columns ready for append_mappings
    """
    rows = []
    for vaastav_name in vaastav_team_names:
        fpl_id = fpl_team_id_map.get(vaastav_name)
        if fpl_id is None:
            continue
        rows.append(
            {
                "season": season,
                "source": "vaastav",
                "source_team_id": "",
                "source_team_name": vaastav_name,
                "fpl_team_id": fpl_id,
                "fpl_team_name": "",
            }
        )

    df = pl.DataFrame(rows)

    # Fill FPL names from existing mappings
    try:
        existing = load_team_mappings()
        fpl_names = existing.filter(
            (pl.col("season") == season) & (pl.col("source") == "fpl")
        )
        if not fpl_names.is_empty():
            name_map = dict(zip(fpl_names["fpl_team_id"], fpl_names["fpl_team_name"]))
            df = df.with_columns(
                pl.col("fpl_team_id")
                .map_elements(lambda x: name_map.get(x, ""), return_dtype=pl.String)
                .alias("fpl_team_name")
            )
    except Exception:
        pass

    return df
