"""Player mapping module for Silver layer.

Creates unified player identity mapping across FPL, Vaastav, and Understat
per season. Uses fuzzy name matching with team-based disambiguation.

Matching strategy (in priority order):
1. Manual overrides (player_overrides.py) — known problem cases
2. Exact name + team match — confidence 1.0
3. Fuzzy name + same team — Levenshtein-based, boosted +0.2
4. Last name + same team + position — for partial name matches
5. Fuzzy name + cross-team + position — lower confidence (×0.7)
6. Exact name cross-team — for mid-season transfers (confidence 0.9)

Thresholds:
- EXACT: 1.0 — character-perfect match
- HIGH: 0.85 — strong fuzzy match, likely correct
- MEDIUM: 0.65 — reasonable fuzzy match with team confirmation
- LOW: 0.55 — weak match, only if position matches
- FALLBACK: 0.40 — minimum threshold to consider any fuzzy match
"""

from __future__ import annotations

import logging
from typing import Any

import polars as pl
from dotenv import load_dotenv

from src.config import ALL_SEASONS, CURRENT_SEASON
from src.data.database import get_supabase_client
from src.utils.name_resolver import (
    build_name_mapping,
    standardize_name,
)
from src.utils.safe_upsert import clean_records_for_upload, safe_upsert
from src.utils.supabase_utils import fetch_all_paginated

logger = logging.getLogger(__name__)

load_dotenv()

# ── Matching thresholds ─────────────────────────────────────────────────────
EXACT_MATCH = 1.0  # Character-perfect name match
HIGH_CONFIDENCE = 0.85  # Strong fuzzy match — very likely correct
MEDIUM_CONFIDENCE = 0.65  # Reasonable match with team confirmation
LOW_CONFIDENCE = 0.55  # Weak match — needs position verification
MIN_FUZZY_THRESHOLD = 0.40  # Floor — below this, ignore fuzzy match
LAST_NAME_BOOST = 0.2  # Added when fuzzy match found within same team
TRANSFER_CONFIDENCE = 0.9  # Exact name match across teams (transfers)

# Position normalization
POS_MAP = {
    "GKP": "GKP",
    "GK": "GKP",
    "GOALKEEPER": "GKP",
    "G": "GKP",
    "DEF": "DEF",
    "D": "DEF",
    "DC": "DEF",
    "DL": "DEF",
    "DR": "DEF",
    "MID": "MID",
    "M": "MID",
    "MC": "MID",
    "ML": "MID",
    "MR": "MID",
    "DMC": "MID",
    "DML": "MID",
    "DMR": "MID",
    "FWD": "FWD",
    "F": "FWD",
    "FW": "FWD",
    "AMC": "FWD",
    "AML": "FWD",
    "AMR": "FWD",
}


def _normalize_position(pos: str | None) -> str:
    """Normalize position to GKP/DEF/MID/FWD."""
    if not pos:
        return ""
    return POS_MAP.get(str(pos).upper(), str(pos).upper()[0] if str(pos) else "")


def get_season_sources(season: str) -> dict:
    """Determine which data sources are available for a season."""
    # Historical seasons available from Vaastav (all except current)
    historical_seasons = [s for s in ALL_SEASONS if s != CURRENT_SEASON]
    sources = {
        "fpl": season == CURRENT_SEASON,  # FPL only has current season
        "vaastav": season in historical_seasons,
        "understat": True,  # Understat has all seasons
    }
    return sources


def get_season_source_type(season: str) -> str:
    """Get the primary source type for a season (for team name lookup)."""
    if season == CURRENT_SEASON:
        return "fpl"
    return "vaastav"


def get_supabase():
    """Get Supabase client."""
    client = get_supabase_client()
    if client is None:
        raise ValueError("Failed to connect to Supabase")
    return client


def load_fpl_players(season: str) -> pl.DataFrame:
    """Load FPL players for a season from Bronze layer."""
    client = get_supabase()

    # FPL players are stored per season - use pagination to get all records
    # Include known_name for better matching against Understat
    df = pl.DataFrame(
        fetch_all_paginated(
            client,
            "bronze_fpl_players",
            select_cols="id,web_name,first_name,second_name,known_name,team,element_type,season",
            filters={"season": season},
        )
    )

    if df.is_empty():
        logger.warning(f"No FPL players found for season {season}")
        return pl.DataFrame()

    # Map element_type to position using replace
    df = df.with_columns(
        pl.when(pl.col("element_type") == 1)
        .then(pl.lit("GKP"))
        .when(pl.col("element_type") == 2)
        .then(pl.lit("DEF"))
        .when(pl.col("element_type") == 3)
        .then(pl.lit("MID"))
        .otherwise(pl.lit("FWD"))
        .alias("position")
    )

    # Get team names
    teams_df = pl.DataFrame(
        fetch_all_paginated(
            client,
            "bronze_fpl_teams",
            select_cols="id,name,season",
            filters={"season": season},
        )
    )

    if not teams_df.is_empty():
        df = df.join(
            teams_df.rename({"id": "team", "name": "team_name"}),
            on="team",
            how="left",
        )

    # Normalize team names to match Understat format (after join)
    if "team_name" in df.columns:
        # Map FPL team names to Understat format
        team_name_mapping = {
            "Man City": "Manchester City",
            "Man Utd": "Manchester United",
            "Newcastle": "Newcastle United",
            "Spurs": "Tottenham",
            "Wolves": "Wolverhampton Wanderers",
            "Nott'm Forest": "Nottingham Forest",
        }

        def normalize_team_name(name: str | None) -> str | None:
            return team_name_mapping.get(name, name) if name else None

        df = df.with_columns(
            pl.col("team_name")
            .map_elements(normalize_team_name, return_dtype=pl.Utf8)
            .alias("team_name"),
        )

    return df


def get_understat_team_id_lookup(client: Any, season: str) -> dict[str, str]:
    """Get team name to Understat team ID mapping for a season.

    Returns dict mapping (normalized) team name -> understat_team_id
    """
    team_mappings = fetch_all_paginated(
        client,
        "silver_team_mapping",
        filters={"season": season},
        select_cols="fpl_team_name,vaastav_team_name,understat_team_id",
    )

    # FPL team name normalization (to match what load_fpl_players produces)
    fpl_normalization = {
        "Spurs": "Tottenham",
        "Man City": "Manchester City",
        "Man Utd": "Manchester United",
        "Newcastle": "Newcastle United",
        "Wolves": "Wolverhampton Wanderers",
        "Nott'm Forest": "Nottingham Forest",
    }

    lookup: dict[str, str] = {}
    for r in team_mappings:
        us_id = r.get("understat_team_id")
        if us_id:
            # Add FPL team name
            if r.get("fpl_team_name"):
                lookup[r["fpl_team_name"]] = str(us_id)
                # Also add normalized version
                normalized = fpl_normalization.get(r["fpl_team_name"])
                if normalized:
                    lookup[normalized] = str(us_id)
            # Add Vaastav team name
            if r.get("vaastav_team_name"):
                lookup[r["vaastav_team_name"]] = str(us_id)

    return lookup


def load_vaastav_players(season: str) -> pl.DataFrame:
    """Load Vaastav players for a season from Bronze layer.

    Returns ONE ROW PER UNIQUE PLAYER (not per player-team combo).
    This ensures we don't create duplicate entries for players who transferred.
    The team info is still available for matching purposes.
    """
    client = get_supabase()

    # Vaastav uses player_id, name, position, team
    # Use pagination to get all records
    df = pl.DataFrame(
        fetch_all_paginated(
            client,
            "bronze_vaastav_player_history_gw",
            select_cols="player_id,name,position,team,season",
            filters={"season": season},
        )
    )

    if df.is_empty():
        logger.warning(f"No Vaastav players found for season {season}")
        return pl.DataFrame()

    # Keep only ONE ROW PER PLAYER - take first occurrence
    # This prevents duplicate entries for players who transferred within a season
    df = df.unique(subset=["player_id"], maintain_order=True)

    return df


def load_understat_players(season: str) -> pl.DataFrame:
    """Load Understat players for a season from bronze table.

    Uses bronze_understat_player_mappings which has player names and team info.
    """
    client = get_supabase()

    # Load player mappings (this table has player names from player_season_stats)
    df = pl.DataFrame(
        fetch_all_paginated(
            client,
            "bronze_understat_player_mappings",
            select_cols="understat_player_id,understat_player_name,understat_team_id,understat_team_name,season",
            filters={"season": season},
        )
    )

    if df.is_empty():
        logger.warning(f"No Understat players found for season {season}")
        return pl.DataFrame()

    # Get unique players per team (handles transfers)
    df = df.unique(subset=["understat_player_id", "understat_team_id"])

    # Rename to match expected column names
    df = df.rename(
        {
            "understat_player_id": "understat_id",
            "understat_player_name": "player",
            "understat_team_id": "team_id_str",
        }
    )

    # Convert team_id to string for lookup matching
    df = df.with_columns(
        pl.col("team_id_str").cast(pl.Utf8).alias("team_id_str"),
    )

    # Position column - not available in mappings, set to None
    df = df.with_columns(pl.lit(None).alias("position"))

    return df.select(["understat_id", "team_id_str", "position", "season", "player"])


def standardize_player_names(df: pl.DataFrame, name_col: str) -> pl.DataFrame:
    """Standardize player names in a DataFrame."""
    if name_col not in df.columns:
        return df

    return df.with_columns(
        pl.col(name_col)
        .map_elements(standardize_name, return_dtype=pl.Utf8)
        .alias(name_col)
    )


def match_players_with_team(
    source_df: pl.DataFrame,
    source_id_col: str,
    source_name_col: str,
    source_team_col: str | None,
    target_df: pl.DataFrame,
    target_id_col: str,
    target_name_col: str,
    target_team_col: str | None,
    source_position_col: str | None = None,
    target_position_col: str | None = None,
) -> pl.DataFrame:
    """Match players using name + team for disambiguation.

    Returns a DataFrame with source ID mapped to target ID.
    """
    results = []

    source_names = source_df[source_name_col].to_list()
    source_ids = source_df[source_id_col].to_list()
    source_teams = (
        source_df[source_team_col].to_list()
        if source_team_col
        else [None] * len(source_names)
    )
    source_positions = (
        source_df[source_position_col].to_list()
        if source_position_col
        else [None] * len(source_names)
    )

    target_names = target_df[target_name_col].to_list()
    target_ids = target_df[target_id_col].to_list()
    target_teams = (
        target_df[target_team_col].to_list()
        if target_team_col
        else [None] * len(target_names)
    )
    target_positions = (
        target_df[target_position_col].to_list()
        if target_position_col
        else [None] * len(target_names)
    )

    # Helper to normalize position to FPL format
    def get_pos_code(pos):
        if not pos:
            return ""
        # FPL codes: GKP, DEF, MID, FWD
        # Understat codes: GK, G, DC, DL, DR, MC, ML, MR, AMC, AML, AMR, FW, etc.
        pos_str = str(pos).upper()
        if pos_str in ["GKP", "GK", "GOALKEEPER"]:
            return "GKP"
        if pos_str in ["DEF", "D", "DC", "DL", "DR"]:
            return "DEF"
        if pos_str in ["MID", "M", "MC", "ML", "MR", "DMC", "DML", "DMR"]:
            return "MID"
        if pos_str in ["FWD", "F", "FW", "AMC", "AML", "AMR"]:
            return "FWD"
        return pos_str[0] if pos_str else ""

    # NEW APPROACH: More aggressive matching
    # 1. Exact name + team
    # 2. Fuzzy match (lower threshold) within same team
    # 3. Fuzzy match across teams with position verification
    # 4. Last name only match within same team
    logger.debug("Building player mappings with team-first approach...")

    # Build name mapping with lower threshold to get more candidates
    name_mapping = build_name_mapping(
        source_names,
        target_names,
        0.40,  # Lower threshold
    )

    # Group target by team for efficient lookup
    team_targets: dict[str, list[tuple]] = {}
    for tgt_id, tgt_name, tgt_team, tgt_pos in zip(
        target_ids, target_names, target_teams, target_positions
    ):
        if tgt_team:
            if tgt_team not in team_targets:
                team_targets[tgt_team] = []
            team_targets[tgt_team].append((tgt_id, tgt_name, tgt_pos))

    for src_id, src_name, src_team, src_pos in zip(
        source_ids, source_names, source_teams, source_positions
    ):
        matched_target_id = None
        confidence = 0.0
        match_type = "none"

        src_pos_code = get_pos_code(src_pos)
        src_last_name = (
            src_name.split()[-1] if src_name else ""
        )  # Last name for fallback

        # 1. Exact match with team
        if src_team and src_team in team_targets:
            for tgt_id, tgt_name, tgt_pos in team_targets[src_team]:
                if tgt_name == src_name:
                    matched_target_id = tgt_id
                    confidence = EXACT_MATCH
                    match_type = "exact"
                    break

        # 2. Fuzzy match within same team
        if matched_target_id is None and src_team and src_team in team_targets:
            mapped_name = name_mapping.get(src_name, (src_name, 0.0))
            matched_name, confidence = mapped_name

            if confidence >= 0.40:
                for tgt_id, tgt_name, tgt_pos in team_targets[src_team]:
                    if tgt_name == matched_name:
                        matched_target_id = tgt_id
                        confidence = min(confidence + 0.2, 1.0)
                        match_type = "fuzzy"
                        break

        # 3. Last name only match within same team (for cases like "Gabriel" vs "Gabriel Magalhaes")
        if (
            matched_target_id is None
            and src_team
            and src_team in team_targets
            and src_last_name
        ):
            for tgt_id, tgt_name, tgt_pos in team_targets[src_team]:
                # Check both directions: last name in target OR target in source
                if (
                    src_last_name in tgt_name
                    or tgt_name in src_name
                    or tgt_name.split()[-1] == src_last_name
                ):
                    tgt_pos_code = get_pos_code(tgt_pos)
                    # For DEF/GKP positions, accept more lenient matching
                    # Understat uses "DC", "DL", "DR" for defenders, "GK" for goalkeepers
                    if tgt_pos_code == src_pos_code:
                        matched_target_id = tgt_id
                        confidence = 0.65
                        match_type = "lastname"
                        break
                    elif src_pos_code in ["DEF", "GKP"] and tgt_pos_code in [
                        "DEF",
                        "GKP",
                        "D",
                        "G",
                    ]:
                        # DEF/GKP can match Understat's D/G/DEF/GKP
                        matched_target_id = tgt_id
                        confidence = 0.55
                        match_type = "lastname_pos"
                        break

        # 4. Fuzzy across teams with position verification (last resort)
        if matched_target_id is None:
            mapped_name = name_mapping.get(src_name, (src_name, 0.0))
            matched_name, confidence = mapped_name

            if confidence >= 0.45:
                for tgt_id, tgt_name, tgt_team, tgt_pos in zip(
                    target_ids, target_names, target_teams, target_positions
                ):
                    if tgt_name == matched_name:
                        tgt_pos_code = get_pos_code(tgt_pos)
                        if tgt_pos_code == src_pos_code:  # Position must match
                            matched_target_id = tgt_id
                            confidence = confidence * 0.7  # Lower confidence
                            match_type = "fuzzy_pos"
                            break

        # 5. Exact name match across teams (for transfers - same player transferred mid-season)
        # If exact name match but different teams, still match (Understat may have old team)
        if matched_target_id is None:
            for tgt_id, tgt_name, tgt_team, tgt_pos in zip(
                target_ids, target_names, target_teams, target_positions
            ):
                if tgt_name == src_name:  # Exact name match
                    matched_target_id = tgt_id
                    confidence = 0.90  # High confidence for exact match
                    match_type = "exact_transfer"
                    break

        results.append(
            {
                "source_id": src_id,
                "matched_id": matched_target_id,
                "confidence": confidence,
                "match_type": match_type,
            }
        )

    return pl.DataFrame(results)


def build_season_mappings(season: str) -> pl.DataFrame:
    """Build player mappings for a single season."""
    logger.info(f"Processing season: {season}")

    # Determine which sources are available
    sources = get_season_sources(season)
    logger.info(f"  Available sources: {sources}")

    # Load data from Bronze layer
    fpl_df = load_fpl_players(season)
    vaastav_df = load_vaastav_players(season)
    understat_df = load_understat_players(season)

    # Build result based on available data
    all_players = []

    # Use FPL for current season
    if sources["fpl"] and not fpl_df.is_empty():
        fpl_df = standardize_player_names(fpl_df, "web_name")

        # Create match_name: use known_name if available, otherwise full_name
        # known_name contains the canonical/full name (e.g., "Gabriel Magalhães")
        # which matches Understat's naming format
        fpl_df = fpl_df.with_columns(
            (pl.col("first_name") + " " + pl.col("second_name")).alias("full_name")
        )
        # Use coalesce to prefer known_name, fall back to full_name
        fpl_df = fpl_df.with_columns(
            pl.coalesce(
                pl.col("known_name").map_elements(
                    lambda x: x.strip() if x else None, return_dtype=pl.Utf8
                ),
                pl.col("full_name"),
            ).alias("match_name")
        )
        fpl_df = standardize_player_names(fpl_df, "match_name")

        # Also standardize full_name for fallback matching
        fpl_df = standardize_player_names(fpl_df, "full_name")

        # Get position if available
        pos_col = "position" if "position" in fpl_df.columns else None
        team_col = "team_name" if "team_name" in fpl_df.columns else None

        # Use match_name for player_name (better matching with Understat)
        cols = ["id", "match_name"]
        if pos_col:
            cols.append(pos_col)
        if team_col:
            cols.append(team_col)

        result = fpl_df.select(cols).rename(
            {
                "id": "fpl_id",
                "match_name": "player_name",
            }
        )

        if pos_col:
            result = result.with_columns(fpl_df[pos_col].alias("position"))
        if team_col:
            result = result.with_columns(fpl_df[team_col].alias("team"))

        result = result.with_columns(pl.lit(season).alias("season"))

        # Add mappings to other sources
        if sources["vaastav"] and not vaastav_df.is_empty():
            mapping = match_players_with_team(
                fpl_df,
                "id",
                "web_name",
                "team_name",
                vaastav_df,
                "player_id",
                "name",
                "team",
            )
            result = result.join(
                mapping.rename(
                    {
                        "source_id": "fpl_id",
                        "matched_id": "vaastav_id",
                        "confidence": "vaastav_confidence",
                    }
                ),
                on="fpl_id",
                how="left",
            )

        # Add Understat mapping using name + team matching
        if sources["understat"] and not understat_df.is_empty():
            # Get team name to Understat team ID mapping
            us_team_lookup = get_understat_team_id_lookup(get_supabase(), season)

            # Add understat_team_id to FPL dataframe
            fpl_df = fpl_df.with_columns(
                pl.col("team_name")
                .map_elements(lambda x: us_team_lookup.get(x, ""), return_dtype=pl.Utf8)
                .alias("understat_team_id")
            )

            understat_df = standardize_player_names(understat_df, "player")

            # Use match_name (which includes known_name) for better matching
            mapping = match_players_with_team(
                fpl_df,
                "id",
                "match_name",  # This uses known_name when available
                "understat_team_id",
                understat_df,
                "understat_id",
                "player",
                "team_id_str",
                source_position_col="position",
                target_position_col="position",
            )

            matched = mapping.filter(pl.col("matched_id").is_not_null()).height
            logger.info(f"  Understat mapping: {matched}/{len(fpl_df)} matched")

            result = result.join(
                mapping.rename(
                    {
                        "source_id": "fpl_id",
                        "matched_id": "understat_id",
                        "confidence": "understat_confidence",
                    }
                ),
                on="fpl_id",
                how="left",
            )

            # Deduplicate: same understat_id mapped to multiple FPL players
            if "understat_id" in result.columns:
                has_ust = result.filter(pl.col("understat_id").is_not_null())
                no_ust = result.filter(pl.col("understat_id").is_null())
                if not has_ust.is_empty():
                    has_ust = has_ust.sort("understat_confidence", descending=True)
                    has_ust = has_ust.unique(subset=["understat_id"], keep="first")
                    result = pl.concat([has_ust, no_ust])

        all_players.append(result)

    # Use Vaastav for historical seasons (if no FPL)
    elif sources["vaastav"] and not vaastav_df.is_empty():
        vaastav_df = standardize_player_names(vaastav_df, "name")

        cols = ["player_id", "name"]
        if "position" in vaastav_df.columns:
            cols.append("position")
        if "team" in vaastav_df.columns:
            cols.append("team")

        result = vaastav_df.select(cols).rename(
            {
                "player_id": "vaastav_id",
                "name": "player_name",
            }
        )

        result = result.with_columns(pl.lit(season).alias("season"))

        # Add Understat mapping using name + team matching
        if sources["understat"] and not understat_df.is_empty():
            understat_df = standardize_player_names(understat_df, "player")
            # Use team_id_str for matching (string version of Understat team_id)
            target_team = (
                "team_id_str" if "team_id_str" in understat_df.columns else None
            )
            if target_team is None:
                # Fallback if we don't have team_id_str
                target_team = None
            mapping = match_players_with_team(
                vaastav_df,
                "player_id",
                "name",
                "team",
                understat_df,
                "understat_id",
                "player",
                target_team,
            )
            result = result.join(
                mapping.rename(
                    {
                        "source_id": "vaastav_id",
                        "matched_id": "understat_id",
                        "confidence": "understat_confidence",
                    }
                ),
                on="vaastav_id",
                how="left",
            )

            # Deduplicate: same understat_id mapped to multiple vaastav players
            # Keep only the highest-confidence match for each understat_id
            if "understat_id" in result.columns:
                has_ust = result.filter(pl.col("understat_id").is_not_null())
                no_ust = result.filter(pl.col("understat_id").is_null())
                if not has_ust.is_empty():
                    has_ust = has_ust.sort("understat_confidence", descending=True)
                    has_ust = has_ust.unique(subset=["understat_id"], keep="first")
                    result = pl.concat([has_ust, no_ust])

        all_players.append(result)

    if not all_players:
        logger.warning(f"No data for season {season}")
        return pl.DataFrame()

    result = pl.concat(all_players)

    # Calculate confidence
    conf_cols = [c for c in result.columns if c.endswith("_confidence")]
    if conf_cols:
        result = result.with_columns(
            pl.mean_horizontal([pl.col(c) for c in conf_cols]).alias("confidence_score")
        )
    else:
        result = result.with_columns(pl.lit(0.0).alias("confidence_score"))

    result = result.with_columns(
        pl.col("confidence_score").fill_null(0.0).alias("confidence_score"),
        (pl.col("confidence_score") < HIGH_CONFIDENCE).alias("requires_manual_review"),
        pl.when(pl.col("confidence_score") == EXACT_MATCH)
        .then(pl.lit("exact"))
        .when(pl.col("confidence_score") >= HIGH_CONFIDENCE)
        .then(pl.lit("fuzzy"))
        .otherwise(pl.lit("manual"))
        .alias("source"),
    )

    auto_matched = result.filter(~pl.col("requires_manual_review")).shape[0]
    needs_review = result.filter(pl.col("requires_manual_review")).shape[0]

    logger.info(
        f"Season {season}: {result.shape[0]} players, {auto_matched} auto, {needs_review} need review"
    )

    return result


def build_all_season_mappings() -> pl.DataFrame:
    """Build player mappings for all seasons."""
    client = get_supabase()

    # Load team mappings to resolve unified_team_id for ALL sources
    team_mappings = fetch_all_paginated(
        client,
        "silver_team_mapping",
        select_cols="season,fpl_team_name,fpl_team_id,vaastav_team_name,understat_team_id,understat_team_name,unified_team_id",
    )

    # FPL team name normalization (FPL uses short names, we normalize to Understat format)
    fpl_team_normalization = {
        "Man City": "Manchester City",
        "Man Utd": "Manchester United",
        "Newcastle": "Newcastle United",
        "Spurs": "Tottenham",
        "Wolves": "Wolverhampton Wanderers",
        "Nott'm Forest": "Nottingham Forest",
    }

    # Build team lookup: (source_type, season, team_name_or_id) -> unified_team_id
    # Include all name variations for each source
    team_lookup: dict[tuple[str, str, str], str] = {}
    for r in team_mappings:
        season = r.get("season")
        unified_id = r.get("unified_team_id")
        if season and unified_id:
            # Vaastav team names
            if r.get("vaastav_team_name"):
                team_lookup[("vaastav", season, r["vaastav_team_name"])] = unified_id
            # FPL team names - add original AND normalized version
            if r.get("fpl_team_name"):
                # Original FPL name (e.g., "Spurs")
                team_lookup[("fpl", season, r["fpl_team_name"])] = unified_id
                # Normalized FPL name (e.g., "Tottenham")
                normalized = fpl_team_normalization.get(r["fpl_team_name"])
                if normalized:
                    team_lookup[("fpl", season, normalized)] = unified_id
            # FPL team ID (numeric)
            if r.get("fpl_team_id"):
                team_lookup[("fpl", season, str(r["fpl_team_id"]))] = unified_id
            # Understat team ID (numeric) - CRITICAL for matching Understat players
            if r.get("understat_team_id"):
                team_lookup[("understat", season, str(r["understat_team_id"]))] = (
                    unified_id
                )
                # Add understat_team_id to FPL lookup for team-based matching
                team_lookup[("fpl", season, str(r["understat_team_id"]))] = unified_id
            # Understat team names (string)
            if r.get("understat_team_name"):
                team_lookup[("understat", season, r["understat_team_name"])] = (
                    unified_id
                )
                # Also add understat names to FPL lookup since player data is normalized to understat format
                team_lookup[("fpl", season, r["understat_team_name"])] = unified_id

    logger.debug(f"Built team_lookup with {len(team_lookup)} entries")

    all_mappings = []

    for season in ALL_SEASONS:
        season_mapping = build_season_mappings(season)
        if not season_mapping.is_empty():
            all_mappings.append(season_mapping)

    if not all_mappings:
        logger.warning("No player mappings generated")
        return pl.DataFrame()

    # Align columns across all seasons - use explicit column order
    fixed_cols = [
        "player_name",
        "position",
        "team",
        "unified_team_id",
        "season",
        "fpl_id",
        "vaastav_id",
        "understat_id",
        "confidence_score",
        "requires_manual_review",
        "source",
    ]

    # Convert team_lookup to a DataFrame for joining
    lookup_data = []
    for (source, season, team), uuid in team_lookup.items():
        lookup_data.append(
            {
                "source_type": source,
                "season": season,
                "team": team,
                "unified_team_id": uuid,
            }
        )
    lookup_df = pl.DataFrame(lookup_data)

    aligned = []
    for df in all_mappings:
        # Resolve unified_team_id from team name using correct source type
        if "team" in df.columns and "season" in df.columns:
            # Add source_type column based on season
            df = df.with_columns(
                pl.when(pl.col("season") == CURRENT_SEASON)
                .then(pl.lit("fpl"))
                .otherwise(pl.lit("vaastav"))
                .alias("source_type")
            )
            # Join with team lookup
            df = df.join(lookup_df, on=["source_type", "season", "team"], how="left")
            # Drop temp column
            df = df.drop("source_type")

        # Add any missing columns
        for col in fixed_cols:
            if col not in df.columns:
                # Determine type based on column name
                if col in ["fpl_id", "vaastav_id", "understat_id"]:
                    df = df.with_columns(pl.lit(None).cast(pl.Int64).alias(col))
                elif col in ["confidence_score"]:
                    df = df.with_columns(pl.lit(0.0).cast(pl.Float64).alias(col))
                elif col in ["requires_manual_review"]:
                    df = df.with_columns(pl.lit(False).cast(pl.Boolean).alias(col))
                else:
                    df = df.with_columns(pl.lit(None).alias(col))
        # Select in fixed order
        cols_present = [c for c in fixed_cols if c in df.columns]
        aligned.append(df.select(cols_present))

    return pl.concat(aligned)


def upload_to_supabase(mappings: pl.DataFrame) -> int:
    """Upload mappings to Supabase silver_player_mapping table.

    Uses safe_upsert with deduplication by (season, fpl_id) business key.
    No longer deletes all rows before inserting — safe for concurrent access.

    Args:
        mappings: DataFrame with player mapping records.

    Returns:
        Number of records written.
    """
    client = get_supabase()

    # Prepare records — strip server-managed columns
    records = clean_records_for_upload(mappings.to_dicts())
    total = len(records)

    logger.info(f"Uploading {total} player mappings to Supabase...")

    written = safe_upsert(
        client,
        "silver_player_mapping",
        records,
        business_key=["season", "fpl_id"],
        score_column=None,  # No data_quality_score column — keep last dedup wins
    )

    if written:
        logger.info(f"Successfully uploaded {written} player mappings")
    else:
        logger.error("Failed to upload player mappings")

    return written


def run() -> None:
    """Main entry point for player mapping generation."""
    logger.info("Starting player mapping generation for Silver layer...")

    # Build mappings
    mappings = build_all_season_mappings()

    if mappings.is_empty():
        logger.error("No mappings generated, aborting")
        return

    # Upload to Supabase
    uploaded = upload_to_supabase(mappings)

    logger.info(f"Player mapping complete! Uploaded {uploaded} records")


if __name__ == "__main__":
    run()
