"""Upload data to Supabase with Medallion Architecture.

This script uploads local raw data to Supabase following the Bronze/Silver/Gold
layer pattern:
- Bronze: Raw data as ingested
- Silver: Cleaned, standardized data
- Gold: Aggregated features and predictions
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import polars as pl
from supabase import create_client

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

CURRENT_SEASON = "2025-26"


def get_supabase_client() -> Any:
    """Create Supabase client from environment variables."""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in .env")

    return create_client(url, key)


def upload_table(
    df: pl.DataFrame,
    table_name: str,
    client: Any,
    batch_size: int = 500,
    use_upsert: bool = True,
    truncate: bool = False,  # New: optionally truncate before upload
) -> None:
    """Upload a DataFrame to Supabase in batches.

    Args:
        df: DataFrame to upload
        table_name: Target Supabase table
        client: Supabase client
        batch_size: Rows per batch
        use_upsert: Use upsert vs insert
        truncate: If True, clear table before upload (prevents duplicates)
    """
    import subprocess

    if df is None:
        logger.info(f"  ⏭️  Skipping {table_name} (None)")
        return

    if hasattr(df, "is_empty") and df.is_empty():
        logger.info(f"  ⏭️  Skipping {table_name} (empty)")
        return

    if client is None:
        logger.error(f"  ❌ No client provided for {table_name}")
        return

    # Truncate if requested (avoids duplicates)
    if truncate:
        token = os.getenv("SUPABASE_ACCESS_TOKEN")
        if token:
            try:
                result = subprocess.run(
                    [
                        "supabase",
                        "db",
                        "query",
                        "--linked",
                        f"TRUNCATE {table_name} CASCADE;",
                    ],
                    capture_output=True,
                    text=True,
                    env={**os.environ, "SUPABASE_ACCESS_TOKEN": token},
                )
                if result.returncode == 0:
                    logger.info(f"  🗑️  Truncated {table_name}")
            except Exception:
                pass  # Continue without truncate if CLI fails

    records = df.to_dicts()
    total = len(records)
    logger.info(f"  📤 Uploading {table_name}: {total} rows...")

    for i in range(0, total, batch_size):
        batch = records[i : i + batch_size]
        try:
            if use_upsert:
                client.table(table_name).upsert(batch).execute()
            else:
                client.table(table_name).insert(batch).execute()
            logger.info(f"     ✅ Rows {i + 1}-{min(i + batch_size, total)}")
        except Exception as e:
            logger.warning(f"     ⚠️  Error at batch {i}: {e}")
            # Continue despite errors (data may already exist)

    logger.info(f"  ✅ {table_name} complete!")


# ============================================================================
# BRONZE LAYER UPLOADS
# ============================================================================


def upload_bronze_fpl_players(client: Any) -> None:
    """Upload FPL players to Bronze layer."""
    logger.info("\n🥉 BRONZE: FPL Players...")

    try:
        with open(f"data/raw/fpl/{CURRENT_SEASON}/bootstrap-static.json") as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"  ❌ Error loading FPL data: {e}")
        return

    try:
        players_df = pl.DataFrame(data["elements"])
        players_df = players_df.with_columns(pl.lit(CURRENT_SEASON).alias("season"))

        # Select columns that match schema
        player_schema_cols = [
            "id",
            "web_name",
            "first_name",
            "second_name",
            "team",
            "element_type",
            "now_cost",
            "total_points",
            "selected_by_percent",
            "status",
            "code",
            "photo",
            "points_per_game",
            "form",
            "value_season",
            "value_form",
            "transfers_in",
            "transfers_out",
            "transfers_in_event",
            "transfers_out_event",
            "news",
            "chance_of_playing_next_round",
            "chance_of_playing_this_round",
            "cost_change_event",
            "cost_change_event_fall",
            "cost_change_start",
            "cost_change_start_fall",
            "dreamteam_count",
            "ep_next",
            "ep_this",
            "event_points",
            "in_dreamteam",
            "removed",
            "special",
            "squad_number",
            "team_code",
            "minutes",
            "goals_scored",
            "assists",
            "clean_sheets",
            "goals_conceded",
            "own_goals",
            "penalties_saved",
            "penalties_missed",
            "yellow_cards",
            "red_cards",
            "saves",
            "bonus",
            "bps",
            "influence",
            "creativity",
            "threat",
            "ict_index",
            "clearances_blocks_interceptions",
            "recoveries",
            "tackles",
            "defensive_contribution",
            "starts",
            "expected_goals",
            "expected_assists",
            "expected_goal_involvements",
            "expected_goals_conceded",
            "season",
        ]
        available_cols = [c for c in player_schema_cols if c in players_df.columns]
        players_df = players_df.select(available_cols)

        upload_table(players_df, "bronze_fpl_players", client, truncate=True)
    except Exception as e:
        logger.error(f"  ❌ Error: {e}")


def upload_bronze_fpl_teams(client: Any) -> None:
    """Upload FPL teams to Bronze layer."""
    logger.info("\n🥉 BRONZE: FPL Teams...")

    try:
        with open(f"data/raw/fpl/{CURRENT_SEASON}/bootstrap-static.json") as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"  ❌ Error loading FPL data: {e}")
        return

    try:
        teams_df = pl.DataFrame(data["teams"])
        teams_df = teams_df.with_columns(pl.lit(CURRENT_SEASON).alias("season"))

        team_schema_cols = [
            "id",
            "name",
            "short_name",
            "code",
            "strength",
            "strength_overall_home",
            "strength_overall_away",
            "strength_attack_home",
            "strength_attack_away",
            "strength_defence_home",
            "strength_defence_away",
            "pulse_id",
            "season",
        ]
        available_cols = [c for c in team_schema_cols if c in teams_df.columns]
        teams_df = teams_df.select(available_cols)

        upload_table(teams_df, "bronze_fpl_teams", client, truncate=True)
    except Exception as e:
        logger.error(f"  ❌ Error: {e}")


def upload_bronze_fpl_fixtures(client: Any) -> None:
    """Upload FPL fixtures to Bronze layer."""
    logger.info("\n🥉 BRONZE: FPL Fixtures...")

    try:
        # Use fixtures.json which has all the fixture data
        with open(f"data/raw/fpl/{CURRENT_SEASON}/fixtures.json") as f:
            fixtures_data = json.load(f)
    except Exception as e:
        logger.error(f"  ❌ Error loading fixtures.json: {e}")
        return

    try:
        fixtures_df = pl.DataFrame(fixtures_data)
        fixtures_df = fixtures_df.with_columns(pl.lit(CURRENT_SEASON).alias("season"))

        # Select columns that match schema
        schema_cols = [
            "id",
            "event",
            "team_h",
            "team_a",
            "finished",
            "started",
            "team_h_score",
            "team_a_score",
            "kickoff_time",
            "team_h_difficulty",
            "team_a_difficulty",
            "pulse_id",
            "season",
        ]
        available_cols = [c for c in schema_cols if c in fixtures_df.columns]
        fixtures_df = fixtures_df.select(available_cols)

        if not fixtures_df.is_empty():
            upload_table(fixtures_df, "bronze_fpl_fixtures", client, truncate=True)
        else:
            logger.info("  ⏭️  No fixtures data")
    except Exception as e:
        logger.error(f"  ❌ Error: {e}")


def upload_bronze_fpl_gw(client: Any) -> None:
    """Upload FPL GW data (player_history) to Bronze layer."""
    logger.info("\n🥉 BRONZE: FPL GW Data...")

    # FPL GW data is only available for current season (2025-26)
    fpl_gw_file = f"data/raw/fpl/{CURRENT_SEASON}/player_history.parquet"

    try:
        df = pl.read_parquet(fpl_gw_file)
        logger.info(f"  Loaded {len(df)} rows from FPL GW data")

        # Drop player_id (duplicate of element) - Supabase schema uses 'element'
        if "player_id" in df.columns:
            df = df.drop("player_id")

        # Add season - keep all original columns (no renaming for Bronze)
        df = df.with_columns(pl.lit(CURRENT_SEASON).alias("season"))

        # Deduplicate: keep row with most minutes per player-gameweek
        # (handles cases where player appears twice - likely sub appearances)
        if (
            "element" in df.columns
            and "round" in df.columns
            and "minutes" in df.columns
        ):
            df = df.sort(
                ["element", "round", "minutes"],
                descending=[False, False, True],
            )
            df = df.unique(subset=["element", "round"], keep="first")
            logger.info(f"  Deduped to {len(df)} rows")

        # Upload ALL columns (no filtering - dump raw data to Bronze)
        upload_table(df, "bronze_fpl_gw", client, truncate=True)

    except Exception as e:
        logger.error(f"  ❌ Error: {e}")


def upload_bronze_player_history(client: Any) -> None:
    """Upload Vaastav player history to Bronze layer."""
    logger.info("\n🥉 BRONZE: Vaastav Player History...")

    # Known columns in Supabase schema (filter to only these)
    valid_cols = {
        "player_id",
        "gameweek",
        "team",
        "minutes",
        "goals_scored",
        "assists",
        "clean_sheets",
        "goals_conceded",
        "expected_goals",
        "expected_assists",
        "total_points",
        "was_home",
        "opponent_team",
        "season",
        "name",
        "position",
        "bonus",
        "bps",
        "saves",
        "starts",
        "own_goals",
        "penalties_missed",
        "penalties_saved",
        "yellow_cards",
        "red_cards",
        "creativity",
        "influence",
        "threat",
        "ict_index",
        "expected_goal_involvements",
        "expected_goals_conceded",
        "transfers_in",
        "transfers_out",
        "value",
        "selected",
        "fixture",
        "kickoff_time",
        "team_a_score",
        "team_h_score",
        "xp",
    }

    for season in ["2021-22", "2022-23", "2023-24", "2024-25"]:
        try:
            df = pl.read_parquet(f"data/raw/vaastav/{season}/gws.parquet")

            # Rename columns to match Supabase schema
            # GW -> gameweek (drop round if it exists alongside GW)
            if "GW" in df.columns:
                df = df.rename({"GW": "gameweek"})
                if "round" in df.columns:
                    df = df.drop("round")
            elif "round" in df.columns:
                df = df.rename({"round": "gameweek"})

            if "element" in df.columns:
                df = df.rename({"element": "player_id"})

            # Rename xP to xp (lowercase) to match Supabase schema
            if "xP" in df.columns:
                df = df.rename({"xP": "xp"})

            df = df.with_columns(pl.lit(season).alias("season"))

            # Drop columns that don't exist in Supabase schema
            cols_to_keep = [c for c in df.columns if c in valid_cols]
            df = df.select(cols_to_keep)

            # Deduplicate: keep row with most minutes per player-gameweek-season
            if (
                "player_id" in df.columns
                and "gameweek" in df.columns
                and "minutes" in df.columns
                and "season" in df.columns
            ):
                df = df.sort(
                    ["player_id", "gameweek", "season", "minutes"],
                    descending=[False, False, False, True],
                )
                df = df.unique(subset=["player_id", "gameweek", "season"], keep="first")
                logger.info(f"  Deduped to {len(df)} rows for {season}")

            # Filter out null player_ids
            df = df.filter(pl.col("player_id").is_not_null())

            upload_table(df, "bronze_player_history", client, truncate=True)
        except Exception as e:
            logger.error(f"  ❌ Error loading {season}: {e}")


def upload_bronze_vaastav_fixtures(client: Any) -> None:
    """Extract and upload Vaastav fixtures from gws data."""
    logger.info("\n🥉 BRONZE: Vaastav Fixtures...")

    all_fixtures = []

    for season in ["2021-22", "2022-23", "2023-24", "2024-25"]:
        try:
            df = pl.read_parquet(f"data/raw/vaastav/{season}/gws.parquet")

            # Deduplicate: keep row with most minutes per player-gameweek
            df = df.sort(
                ["element", "round", "minutes"], descending=[False, False, True]
            )
            df = df.unique(subset=["element", "round"], keep="first")

            # Get unique home games
            home = (
                df.filter(pl.col("was_home") == True)
                .select(
                    [
                        "fixture",
                        "round",
                        "kickoff_time",
                        "team",
                        "opponent_team",
                        "team_h_score",
                        "team_a_score",
                    ]
                )
                .unique(subset=["fixture"])
                .rename(
                    {
                        "team": "team_h",
                        "team_h_score": "team_h_score",
                        "team_a_score": "team_a_score",
                    }
                )
            )

            # Get unique away games
            away = (
                df.filter(pl.col("was_home") == False)
                .select(["fixture", "team", "opponent_team"])
                .unique(subset=["fixture"])
                .rename({"team": "team_a", "opponent_team": "team_h"})
            )

            # Join to get complete fixture
            fixtures = home.join(away, on="fixture", how="inner")
            fixtures = fixtures.with_columns(
                pl.lit(season).alias("season"),
                pl.col("round").alias("event"),
                pl.col("fixture").alias("id"),
                pl.lit(True).alias("finished"),
                pl.lit(True).alias("started"),
            )

            # Select and order columns to match FPL fixtures
            fixtures = fixtures.select(
                [
                    "id",
                    "event",
                    "kickoff_time",
                    "team_h",
                    "team_a",
                    "team_h_score",
                    "team_a_score",
                    "finished",
                    "started",
                    "season",
                ]
            )

            all_fixtures.append(fixtures)
            logger.info(f"  {season}: {len(fixtures)} fixtures")

        except Exception as e:
            logger.error(f"  ❌ Error loading {season}: {e}")

    if all_fixtures:
        combined = pl.concat(all_fixtures).sort(["season", "event"])
        upload_table(combined, "bronze_vaastav_fixtures", client, truncate=True)
        logger.info(f"  ✅ Uploaded {len(combined)} total fixtures")


def upload_bronze_team_mappings(client: Any) -> None:
    """Upload team mappings to Bronze layer."""
    logger.info("\n🥉 BRONZE: Team Mappings...")

    try:
        mappings_df = pl.read_csv("data/raw/team_mappings.csv")
        upload_table(mappings_df, "bronze_team_mappings", client, truncate=True)
    except Exception as e:
        logger.error(f"  ❌ Error: {e}")


def upload_bronze_understat(client: Any) -> None:
    """Upload Understat data to Bronze layer."""
    logger.info("\n🥉 BRONZE: Understat Data...")

    # Schema columns that exist in bronze_understat_shots table
    shots_schema_cols = [
        "game_id",
        "player_id",
        "team_id",
        "assist_player_id",
        "assist_player",
        "xg",
        "location_x",
        "location_y",
        "minute",
        "body_part",
        "situation",
        "result",
        "date",
        "season",
    ]

    for season in ["2021_22", "2022_23", "2023_24", "2024_25", "2025_26"]:
        season_dir = Path(f"data/raw/understat/{season}")
        if not season_dir.exists():
            continue

        # Load match_stats to get game_id -> gameweek mapping
        match_file = season_dir / "match_stats.parquet"
        gameweek_map = {}
        if match_file.exists():
            match_df = pl.read_parquet(match_file)
            match_df = match_df.sort("date")
            for gw, row in enumerate(match_df.iter_rows(named=True), 1):
                gameweek_map[row["game_id"]] = gw

        # Player match stats
        stats_file = season_dir / "player_match_stats.parquet"
        if stats_file.exists():
            try:
                logger.info(f"  📄 {season}/player_match_stats")
                df = pl.read_parquet(stats_file)
                df = df.with_columns(pl.lit(season.replace("_", "-")).alias("season"))

                # Map game_id to gameweek
                if "game_id" in df.columns:
                    df = df.with_columns(
                        pl.col("game_id")
                        .map_elements(
                            lambda x: gameweek_map.get(x), return_dtype=pl.Int64
                        )
                        .alias("gameweek")
                    )
                else:
                    df = df.with_columns(pl.lit(None).alias("gameweek"))

                cols = [
                    "player_id",
                    "gameweek",
                    "game_id",
                    "team_id",
                    "position",
                    "minutes",
                    "goals",
                    "assists",
                    "shots",
                    "xg",
                    "xa",
                    "xg_chain",
                    "xg_buildup",
                    "key_passes",
                    "yellow_cards",
                    "red_cards",
                    "season",
                ]
                available = [c for c in cols if c in df.columns]
                df = df.select(available)

                upload_table(df, "bronze_understat_player_stats", client, truncate=True)
            except Exception as e:
                logger.error(f"  ❌ Error {season}/player_match_stats: {e}")

        # Shots
        shots_file = season_dir / "shots.parquet"
        if shots_file.exists():
            try:
                logger.info(f"  📄 {season}/shots")
                df = pl.read_parquet(shots_file)
                df = df.with_columns(pl.lit(season.replace("_", "-")).alias("season"))

                # Drop datetime columns that can't be serialized to JSON
                datetime_cols = [c for c in df.columns if df[c].dtype == pl.Datetime]
                if datetime_cols:
                    df = df.drop(datetime_cols)

                available_cols = [c for c in shots_schema_cols if c in df.columns]
                df = df.select(available_cols)

                upload_table(df, "bronze_understat_shots", client, truncate=True)
            except Exception as e:
                logger.error(f"  ❌ Error {season}/shots: {e}")

        # Match stats
        match_file = season_dir / "match_stats.parquet"
        if match_file.exists():
            try:
                logger.info(f"  📄 {season}/match_stats")
                df = pl.read_parquet(match_file)
                df = df.with_columns(pl.lit(season.replace("_", "-")).alias("season"))

                # Drop datetime columns that can't be serialized to JSON
                datetime_cols = [c for c in df.columns if df[c].dtype == pl.Datetime]
                if datetime_cols:
                    df = df.drop(datetime_cols)

                # Filter to only columns that exist in Supabase schema
                valid_cols = {
                    "game_id",
                    "date",
                    "season",
                    "home_team_id",
                    "away_team_id",
                    "home_team",
                    "away_team",
                    "home_goals",
                    "away_goals",
                    "home_xg",
                    "away_xg",
                    "home_np_xg",
                    "away_np_xg",
                    "home_np_xg_difference",
                    "away_np_xg_difference",
                    "home_ppda",
                    "away_ppda",
                    "home_deep_completions",
                    "away_deep_completions",
                    "home_expected_points",
                    "away_expected_points",
                    "home_points",
                    "away_points",
                    "away_team_code",
                    "home_team_code",
                    "league_id",
                }
                cols_to_keep = [c for c in df.columns if c in valid_cols]
                df = df.select(cols_to_keep)

                upload_table(df, "bronze_understat_match_stats", client, truncate=True)
            except Exception as e:
                logger.error(f"  ❌ Error {season}/match_stats: {e}")


def upload_bronze_understat_mappings(client: Any) -> None:
    """Fetch and upload Understat player/team name mappings via soccerdata.

    Uses read_player_match_stats() instead of read_shot_events() because
    it returns more complete player coverage (all players who played, not just shooters).
    """
    logger.info("\n🥉 BRONZE: Understat Mappings (via soccerdata)...")

    try:
        import soccerdata as sd
    except ImportError:
        logger.warning("  ⏭️  soccerdata not installed, skipping Understat mappings")
        return

    all_players = []
    all_teams = []

    for season in ["2021-22", "2022-23", "2023-24", "2024-25", "2025-26"]:
        try:
            us = sd.Understat(leagues=["ENG-Premier League"], seasons=[season])

            # Use read_player_match_stats for complete player coverage
            # This includes all players who played, not just shooters
            df = us.read_player_match_stats()

            if df is None or df.empty:
                logger.warning(f"    No data for {season}")
                continue

            # Reset index to get all fields
            df = df.reset_index()
            # Convert to polars
            df = pl.DataFrame(df)

            # Get player mappings from MATCH stats (not season stats!)
            # This ensures we get ALL teams a player has played for in a season (handles transfers)
            player_count = 0
            if "player_id" in df.columns and "team_id" in df.columns:
                # Use player_match_stats to get all team appearances
                player_df = df.select(
                    ["player_id", "player", "team_id", "team"]
                ).unique(subset=["player_id", "team_id"])
                player_df = player_df.with_columns(
                    pl.lit(season.replace("-", "_")).alias("season")
                )
                all_players.append(player_df)
                player_count = len(player_df)

            # Get team mappings: unique (team_id, team) combos
            team_count = 0
            if "team_id" in df.columns and "team" in df.columns:
                team_df = df.select(["team_id", "team"]).unique()
                team_df = team_df.with_columns(
                    pl.lit(season.replace("-", "_")).alias("season")
                )
                all_teams.append(team_df)
                team_count = len(team_df)

            logger.info(
                f"  📥 Fetched {season}: {player_count} players, {team_count} teams"
            )
        except Exception as e:
            logger.warning(f"    Failed {season}: {e}")

    # Upload player mappings
    if all_players:
        # IMPORTANT: Use player_id + team_id as unique key
        # Players who transferred within a season should appear once per team
        players_df = pl.concat(all_players).unique(subset=["player_id", "team_id"])

        # Rename columns to match Supabase schema
        rename_map = {}
        if "player_id" in players_df.columns:
            rename_map["player_id"] = "understat_player_id"
        if "player" in players_df.columns:
            rename_map["player"] = "understat_player_name"
        if "team_id" in players_df.columns:
            rename_map["team_id"] = "understat_team_id"
        if "team" in players_df.columns:
            rename_map["team"] = "understat_team_name"

        if rename_map:
            players_df = players_df.rename(rename_map)

        # Filter to only known Supabase columns
        valid_cols = {
            "understat_player_id",
            "understat_player_name",
            "understat_team_id",
            "understat_team_name",
            "season",
        }
        cols_to_keep = [c for c in players_df.columns if c in valid_cols]
        players_df = players_df.select(cols_to_keep)

        # Clear existing data first to avoid PK conflicts
        logger.info(f"  🗑️  Clearing existing bronze_understat_player_mappings...")
        try:
            # Delete all rows (the condition always matches)
            client.table("bronze_understat_player_mappings").delete().execute()
        except Exception as e:
            logger.warning(f"    Clear failed: {e}")

        # Small delay to ensure delete completes
        import time

        time.sleep(1)

        # Upload with insert (not upsert)
        upload_table(
            players_df, "bronze_understat_player_mappings", client, use_upsert=False
        )
        logger.info(f"  ✅ Uploaded {len(players_df)} player mappings")

    # Upload team mappings
    if all_teams:
        teams_df = pl.concat(all_teams).unique(subset=["team_id"])
        teams_df = teams_df.rename(
            {"team_id": "understat_team_id", "team": "understat_team_name"}
        )
        upload_table(teams_df, "bronze_understat_team_mappings", client, truncate=True)
        logger.info(f"  ✅ Uploaded {len(teams_df)} team mappings")


# ============================================================================
# MAIN
# ============================================================================


def main() -> None:
    """Main upload function."""
    logger.info("🚀 Starting Supabase upload with Medallion Architecture...")
    logger.info(f"   Season: {CURRENT_SEASON}")

    try:
        client = get_supabase_client()
        logger.info("🔗 Connected to Supabase")
    except Exception as e:
        logger.error(f"❌ Failed to connect: {e}")
        return

    # Wait for schema cache
    logger.info("⏳ Waiting 5s for schema cache to refresh...")
    time.sleep(5)

    # BRONZE LAYER
    logger.info("\n" + "=" * 50)
    logger.info("BRONZE LAYER (Raw Data)")
    logger.info("=" * 50)

    upload_bronze_fpl_players(client)
    upload_bronze_fpl_teams(client)
    upload_bronze_fpl_fixtures(client)
    upload_bronze_fpl_gw(client)  # New: FPL GW data
    upload_bronze_player_history(client)
    upload_bronze_vaastav_fixtures(client)  # New: Vaastav fixtures
    upload_bronze_team_mappings(client)
    upload_bronze_understat(client)
    upload_bronze_understat_mappings(client)  # New: Understat player/team mappings

    # SILVER LAYER (skipped - will be implemented in future session)
    logger.info("\n" + "=" * 50)
    logger.info("SILVER LAYER (Cleaned Data)")
    logger.info("=" * 50)
    logger.info("  ⏭️  Silver layer will be implemented in future session")

    # GOLD LAYER (For future: features and predictions)
    logger.info("\n" + "=" * 50)
    logger.info("GOLD LAYER (Features & Predictions)")
    logger.info("=" * 50)
    logger.info("  ⏭️  Gold layer will be populated by daily_update.py after ML runs")

    logger.info("\n✅ Upload complete!")
    logger.info("\n📊 Layer Summary:")
    logger.info("  - Bronze: Raw data from all sources")
    logger.info("  - Silver: Will be implemented in future session")
    logger.info("  - Gold: Features and predictions (via daily pipeline)")


if __name__ == "__main__":
    main()
