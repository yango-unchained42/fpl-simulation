     1|"""Daily Bronze layer update for FPL Simulation.
     2|
     3|This script runs daily to:
     4|1. Check if FPL source data has been updated
     5|2. If updated, fetch and upsert to Bronze tables
     6|3. Check if Understat source data has been updated
     7|4. If updated, fetch and upsert to Bronze tables
     8|
     9|Only processes current season data (no historical).
    10|Uses incremental updates - only upserts changed data.
    11|"""
    12|
    13|from __future__ import annotations
    14|
    15|import hashlib
    16|import json
    17|import logging
    18|import os
    19|import time
    20|from datetime import datetime
    21|from pathlib import Path
    22|from typing import Any
    23|
    24|import polars as pl
    25|import requests
    26|from dotenv import load_dotenv
    27|
    28|from src.config import BATCH_SIZE, CURRENT_SEASON, FPL_API_BASE, get_supabase as _get_supabase
    29|
    30|logging.basicConfig(level=logging.INFO)
    31|logger = logging.getLogger(__name__)
    32|
    33|# Configuration — can be overridden by --season arg
    34|SEASON = CURRENT_SEASON
    35|
    36|
    37|def get_supabase():
    38|    """Get Supabase client."""
    39|    load_dotenv()
    40|    return _get_supabase()
    41|
    42|
    43|DATA_DIR = Path("data/raw/fpl")
    44|
    45|
    46|def compute_file_hash(file_path: Path) -> str:
    47|    """Compute MD5 hash of a file."""
    48|    if not file_path.exists():
    49|        return ""
    50|    with open(file_path, "rb") as f:
    51|        return hashlib.md5(f.read()).hexdigest()
    52|
    53|
    54|def get_source_hash(table_name: str, season: str = SEASON) -> str:
    55|    """Get the current hash of source data from local cache."""
    56|    season_dir = DATA_DIR / season
    57|
    58|    if table_name == "fpl_players":
    59|        return compute_file_hash(season_dir / "bootstrap-static.json")
    60|    elif table_name == "fpl_fixtures":
    61|        return compute_file_hash(season_dir / "fixtures.json")
    62|    elif table_name == "fpl_gw":
    63|        return compute_file_hash(season_dir / "player_history.parquet")
    64|    return ""
    65|
    66|
    67|def has_source_changed(table_name: str, season: str = SEASON) -> bool:
    68|    """Check if source data has changed since last upload."""
    69|    supabase = get_supabase()
    70|
    71|    # Get last update timestamp from metadata table
    72|    try:
    73|        result = (
    74|            supabase.table("metadata")
    75|            .select("*")
    76|            .eq("table_name", table_name)
    77|            .eq("season", season)
    78|            .execute()
    79|        )
    80|        if result.data:
    81|            last_hash = result.data[0].get("source_hash", "")
    82|            current_hash = get_source_hash(table_name, season)
    83|            return last_hash != current_hash
    84|    except Exception:
    85|        pass
    86|
    87|    # If no metadata, assume changed
    88|    return True
    89|
    90|
    91|def update_metadata(table_name: str, season: str, row_count: int) -> None:
    92|    """Update metadata table with current hash and row count."""
    93|    supabase = get_supabase()
    94|    current_hash = get_source_hash(table_name, season)
    95|
    96|    try:
    97|        supabase.table("metadata").upsert(
    98|            {
    99|                "table_name": table_name,
   100|                "season": season,
   101|                "source_hash": current_hash,
   102|                "row_count": row_count,
   103|                "last_updated": datetime.now().isoformat(),
   104|            },
   105|            on_conflict="table_name,season",
   106|        ).execute()
   107|    except Exception as e:
   108|        logger.warning(f"Could not update metadata: {e}")
   109|
   110|
   111|def fetch_fpl_players() -> pl.DataFrame:
   112|    """Fetch FPL players from API or local cache."""
   113|    season_dir = DATA_DIR / SEASON
   114|    cache_file = season_dir / "bootstrap-static.json"
   115|
   116|    # Try API first
   117|    try:
   118|        logger.info("  Fetching FPL players from API...")
   119|        response = requests.get(f"{FPL_API_BASE}bootstrap-static/", timeout=30)
   120|        response.raise_for_status()
   121|        data = response.json()
   122|
   123|        # Cache locally
   124|        cache_file.parent.mkdir(parents=True, exist_ok=True)
   125|        with open(cache_file, "w") as f:
   126|            json.dump(data, f)
   127|
   128|        return pl.DataFrame(data.get("elements", []))
   129|    except Exception as e:
   130|        logger.warning(f"  API fetch failed: {e}, using cache")
   131|
   132|    # Fallback to cache
   133|    if cache_file.exists():
   134|        with open(cache_file) as f:
   135|            data = json.load(f)
   136|        return pl.DataFrame(data.get("elements", []))
   137|
   138|    return pl.DataFrame()
   139|
   140|
   141|def fetch_fpl_fixtures() -> pl.DataFrame:
   142|    """Fetch FPL fixtures from API or local cache."""
   143|    season_dir = DATA_DIR / SEASON
   144|    cache_file = season_dir / "fixtures.json"
   145|
   146|    # Try API first
   147|    try:
   148|        logger.info("  Fetching FPL fixtures from API...")
   149|        response = requests.get(f"{FPL_API_BASE}fixtures/", timeout=30)
   150|        response.raise_for_status()
   151|        data = response.json()
   152|
   153|        # Cache locally
   154|        with open(cache_file, "w") as f:
   155|            json.dump(data, f)
   156|
   157|        return pl.DataFrame(data)
   158|    except Exception as e:
   159|        logger.warning(f"  API fetch failed: {e}, using cache")
   160|
   161|    # Fallback to cache
   162|    if cache_file.exists():
   163|        with open(cache_file) as f:
   164|            data = json.load(f)
   165|        return pl.DataFrame(data)
   166|
   167|    return pl.DataFrame()
   168|
   169|
   170|def fetch_fpl_gw() -> pl.DataFrame:
   171|    """Fetch FPL gameweek data from API or local cache."""
   172|    season_dir = DATA_DIR / SEASON
   173|    cache_file = season_dir / "player_history.parquet"
   174|
   175|    # Try API first
   176|    try:
   177|        logger.info("  Fetching FPL GW data from API...")
   178|        response = requests.get(f"{FPL_API_BASE}element-summary/{1}/", timeout=30)
   179|        # This would need to iterate all players - for now use cache approach
   180|        # Actually FPL doesn't have a bulk GW endpoint, we use the cached approach
   181|    except Exception:
   182|        pass
   183|
   184|    # Fallback to cache
   185|    if cache_file.exists():
   186|        df = pl.read_parquet(cache_file)
   187|        # Deduplicate: keep row with most minutes per player-gameweek
   188|        df = df.sort(["element", "round", "minutes"], descending=[False, False, True])
   189|        df = df.unique(subset=["element", "round"], keep="first")
   190|        return df
   191|
   192|    return pl.DataFrame()
   193|
   194|
   195|def fetch_fpl_teams() -> pl.DataFrame:
   196|    """Fetch FPL teams from API or local cache."""
   197|    season_dir = DATA_DIR / SEASON
   198|    cache_file = season_dir / "bootstrap-static.json"
   199|
   200|    if cache_file.exists():
   201|        with open(cache_file) as f:
   202|            data = json.load(f)
   203|        return pl.DataFrame(data.get("teams", []))
   204|
   205|    return pl.DataFrame()
   206|
   207|
   208|def get_table_columns(supabase, table_name: str) -> set:
   209|    """Get columns that exist in Supabase table."""
   210|    try:
   211|        result = supabase.table(table_name).select("*").limit(1).execute()
   212|        if result.data:
   213|            return set(result.data[0].keys())
   214|    except Exception:
   215|        pass
   216|    return set()
   217|
   218|
   219|def filter_to_schema(df: pl.DataFrame, valid_cols: set) -> pl.DataFrame:
   220|    """Filter DataFrame to only include columns in valid_cols."""
   221|    cols_to_keep = [c for c in df.columns if c in valid_cols]
   222|    return df.select(cols_to_keep)
   223|
   224|
   225|def upload_table(supabase, table_name: str, df: pl.DataFrame) -> int:
   226|    """Upload DataFrame to Supabase, returning row count.
   227|
   228|    Truncates table before upload to avoid duplicates from multiple runs.
   229|    """
   230|    import os
   231|    import subprocess
   232|
   233|    if df.is_empty():
   234|        logger.info(f"  ⏭️  Skipping {table_name} (empty)")
   235|        return 0
   236|
   237|    # Truncate table to avoid duplicates (use CLI if available)
   238|    token = os.getenv("SUPABASE_ACCESS_TOKEN")
   239|    if token:
   240|        try:
   241|            result = subprocess.run(
   242|                [
   243|                    "supabase",
   244|                    "db",
   245|                    "query",
   246|                    "--linked",
   247|                    f"TRUNCATE {table_name} CASCADE;",
   248|                ],
   249|                capture_output=True,
   250|                text=True,
   251|                env={**os.environ, "SUPABASE_ACCESS_TOKEN": token},
   252|            )
   253|            if result.returncode == 0:
   254|                logger.info(f"  🗑️  Truncated {table_name}")
   255|        except Exception:
   256|            pass  # Continue without truncate if CLI fails
   257|
   258|    # Filter to only columns that exist in Supabase schema
   259|    valid_cols = get_table_columns(supabase, table_name)
   260|
   261|    # Convert datetime columns to ISO strings for JSON serialization
   262|    datetime_cols = [c for c in df.columns if df[c].dtype == pl.Datetime]
   263|    if datetime_cols:
   264|        for col in datetime_cols:
   265|            df = df.with_columns(
   266|                pl.col(col).dt.strftime("%Y-%m-%dT%H:%M:%S").alias(col)
   267|            )
   268|
   269|    df = filter_to_schema(df, valid_cols)
   270|
   271|    if df.is_empty():
   272|        logger.info(f"  ⏭️  Skipping {table_name} (no valid columns)")
   273|        return 0
   274|
   275|    records = df.to_dicts()
   276|    total = len(records)
   277|    logger.info(f"  📤 Uploading {table_name}: {total} rows...")
   278|
   279|    for i in range(0, total, BATCH_SIZE):
   280|        batch = records[i : i + BATCH_SIZE]
   281|        try:
   282|            supabase.table(table_name).upsert(batch).execute()
   283|        except Exception as e:
   284|            logger.error(f"  ❌ Error at batch {i}: {e}")
   285|
   286|    logger.info(f"  ✅ {table_name} complete!")
   287|    return total
   288|
   289|
   290|def update_fpl_bronze() -> bool:
   291|    """Update FPL Bronze tables if source changed."""
   292|    supabase = get_supabase()
   293|    updated = False
   294|
   295|    # 1. FPL Players
   296|    if has_source_changed("fpl_players"):
   297|        logger.info("🥉 FPL Players - source changed, updating...")
   298|        df = fetch_fpl_players()
   299|        if not df.is_empty():
   300|            df = df.with_columns(pl.lit(SEASON).alias("season"))
   301|            upload_table(supabase, "bronze_fpl_players", df)
   302|            update_metadata("fpl_players", SEASON, len(df))
   303|            updated = True
   304|    else:
   305|        logger.info("🥉 FPL Players - no changes, skipping")
   306|
   307|    # 2. FPL Teams
   308|    if has_source_changed("fpl_teams"):
   309|        logger.info("🥉 FPL Teams - source changed, updating...")
   310|        df = fetch_fpl_teams()
   311|        if not df.is_empty():
   312|            df = df.with_columns(pl.lit(SEASON).alias("season"))
   313|            upload_table(supabase, "bronze_fpl_teams", df)
   314|            update_metadata("fpl_teams", SEASON, len(df))
   315|            updated = True
   316|    else:
   317|        logger.info("🥉 FPL Teams - no changes, skipping")
   318|
   319|    # 3. FPL Fixtures
   320|    if has_source_changed("fpl_fixtures"):
   321|        logger.info("🥉 FPL Fixtures - source changed, updating...")
   322|        df = fetch_fpl_fixtures()
   323|        if not df.is_empty():
   324|            df = df.with_columns(pl.lit(SEASON).alias("season"))
   325|            upload_table(supabase, "bronze_fpl_fixtures", df)
   326|            update_metadata("fpl_fixtures", SEASON, len(df))
   327|            updated = True
   328|    else:
   329|        logger.info("🥉 FPL Fixtures - no changes, skipping")
   330|
   331|    # 4. FPL GW (only if new gameweek data exists)
   332|    if has_source_changed("fpl_gw"):
   333|        logger.info("🥉 FPL GW - source changed, updating...")
   334|        df = fetch_fpl_gw()
   335|        if not df.is_empty():
   336|            df = df.with_columns(pl.lit(SEASON).alias("season"))
   337|            # Drop player_id if exists (duplicate of element)
   338|            if "player_id" in df.columns:
   339|                df = df.drop("player_id")
   340|            upload_table(supabase, "bronze_fpl_gw", df)
   341|            update_metadata("fpl_gw", SEASON, len(df))
   342|            updated = True
   343|    else:
   344|        logger.info("🥉 FPL GW - no changes, skipping")
   345|
   346|    return updated
   347|
   348|
   349|def update_understat_bronze() -> bool:
   350|    """Update Understat Bronze tables if source changed."""
   351|    supabase = get_supabase()
   352|    updated = False
   353|
   354|    # Understat - fetch current season from API
   355|    logger.info("🥉 Understat - fetching from API...")
   356|
   357|    try:
   358|        from src.data.ingest_understat import (
   359|            ingest_understat_player_match_stats,
   360|            ingest_understat_shots,
   361|            ingest_understat_match_stats,
   362|        )
   363|
   364|        # Only fetch current season
   365|        current_season_list = [SEASON.replace("-", "_")]
   366|
   367|        # 1. Player match stats (xG, xA, shots per player-game)
   368|        logger.info("  🥉 Fetching understat_player_stats...")
   369|        df = ingest_understat_player_match_stats(
   370|            seasons=current_season_list, use_cache=False
   371|        )
   372|        if not df.is_empty():
   373|            # Add season column
   374|            df = df.with_columns(pl.lit(SEASON).alias("season"))
   375|            # Filter to PL
   376|            if "league_id" in df.columns:
   377|                df = df.filter(pl.col("league_id") == "1")
   378|            # Drop null keys
   379|            if "player_id" in df.columns:
   380|                df = df.filter(pl.col("player_id").is_not_null())
   381|            if "game_id" in df.columns:
   382|                df = df.filter(pl.col("game_id").is_not_null())
   383|            if "season_id" in df.columns:
   384|                df = df.drop("season_id")
   385|
   386|            upload_table(supabase, "bronze_understat_player_stats", df)
   387|            update_metadata("understat_player_match_stats", SEASON, len(df))
   388|            updated = True
   389|            logger.info(f"    Uploaded {len(df)} player stats")
   390|        else:
   391|            logger.info("    No data returned")
   392|
   393|        # 2. Shots
   394|        logger.info("  🥉 Fetching understat_shots...")
   395|        df = ingest_understat_shots(seasons=current_season_list, use_cache=False)
   396|        if not df.is_empty():
   397|            df = df.with_columns(pl.lit(SEASON).alias("season"))
   398|            if "league_id" in df.columns:
   399|                df = df.filter(pl.col("league_id") == "1")
   400|            if "shot_id" in df.columns:
   401|                df = df.filter(pl.col("shot_id").is_not_null())
   402|                df = df.rename({"shot_id": "id"})
   403|            if "season_id" in df.columns:
   404|                df = df.drop("season_id")
   405|
   406|            upload_table(supabase, "bronze_understat_shots", df)
   407|            update_metadata("understat_shots", SEASON, len(df))
   408|            updated = True
   409|            logger.info(f"    Uploaded {len(df)} shots")
   410|        else:
   411|            logger.info("    No data returned")
   412|
   413|        # 3. Match stats
   414|        logger.info("  🥉 Fetching understat_match_stats...")
   415|        df = ingest_understat_match_stats(seasons=current_season_list, use_cache=False)
   416|        if not df.is_empty():
   417|            df = df.with_columns(pl.lit(SEASON).alias("season"))
   418|            if "league_id" in df.columns:
   419|                df = df.filter(pl.col("league_id") == "1")
   420|            if "game_id" in df.columns:
   421|                df = df.filter(pl.col("game_id").is_not_null())
   422|            if "season_id" in df.columns:
   423|                df = df.drop("season_id")
   424|
   425|            upload_table(supabase, "bronze_understat_match_stats", df)
   426|            update_metadata("understat_match_stats", SEASON, len(df))
   427|            updated = True
   428|            logger.info(f"    Uploaded {len(df)} match stats")
   429|        else:
   430|            logger.info("    No data returned")
   431|
   432|    except Exception as e:
   433|        logger.error(f"  ❌ Error fetching Understat from API: {e}")
   434|
   435|    return updated
   436|
   437|
   438|def create_metadata_table(supabase) -> None:
   439|    """Create metadata table if it doesn't exist."""
   440|    try:
   441|        supabase.table("metadata").select("*").limit(1).execute()
   442|    except Exception:
   443|        logger.info("Creating metadata table...")
   444|        # This would need SQL - in practice you'd run this once in Supabase
   445|        pass
   446|
   447|
   448|def main() -> None:
   449|    """Main daily Bronze update function."""
   450|    import argparse
   451|
   452|    global SEASON  # noqa: PLW0603
   453|
   454|    parser = argparse.ArgumentParser(description="Daily Bronze layer update")
   455|    parser.add_argument(
   456|        "--season",
   457|        type=str,
   458|        default=SEASON,
   459|        help=f"Season to update (default: {SEASON})",
   460|    )
   461|    args = parser.parse_args()
   462|    SEASON = args.season
   463|
   464|    logger.info("🚀 Starting daily Bronze layer update...")
   465|    logger.info(f"   Season: {SEASON}")
   466|    start_time = time.time()
   467|
   468|    try:
   469|        supabase = get_supabase()
   470|        logger.info("🔗 Connected to Supabase")
   471|    except Exception as e:
   472|        logger.error(f"❌ Failed to connect: {e}")
   473|        return
   474|
   475|    # Ensure metadata table exists
   476|    create_metadata_table(supabase)
   477|
   478|    # Update FPL Bronze tables
   479|    logger.info("\n" + "=" * 50)
   480|    logger.info("FPL BRONZE LAYER")
   481|    logger.info("=" * 50)
   482|    fpl_updated = update_fpl_bronze()
   483|
   484|    # Update Understat Bronze tables
   485|    logger.info("\n" + "=" * 50)
   486|    logger.info("UNDERSTAT BRONZE LAYER")
   487|    logger.info("=" * 50)
   488|    understat_updated = update_understat_bronze()
   489|
   490|    elapsed = time.time() - start_time
   491|
   492|    if fpl_updated or understat_updated:
   493|        logger.info(f"\n✅ Bronze update complete in {elapsed:.2f}s - data was updated")
   494|    else:
   495|        logger.info(
   496|            f"\n✅ Bronze update complete in {elapsed:.2f}s - no changes detected"
   497|        )
   498|
   499|
   500|if __name__ == "__main__":
   501|