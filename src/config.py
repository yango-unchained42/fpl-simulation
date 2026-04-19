"""Central configuration for the FPL Simulation pipeline.

All environment-dependent values live here. Scripts and modules import from
this module instead of hardcoding constants or reading env vars directly.

Environment variables (loaded from .env or shell):
    CURRENT_SEASON      — e.g. "2025-26" (auto-detected from FPL API if not set)
    SUPABASE_URL        — Supabase project URL
    SUPABASE_KEY        — Supabase anon/service key
    SUPABASE_ACCESS_TOKEN — Supabase CLI access token (for TRUNCATE etc.)
    FPL_API_BASE        — FPL API base URL (default: https://fantasy.premierleague.com/api/)
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root (two levels up from this file)
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")
# Also try cwd as fallback
load_dotenv()

# ── Season ──────────────────────────────────────────────────────────────────
CURRENT_SEASON: str = os.getenv("CURRENT_SEASON", "2025-26")

# Understat uses slash format ("2025/26"), FPL uses dash ("2025-26")
CURRENT_SEASON_UNDERSTAT: str = CURRENT_SEASON.replace("-", "/")

ALL_SEASONS: list[str] = [
    "2021-22",
    "2022-23",
    "2023-24",
    "2024-25",
    "2025-26",
]

# ── FPL API ─────────────────────────────────────────────────────────────────
FPL_API_BASE: str = os.getenv("FPL_API_BASE", "https://fantasy.premierleague.com/api/")

# ── Supabase ────────────────────────────────────────────────────────────────
SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "")
SUPABASE_ACCESS_TOKEN: str = os.getenv("SUPABASE_ACCESS_TOKEN", "")

# ── Paths ───────────────────────────────────────────────────────────────────
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent.parent
DATA_DIR: Path = PROJECT_ROOT / "data"
RAW_DATA_DIR: Path = DATA_DIR / "raw"
PROCESSED_DATA_DIR: Path = DATA_DIR / "processed"
MODEL_DIR: Path = DATA_DIR / "models"

# ── Pipeline settings ──────────────────────────────────────────────────────
BATCH_SIZE: int = 500
CACHE_TTL_SECONDS: int = 3600  # 1 hour
MAX_RETRIES: int = 3
RETRY_DELAY_SECONDS: int = 2

# ── FPL constraints ────────────────────────────────────────────────────────
BUDGET: float = 100.0
SQUAD_SIZE: int = 15
MAX_PER_CLUB: int = 3
POSITION_COUNTS: dict[str, int] = {"GK": 2, "DEF": 5, "MID": 5, "FWD": 3}

# ── Monte Carlo ─────────────────────────────────────────────────────────────
N_SIMULATIONS: int = 10_000


def get_supabase():
    """Get a configured Supabase client.

    Returns:
        Supabase client instance.

    Raises:
        ValueError: If SUPABASE_URL or SUPABASE_KEY is missing.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError(
            "Missing SUPABASE_URL or SUPABASE_KEY. "
            "Set them in .env or as environment variables."
        )
    from supabase import create_client  # type: ignore[attr-defined]

    return create_client(SUPABASE_URL, SUPABASE_KEY)
