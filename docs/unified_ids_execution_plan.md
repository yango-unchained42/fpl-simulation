# Execution Plan: Unified ID System for Silver Tables

## Overview
Replace fragmented IDs (FPL/vaastav/Understat) with unified IDs at the Silver layer for simple joins.

---

## Data Sources

| Source | Player ID | Team ID | Game ID | Seasons |
|--------|-----------|---------|---------|----------|
| **FPL API** | element | team (FPL ID) | fixture | 2024-25+ |
| **vaastav** | id | team (vaastav) | fixture | 2016-2024 |
| **Understat** | id | team_id | game_id | 2016-2025 |

---

## Unified ID Tables (Already Exist / To Create)

### 1. silver_player_mapping ✅ EXISTS
```
unified_player_id (UUID, PK)
season
fpl_id          (from FPL API)
vaastav_id       (from vaastav)
understat_id     (from Understat)
player_name
position
team
confidence_score
```

### 2. silver_team_mapping ✅ EXISTS
```
unified_team_id (UUID, PK)
season
fpl_team_id
vaastav_team_id
understat_team_id
team_name
```

### 3. match_mapping (NEW)
```
match_id (UUID, PK)
season
date
fpl_fixture_id
vaastav_fixture_id
understat_game_id
home_team_id (FPL)
away_team_id (FPL)
```

---

## Execution Plan

### Phase 1: Create match_mapping table

Migration: `021_create_match_mapping.sql`

```sql
CREATE TABLE match_mapping (
    match_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season TEXT NOT NULL,
    game_date DATE NOT NULL,

    -- FPL/vaastav fixtures
    fpl_fixture_id INTEGER,
    vaastav_fixture_id INTEGER,

    -- Understat game
    understat_game_id INTEGER,

    -- Teams (FPL IDs)
    home_team_id INTEGER,
    away_team_id INTEGER,

    -- Metadata
    created_at TIMESTAMP DEFAULT NOW(),

    UNIQUE(season, fpl_fixture_id),
    UNIQUE(season, vaastav_fixture_id),
    UNIQUE(season, understat_game_id)
);
```

### Phase 2: Populate match_mapping

**Sources to join:**
- `silver_fixtures` (FPL fixtures with kickoff_time)
- `silver_understat_match_stats` (Understat games with date)

**Process:**
```python
# Step a: Get all FPL fixtures
fpl_fixtures = get_all("silver_fixtures")  # has kickoff_time, team_h, team_a

# Step b: Get all Understat matches
understat_matches = get_all("silver_understat_match_stats")  # has date, home_team_id, away_team_id

# Step c: Match by (date, home_team, away_team)
for fpl_fix in fpl_fixtures:
    for us_match in understat_matches:
        if (fpl_fix.date == us_match.date
            and fpl_fix.team_h == us_match.home_team_id
            and fpl_fix.team_a == us_match.away_team_id):
            # Create unified match_id
            match_mapping[(fpl_fix.season, fpl_fix.id)] = match_id
            match_mapping[(us_match.season, us_match.game_id)] = match_id
```

### Phase 3: Update silver_fpl_player_stats

Add columns:
- `unified_player_id` (from player_mapping)
- `unified_team_id` (from team_mapping)
- `match_id` (from match_mapping)

```python
def add_unified_ids_to_fpl():
    for rec in silver_fpl_player_stats:
        # Get unified_player_id
        season = rec.season
        player_id = rec.player_id
        rec.unified_player_id = player_mapping[(season, player_id)]

        # Get match_id from fixture
        rec.match_id = match_mapping[(season, rec.fixture)]

        # Get team - need to derive from fixture (team_h or team_a)
        # Use (was_home, opponent_team) to find player's team
```

### Phase 4: Update silver_understat_player_stats

Add columns:
- `unified_player_id` (from player_mapping)
- `unified_team_id` (from team_mapping)
- `match_id` (from match_mapping)

```python
def add_unified_ids_to_understat():
    for rec in silver_understat_player_stats:
        # Get unified_player_id
        season = rec.season
        us_player_id = rec.player_id
        rec.unified_player_id = player_mapping[(season, us_player_id)]

        # Get match_id
        rec.match_id = match_mapping[(season, rec.game_id)]

        # Get unified_team_id
        rec.unified_team_id = team_mapping[(season, rec.team_id)]
```

### Phase 5: Join is now trivial!

```python
def merge_unified():
    # Load both tables
    fpl_data = get("silver_fpl_player_stats")      # has unified_player_id, match_id
    understat_data = get("silver_understat_player_stats")  # has unified_player_id, match_id

    # Simple join!
    merged = {}
    for fpl in fpl_data:
        key = (fpl.unified_player_id, fpl.match_id)
        merged[key] = fpl

    for us in understat_data:
        key = (us.unified_player_id, us.match_id)
        if key in merged:
            # Add Understat columns
            merged[key].xg = us.xg
            merged[key].xa = us.xa
            merged[key].shots = us.shots
            # etc.
        else:
            # Understat-only record
            merged[key] = us

    # Upload to silver_unified_player_stats
    upload(merged)
```

---

## Player ID Mapping Logic

Since we have 3 source IDs, map in priority order:

```python
def get_unified_player_id(season, fpl_id=None, vaastav_id=None, understat_id=None):
    # Priority: fpl_id > vaastav_id > understat_id
    if fpl_id:
        return player_map[(season, fpl_id)]  # from player_mapping
    if vaastav_id:
        return player_map[(season, vaastav_id)]
    if understat_id:
        return player_map[(season, understat_id)]
    return None
```

---

## Expected Results

| Metric | Value |
|--------|-------|
| Unified player IDs | ~100% (all sources mapped) |
| Unified match IDs | ~100% (date+teams matched) |
| Join match rate | 90%+ (should be near complete) |

---

## Implementation Order

1. ✅ Create match_mapping table (migration)
2. ✅ Populate match_mapping (from fixtures + Understat)
3. 🔲 Add unified columns to silver_fpl_player_stats
4. 🔲 Add unified columns to silver_understat_player_stats
5. 🔲 Update daily_silver_update.py with simple join
6. 🔲 Test and verify

---

## Notes

- `vaastav` data comes from local parquet files historically
- FPL API for current season
- Understat for all seasons
- Need to handle the mapping for each source appropriately
