# Unified Player Stats Table - Schema Design

## Overview
Single unified table `silver_unified_player_stats` combining:
- `silver_fpl_player_stats` (FPL match stats)
- `silver_understat_player_stats` (Understat advanced stats)

---

## Tables Being Unified

| Source Table | Primary Data |
|--------------|-------------|
| `silver_fpl_player_stats` | goals, assists, minutes, ICT, BPS, clean sheets, defensive stats |
| `silver_understat_player_stats` | xG, xA, shots, key_passes, xg_chain, xg_buildup |

**Output:** `silver_unified_player_stats` (NEW)

---

## Column Mapping

### Key Identifiers (Required)
| Column | Type | Source Table | Source Column | Notes |
|--------|------|--------------|---------------|-------|
| player_id | INTEGER | Both | player_id | Primary key component |
| season | TEXT | Both | season | Primary key component |
| gameweek | INTEGER | Both | gameweek | Primary key component |

---

### Core Match Stats
| Column | Type | Source Table | Source Column | Priority |
|--------|------|--------------|---------------|----------|
| total_points | INTEGER | silver_fpl_player_stats | total_points | FPL |
| minutes | INTEGER | silver_understat_player_stats | minutes | **Understat** |
| goals_scored | INTEGER | silver_fpl_player_stats | goals_scored | FPL |
| assists | INTEGER | silver_fpl_player_stats | assists | FPL |
| clean_sheets | INTEGER | silver_fpl_player_stats | clean_sheets | FPL |
| goals_conceded | INTEGER | silver_fpl_player_stats | goals_conceded | FPL |
| starts | INTEGER | silver_fpl_player_stats | starts | FPL |

---

### Expected Stats (xG/xA)
| Column | Type | Source Table | Source Column | Priority |
|--------|------|--------------|---------------|----------|
| xg | REAL | silver_understat_player_stats | xg | **Understat** (superior methodology) |
| xa | REAL | silver_understat_player_stats | xa | **Understat** (superior methodology) |
| xg_chain | REAL | silver_understat_player_stats | xg_chain | Understat only |
| xg_buildup | REAL | silver_understat_player_stats | xg_buildup | Understat only |
| expected_goals | REAL | silver_fpl_player_stats | expected_goals | FPL (fallback) |
| expected_assists | REAL | silver_fpl_player_stats | expected_assists | FPL (fallback) |
| expected_goal_involvements | REAL | silver_fpl_player_stats | expected_goal_involvements | FPL only |
| expected_goals_conceded | REAL | silver_fpl_player_stats | expected_goals_conceded | FPL only |

---

### Shot/Creative Stats
| Column | Type | Source Table | Source Column | Priority |
|--------|------|--------------|---------------|----------|
| shots | INTEGER | silver_understat_player_stats | shots | **Understat** (detailed) |
| key_passes | INTEGER | silver_understat_player_stats | key_passes | Understat only |

---

### Discipline Stats
| Column | Type | Source Table | Source Column | Priority |
|--------|------|--------------|---------------|----------|
| yellow_cards | INTEGER | silver_fpl_player_stats | yellow_cards | **FPL** (official) |
| red_cards | INTEGER | silver_fpl_player_stats | red_cards | **FPL** (official) |
| own_goals | INTEGER | silver_fpl_player_stats | own_goals | **FPL** (official) |
| penalties_saved | INTEGER | silver_fpl_player_stats | penalties_saved | FPL only |
| penalties_missed | INTEGER | silver_fpl_player_stats | penalties_missed | FPL only |

---

### Bonus/BPS (FPL-only)
| Column | Type | Source Table | Source Column |
|--------|------|--------------|---------------|
| bonus | INTEGER | silver_fpl_player_stats | bonus |
| bps | INTEGER | silver_fpl_player_stats | bps |

---

### ICT Index (FPL-only)
| Column | Type | Source Table | Source Column |
|--------|------|--------------|---------------|
| influence | REAL | silver_fpl_player_stats | influence |
| creativity | REAL | silver_fpl_player_stats | creativity |
| threat | REAL | silver_fpl_player_stats | threat |
| ict_index | REAL | silver_fpl_player_stats | ict_index |

---

### Defensive Stats (FPL-only)
| Column | Type | Source Table | Source Column |
|--------|------|--------------|---------------|
| tackles | INTEGER | silver_fpl_player_stats | tackles |
| clearances_blocks_interceptions | INTEGER | silver_fpl_player_stats | clearances_blocks_interceptions |
| recoveries | INTEGER | silver_fpl_player_stats | recoveries |
| defensive_contribution | REAL | silver_fpl_player_stats | defensive_contribution |
| saves | INTEGER | silver_fpl_player_stats | saves |

---

### Team/Position Context
| Column | Type | Source Table | Source Column | Notes |
|--------|------|--------------|---------------|-------|
| team_id | INTEGER | silver_understat_player_stats | team_id | Understat |
| position | TEXT | silver_understat_player_stats | position | |
| position_id | INTEGER | silver_understat_player_stats | position_id | 1=GKP, 2=DEF, 3=MID, 4=FWD |
| game_id | INTEGER | silver_understat_player_stats | game_id | Understat match ID |

---

### Match Context (IDs only - NO names!)
| Column | Type | Source Table | Source Column | Notes |
|--------|------|--------------|---------------|-------|
| was_home | BOOLEAN | silver_fpl_player_stats | was_home | True = home match |
| opponent_team_id | INTEGER | silver_fpl_player_stats | (convert from opponent_team) | **NEW** - ID not name |
| fixture_id | INTEGER | silver_fpl_player_stats | fixture | FPL fixture ID |
| kickoff_time | TIMESTAMP | silver_fpl_player_stats | kickoff_time | |
| home_score | INTEGER | silver_fpl_player_stats | team_h_score | Renamed |
| away_score | INTEGER | silver_fpl_player_stats | team_a_score | Renamed |

---

### Data Quality Flags
| Column | Type | Source Table | Source Column |
|--------|------|--------------|---------------|
| data_quality_score | REAL | Both | data_quality_score |
| is_incomplete | BOOLEAN | Both | is_incomplete |
| missing_fields | TEXT[] | Both | missing_fields |

---

### Metadata
| Column | Type | Source |
|--------|------|--------|
| created_at | TIMESTAMP WITH TIME ZONE | Auto-generated |
| updated_at | TIMESTAMP WITH TIME ZONE | Auto-generated |

---

## Primary Key
```sql
PRIMARY KEY (player_id, season, gameweek)
```

---

## Columns Being DROPPED

### From silver_fpl_player_stats (NOT included in unified):
| Column | Type | Reason |
|--------|------|--------|
| source | TEXT | Not needed in unified (always "merged") |
| opponent_team | TEXT | ❌ Dropped - replaced by opponent_team_id |
| fixture | INTEGER | Keep as fixture_id |
| kickoff_time | TEXT | Keep as TEXT (keep as-is) |
| team_a_score | INTEGER | Renamed to away_score |
| team_h_score | INTEGER | Renamed to home_score |

### From silver_understat_player_stats (NOT included in unified):
| Column | Type | Reason |
|--------|------|--------|
| league_id | INTEGER | Not needed (always filter to PL) |
| season_id | INTEGER | Not needed |

---

## Duplicate Column Resolution (when both have it)

| Column | Winner | Source | Reason |
|--------|--------|--------|--------|
| minutes | **Understat** | silver_understat_player_stats | More granular, accounts for subs |
| goals | **FPL** | silver_fpl_player_stats | Official Premier League data |
| assists | **FPL** | silver_fpl_player_stats | Official Premier League data |
| xg | **Understat** | silver_understat_player_stats | Understat methodology is superior |
| xa | **Understat** | silver_understat_player_stats | Understat methodology is superior |
| shots | **Understat** | silver_understat_player_stats | Detailed shot location data |
| yellow_cards | **FPL** | silver_fpl_player_stats | Official data |
| red_cards | **FPL** | silver_fpl_player_stats | Official data |
| own_goals | **FPL** | silver_fpl_player_stats | Official data |
| team_id | **Understat** | silver_understat_player_stats | Understat provides team context |

---

## Join Strategy for Streamlit Display

```python
# At display time, join to mapping tables for names
player_stats = (
    silver_unified_player_stats
    .join(silver_player_mapping, on="player_id")  # adds web_name, full_name
    .join(silver_team_mapping, on="team_id")     # adds team_name
    .join(silver_team_mapping, on="opponent_team_id", suffix="_opp")  # adds opponent_team_name
)
```

---

## Next Steps
1. Create migration: `018_create_unified_player_stats.sql`
2. Update daily_silver_update.py to merge FPL + Understat into unified table
3. Populate opponent_team_id from existing opponent_team data
4. Update downstream queries (features, models) to use new table
5. Optionally: Drop old silver_fpl_player_stats and silver_understat_player_stats tables
