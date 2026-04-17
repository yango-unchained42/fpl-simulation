# Silver Layer Pipeline Architecture

## Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              BRONZE LAYER (Source Data)                          │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌────────────────────┐  │
│  │ FPL Players │   │ Vaastav GW   │   │ Understat    │   │ Team Mappings    │  │
│  │              │   │ Player Stats │   │ Player Stats │   │ (all sources)    │  │
│  └──────────────┘   └──────────────┘   └──────────────┘   └────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           SILVER LAYER - Step 1: Team Mapping                       │
│  ┌────────────────────────────────────────────────────────────────────────────────┐   │
│  │ Script: daily_team_mapping_update.py                                       │   │
│  │                                                                             │   │
│  │ Input:  bronze_fpl_teams, bronze_vaastav_teams, bronze_understat_*          │   │
│  │ Output: silver_team_mapping                                               │   │
│  │                                                                             │   │
│  │ silver_team_mapping columns:                                               │   │
│  │   • unified_team_id (UUID) ← PRIMARY KEY                                  │   │
│  │   • season                                                                  │   │
│  │   • fpl_team_id, fpl_team_name                                             │   │
│  │   • vaastav_team_name                                                      │   │
│  │   • understat_team_id, understat_team_name                                  │   │
│  │   • source, confidence_score                                              │   │
│  └────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                      │
│  Goal: One row per team per season with a stable UUID across all data sources        │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                          SILVER LAYER - Step 2: Player Mapping                        │
│  ┌────────────────────────────────────────────────────────────────────────────────┐   │
│  │ Script: daily_player_mapping_update.py                                       │   │
│  │                                                                             │   │
│  │ Input:  bronze_fpl_players, bronze_vaastav_player_history_gw,               │   │
│  │         bronze_understat_player_mappings, silver_team_mapping               │   │
│  │ Output: silver_player_mapping                                             │   │
│  │                                                                             │   │
│  │ silver_player_mapping columns:                                             │   │
│  │   • unified_player_id (UUID) ← PRIMARY KEY                                  │   │
│  │   • season                                                                  │   │
│  │   • fpl_id ← from bronze_fpl_players.id                                     │   │
│  │   • vaastav_id ← from bronze_vaastav_player_history_gw.player_id            │   │
│  │   • understat_id ← from bronze_understat_player_mappings.understat_player_id│   │
│  │   • unified_team_id ← JOIN from silver_team_mapping                       │   │
│  │   • player_name, position, team, source, confidence_score                   │   │
│  └────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                      │
│  Goal: One row per player per season with stable UUID + all source IDs               │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         SILVER LAYER - Step 3: Match Mapping                           │
│  ┌────────────────────────────────────────────────────────────────────────────────┐   │
│  │ Script: daily_match_mapping_update.py                                       │   │
│  │                                                                             │   │
│  │ Input:  bronze_fpl_fixtures, bronze_understat_match_stats,                  │   │
│  │         silver_team_mapping                                                │   │
│  │ Output: silver_match_mapping                                              │   │
│  │                                                                             │   │
│  │ silver_match_mapping columns:                                             │   │
│  │   • match_id (UUID) ← PRIMARY KEY                                          │   │
│  │   • season                                                                  │   │
│  │   • fpl_fixture_id                                                         │   │
│  │   • understat_game_id                                                      │   │
│  │   • unified_team_home_id ← JOIN from silver_team_mapping                  │   │
│  │   • unified_team_away_id ← JOIN from silver_team_mapping                   │   │
│  │   • date, home_score, away_score                                            │   │
│  └────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                      │
│  Goal: One row per match with stable UUID + team UUIDs from team_mapping             │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              SILVER LAYER - Derived Tables                             │
│                                                                                      │
│  Once we have the three mapping tables, all other silver tables become simple:       │
│                                                                                      │
│  ┌──────────────────────────┐   ┌──────────────────────────┐   ┌────────────────┐   │
│  │ silver_fpl_gw             │   │ silver_vaastav_gw       │   │ silver_fixtures│   │
│  │                          │   │                          │   │                │   │
│  │ • unified_player_id       │   │ • unified_player_id     │   │ • match_id     │   │
│  │ • unified_team_id        │   │ • unified_team_id        │   │ • unified_team_│   │
│  │ • match_id               │   │ • match_id               │   │   home_id     │   │
│  │ • all stats columns...  │   │ • all stats columns...  │   │ • unified_team_│   │
│  │                          │   │                          │   │   away_id     │   │
│  └──────────────────────────┘   └──────────────────────────┘   │ • xG, xGA     │   │
│                                                                └────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

## Daily Pipeline Scripts

| Script | Purpose | Input Tables | Output Tables |
|--------|---------|--------------|---------------|
| `daily_team_mapping_update.py` | Create team UUIDs | `bronze_fpl_teams`, `bronze_understat_*` | `silver_team_mapping` |
| `daily_player_mapping_update.py` | Create player UUIDs | `bronze_*_players`, `silver_team_mapping` | `silver_player_mapping` |
| `daily_match_mapping_update.py` | Create match UUIDs + team links | `bronze_fpl_fixtures`, `bronze_understat_*`, `silver_team_mapping` | `silver_match_mapping` |
| `daily_silver_stats_update.py` | Transform stats with UUIDs | All bronze tables, all silver mappings | `silver_*_gw`, `silver_fixtures` |

## Why This Architecture?

1. **Single Source of Truth**: Each entity (player/team/match) has ONE UUID
2. **All Source IDs Preserved**: Keep original IDs for back-references
3. **Join-Based Resolution**: All stats tables use UUIDs - no more string matching
4. **Daily Reproducible**: Scripts can rerun from scratch each day
5. **Historical Continuity**: Same player across seasons gets same unified_player_id

## Key Join Patterns

```python
# Example: Join player stats to get unified_player_id
player_stats = bronze_fpl_gw.join(
    silver_player_mapping,
    left_on=["element", "season"],
    right_on=["fpl_id", "season"],
    how="left"
)

# Example: Join matches to get team UUIDs
matches = bronze_fpl_fixtures.join(
    silver_match_mapping,
    left_on=["id", "season"],
    right_on=["fpl_fixture_id", "season"],
    how="left"
).join(
    silver_team_mapping,
    left_on=["team_h", "season"],
    right_on=["fpl_team_id", "season"],
    how="left"
)
```

## Migration Status

- [x] `silver_team_mapping` - populated for all seasons
- [x] `silver_player_mapping` - populated for all seasons  
- [ ] `silver_match_mapping` - needs creation/review
