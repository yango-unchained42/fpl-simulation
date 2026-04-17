# Ticket: SPRINT11-003 - Silver Data Consolidation

## Description
Consolidate all Bronze layer data into Silver tables across all seasons. This is the foundation for ML features in Gold - keeping raw data clean and aggregated while preparing for cross-source merging.

## Updated Approach (2026-04-09)

Instead of merging all sources into one table (complex), we'll consolidate each source separately and add team-level aggregations:

### Silver Tables to Create

#### FPL Layer
| Table | Source | Description |
|-------|--------|--------------|
| `silver_fpl_gw` | `bronze_fpl_gw` + `bronze_fpl_players` | FPL GW data enriched with price/ownership from players table |
| `silver_fpl_gw_teams` | Aggregated from `silver_fpl_gw` | Team stats per GW (goals for/against, clean sheets, etc.) |
| `silver_fpl_fixtures` | `bronze_fpl_fixtures` | Fixture dates, team strengths (join as needed) |
| `silver_fpl_teams` | `bronze_fpl_teams` | Team metadata |

#### Vaastav Layer
| Table | Source | Description |
|-------|--------|--------------|
| `silver_vaastav_gw` | `bronze_vaastav_player_history_gw` | All Vaastav GW data across seasons |
| `silver_vaastav_gw_teams` | Aggregated from `silver_vaastav_gw` | Team stats per GW |
| `silver_vaastav_fixtures` | `bronze_vaastav_fixtures` | Vaastav fixtures |

#### Understat Layer
| Table | Source | Description |
|-------|--------|--------------|
| `silver_understat_gw` | `bronze_understat_player_stats` | xG, xA, shots per player-GW |
| `silver_understat_gw_teams` | Aggregated from `silver_understat_gw` | Team xG/xA per GW |
| `silver_understat_shots` | `bronze_understat_shots` | Shot-level data |
| `silver_understat_match_history` | `bronze_understat_match_stats` | Match results |

#### Already Created (SPRINT11-001, 002)
| Table | Status |
|-------|--------|
| `silver_player_mapping` | ✅ Created |
| `silver_team_mapping` | ✅ Created |

## Implementation Rules
- **No dropping columns** - Keep all data, add NULL for missing values
- **Add data quality flags** - Mark incomplete records
- **Consolidate seasons** - All data in one table per source
- **Aggregate team stats** - Calculate per-GW team metrics from player data
- **Keep fixtures separate** - Can join in Gold or Streamlit as needed

## Technical Implementation

### Data Sources Available (2026-04-09)
- `bronze_fpl_gw` - 2025-26, 44 cols
- `bronze_fpl_fixtures` - 2025-26, 19 cols  
- `bronze_fpl_players` - 2025-26, 107 cols
- `bronze_fpl_teams` - 2025-26, 23 cols
- `bronze_vaastav_player_history_gw` - 2021-22 to 2023-24, 41 cols
- `bronze_vaastav_fixtures` - 2021-22, 2022-23, 11 cols
- `bronze_understat_player_stats` - All seasons, 22 cols
- `bronze_understat_player_mappings` - All seasons, 6 cols
- `bronze_understat_match_stats` - All seasons, 28 cols
- `bronze_understat_shots` - 2021-22, 19 cols
- `bronze_understat_team_mappings` - All, 4 cols

### Key Scripts Updated
- `src/silver/player_mapping.py` - Uses `bronze_vaastav_player_history_gw` (renamed from `bronze_player_history`)
- `src/silver/team_mapping.py` - Uses `bronze_vaastav_player_history_gw`

## Acceptance Criteria
- [ ] All 11 Silver tables created in Supabase
- [ ] No columns dropped from Bronze sources
- [ ] All seasons consolidated per source
- [ ] Team-level aggregations created
- [ ] Data quality flags added
- [ ] Streamlit can join fixtures when needed

## Definition of Done
- [ ] SQL migrations for all tables
- [ ] Python scripts for consolidation
- [ ] All data uploaded to Supabase
- [ ] Documentation updated

## Agent
build

## Status
In Progress

## Progress Log
- 2026-04-09: Updated approach - consolidate by source instead of merging
- 2026-04-09: Updated table names in player_mapping.py and team_mapping.py to use `bronze_vaastav_player_history_gw`
- 2026-04-09: Defined final Silver table structure (11 tables)
- 2026-04-10: Created bronze_vaastav_players table and uploaded 3,184 players
- 2026-04-10: Created silver_vaastav_gw with quality flags, uploaded 104,160 records
- 2026-04-10: Refined approach - drop full silver_vaastav_players, create silver_player_state instead
- 2026-04-10: Created silver_player_state with 3,184 records (GW38 snapshot for all seasons)
- 2026-04-10: Fixed silver_understat_player_stats - added pagination to fetch all 54,124 records (was 1,000 due to Supabase 1000-row limit)
- 2026-04-10: Confirmed silver_understat_match_stats has 0 records because all 1,829 Bronze records have NULL dates (source data issue)